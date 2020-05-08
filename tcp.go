package tcpwrapper

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"
)

type ConnSession struct {
	Conn                   net.Conn
	connUserClose          chan struct{}
	internalConnErrorClose chan struct{}
	closingRS              chan struct{}
	closing                *sync.WaitGroup
	sendingQueue           chan []byte
	recevingQueue          chan []byte
}

func (conn *ConnSession) Init() {
	conn.connUserClose = make(chan struct{})
	conn.internalConnErrorClose = make(chan struct{})
	conn.closingRS = make(chan struct{})
	conn.closing = &sync.WaitGroup{}
}

func (conn *ConnSession) SetupConn(c net.Conn) {
	if conn.Conn != nil {
		conn.sendingQueue = nil
		conn.recevingQueue = nil
		conn.Init()
	}
	conn.Conn = c
}

func (conn *ConnSession) Remote() string {
	return conn.Conn.RemoteAddr().String()
}

func (conn *ConnSession) RequestClose() {
	close(conn.connUserClose)
}

func (conn *ConnSession) SafeWaitClose() {
	close(conn.closingRS)
	conn.Conn.SetDeadline(time.Now())
	if SpamLogger != nil {
		SpamLogger(fmt.Sprintf("[CONN] %s is waiting for all closing units.", conn.Remote()))
	}
	conn.closing.Wait()
	if InfoLogger == nil {
		conn.Conn.Close()
	} else {
		remote := conn.Remote()
		conn.Conn.Close()
		InfoLogger(fmt.Sprintf("[CONN] Conn to %s is closed completedly.", remote))
	}
}

func (conn *ConnSession) errorClose(err error, info string) {
	close(conn.internalConnErrorClose)
	if ErrorLogger != nil {
		ErrorLogger(fmt.Sprintf("[CONN] An error occured in conn to %s: %s. Info: %s", conn.Remote(), err, info))
	}
}

func (conn *ConnSession) WaitAnyClose() {
	select {
	case <-conn.connUserClose:
	case <-conn.internalConnErrorClose:
	}
}

// Sender opens goroutine to do sending work.
// Buffered Sender introduce a small interval between network sending tp cache data to send at once. May provide slight more cpu efficiency and cause some delay.
func (conn *ConnSession) Sender(chanSize int, buffered bool, bufferSize int) chan []byte {
	if conn.sendingQueue != nil {
		panic("Multiple Sender opened")
	}
	conn.sendingQueue = make(chan []byte, chanSize)

	if !buffered {
		conn.closing.Add(1)
		go func() {
			defer func() {
				conn.closing.Done()
			}()
			if InfoLogger != nil {
				InfoLogger(fmt.Sprintf("[CONN] Sender for %s is up.\n", conn.Remote()))
				defer InfoLogger(fmt.Sprintf("[CONN] Sender for %s is down!\n", conn.Remote()))
			}

			for {
				// Wait until the conn has a message to send or any closing operation
				select {
				case <-conn.connUserClose:
					defaultOnUserClosingSender(conn)
					return
				case <-conn.internalConnErrorClose:
					defaultOnErrorClosingSender(conn)
					return
				case msg := <-conn.sendingQueue:
					// After getting the message to send, still

					err := conn.WriteBytes(msg, defaultSenderInterruptor)
					if err != nil {
						// Check if the error is our custom interrupt error
						switch err {
						case &sharedInterruptedByError:
							defaultOnErrorClosingSender(conn)
							return
						case &sharedInterruptedByUser:
							defaultOnUserClosingSender(conn)
							return
						default:
						}

						// Handle the error if it's a timeout we triggered
						if handleClosingTimedout(conn, err, defaultOnUserClosingSender, defaultOnErrorClosingSender) {
							return
						}

						// Unexpected networking errors...
						conn.errorClose(err, "sending")
						if OnSenderErrorClosed != nil {
							OnSenderErrorClosed(conn)
						}
						return
					}
				}
			}
		}()
	} else {
		sendBuffer := bytes.NewBuffer(make([]byte, 0, bufferSize))
		batchInterval := time.Millisecond * time.Duration(BUFFERED_SEND_INTERVAL_MS)
		conn.closing.Add(1)
		go func() {
			timeout := time.NewTimer(batchInterval)
			defer func() {
				conn.closing.Done()
			}()
			if InfoLogger != nil {
				InfoLogger(fmt.Sprintf("[CONN] Sender for %s is up.\n", conn.Remote()))
				defer InfoLogger(fmt.Sprintf("[CONN] Sender for %s is down!\n", conn.Remote()))
			}
			var cachedMsgBeforeFlushing []byte
			for {
				// Wait until the conn has a message to send or any closing operation
				// if last sent time is closer then BATCH_TIME, keep taking messgges until the buffer is full or BATCH_TIME is past
				// Then write all data in the buffer to remote
				timeout.Reset(batchInterval)
			buffering:
				for {
					select {
					case <-conn.connUserClose:
						defaultOnUserClosingSender(conn)
						return
					case <-conn.internalConnErrorClose:
						defaultOnErrorClosingSender(conn)
						return
					case msg := <-conn.sendingQueue:
						if sendBuffer.Cap()-sendBuffer.Len() < len(msg) {
							cachedMsgBeforeFlushing = msg
							break buffering
						} else {
							written, err := sendBuffer.Write(msg)
							if err != nil {
								conn.errorClose(err, "writing to send buffer")
								if OnSenderErrorClosed != nil {
									OnSenderErrorClosed(conn)
								}
								return
							}
							if written != len(msg) {
								conn.errorClose(err, "buffered bytes mismatch")
								if OnSenderErrorClosed != nil {
									OnSenderErrorClosed(conn)
								}
								return
							}
						}
					case <-timeout.C:
						if sendBuffer.Len() == 0 {
							msg := <-conn.sendingQueue
							written, err := sendBuffer.Write(msg)
							if err != nil {
								conn.errorClose(err, "writing to send buffer")
								if OnSenderErrorClosed != nil {
									OnSenderErrorClosed(conn)
								}
								return
							}
							if written != len(msg) {
								conn.errorClose(err, "buffered  bytes mismatch")
								if OnSenderErrorClosed != nil {
									OnSenderErrorClosed(conn)
								}
								return
							}
						}
						break buffering
					}
				}

				err := conn.WriteBytes(sendBuffer.Next(sendBuffer.Len()), defaultSenderInterruptor)
				if err != nil {
					// Check if the error is our custom interrupt error
					switch err {
					case &sharedInterruptedByError:
						defaultOnErrorClosingSender(conn)
						return
					case &sharedInterruptedByUser:
						defaultOnUserClosingSender(conn)
						return
					default:
					}

					// Handle the error if it's a timeout we triggered
					if handleClosingTimedout(conn, err, defaultOnUserClosingSender, defaultOnErrorClosingSender) {
						return
					}

					// Unexpected networking errors...
					conn.errorClose(err, "writing bytes")
					if OnSenderErrorClosed != nil {
						OnSenderErrorClosed(conn)
					}
					return
				}

				if cachedMsgBeforeFlushing != nil {
					sendBuffer.Write(cachedMsgBeforeFlushing)
					cachedMsgBeforeFlushing = nil
				}
			}
		}()

	}
	return conn.sendingQueue
}

func (conn *ConnSession) Receiver(chanSize int, buffered bool, bufferSize int, discardMessage bool) chan []byte {
	if conn.recevingQueue != nil {
		panic("Multiple receiver opened")
	}
	if !discardMessage {
		conn.recevingQueue = make(chan []byte, chanSize)
	}
	if !buffered {
		go func() {
			conn.closing.Add(1)
			defer func() {
				conn.closing.Done()
			}()
			if InfoLogger != nil {
				InfoLogger(fmt.Sprintf("[CONN] Receiver of %s is up", conn.Remote()))
				defer InfoLogger(fmt.Sprintf("[CONN] Receiver of %s is down!", conn.Remote()))
			}
			recvWorkspace := make([]byte, bufferSize)
			for {
				select {
				case <-conn.connUserClose:
					defaultOnUserClosingReceiver(conn)
				case <-conn.internalConnErrorClose:
					defaultOnErrorClosingReceiver(conn)
				default:
				}
				receivedLength, err := conn.ReadMessage(recvWorkspace, defaultReceiverInterruptor)
				if SpamLogger != nil {
					SpamLogger(fmt.Sprintf("[CONN] Receiver read a message of length: %d", receivedLength))
				}
				
				switch err {
				case &sharedInterruptedByUser:
					defaultOnUserClosingReceiver(conn)
				case &sharedInterruptedByError:
					defaultOnErrorClosingReceiver(conn)
				case nil:
				default:
					if handleClosingTimedout(conn, err, defaultOnUserClosingReceiver, defaultOnErrorClosingReceiver) {
						return
					}
					conn.errorClose(err, "reading message")
					if OnReceiverErrorClosed != nil {
						OnReceiverErrorClosed(conn)
					}
					return
				}

				if discardMessage {
					continue
				}

				msg := make([]byte, receivedLength)
				copy(msg, recvWorkspace[:receivedLength])
				conn.recevingQueue <- msg
				if SpamLogger != nil {
					SpamLogger(fmt.Sprintf("[CONN] Receiver piped a message of length: %d.", len(msg)))
				}
			}
		}()
	} else {
		recvBuffer := bytes.NewBuffer(make([]byte, 0, bufferSize))
		waitingForBuffer := make(chan struct{})
		go func() {
			conn.closing.Add(1)
			defer func() {
				conn.closing.Done()
			}()
			if InfoLogger != nil {
				InfoLogger(fmt.Sprintf("\n[CONN] Receiver of %s is up", conn.Remote()))
				defer InfoLogger(fmt.Sprintf("\n[CONN] Receiver of %s is down!", conn.Remote()))
			}
			var shouldContinue = func() bool {
				select {
				case <-conn.connUserClose:
					if InfoLogger != nil {
						InfoLogger(fmt.Sprintf("[CONN] Signaled. Closing receiver for %s.\n", conn.Remote()))
					}
					if OnReceiverUserClosed != nil {
						OnReceiverUserClosed(conn)
					}
					return false
				case <-conn.internalConnErrorClose:
					if InfoLogger != nil {
						InfoLogger(fmt.Sprintf("[CONN] Internal Error Close. Closing Receiver for %s.\n", conn.Remote()))
					}
					if OnReceiverErrorClosed != nil {
						OnReceiverErrorClosed(conn)
					}
					return false
				default:
					return true
				}
			}
			recvWorkspace := make([]byte, bufferSize)
			for {
				if !shouldContinue() {
					return
				}
				read, err := conn.Conn.Read(recvWorkspace)
				if !shouldContinue() {
					return
				}
				if err != nil {
					conn.errorClose(err, "receiving")
					if OnReceiverErrorClosed != nil {
						OnReceiverErrorClosed(conn)
					}
					return
				}

				if discardMessage {
					continue
				}

				for recvBuffer.Cap()-recvBuffer.Len() < read {
					if SpamLogger != nil {
						SpamLogger(fmt.Sprintf("[CONN] Receiver is waiting for buffer being resolved"))
					}
					waitingForBuffer <- struct{}{}
					if !shouldContinue() {
						return
					}
				}

				buffered := 0
				for buffered < read {
					w, err := recvBuffer.Write(recvWorkspace[buffered:read])
					if err != nil {
						if _, ok := <-conn.closingRS; !ok {
							if err.(net.Error).Timeout() {
								continue
							}
						}
						conn.errorClose(err, "receiving and writing to buffer")
						if OnReceiverErrorClosed != nil {
							OnReceiverErrorClosed(conn)
						}
						return
					}
					buffered += w
				}
				if SpamLogger != nil {
					SpamLogger(fmt.Sprintf("[CONN] Receiver finished %d bytes to buffer", buffered))
				}

			}
		}()

		if !discardMessage {
			go func() {
				conn.closing.Add(1)
				defer func() {
					conn.closing.Done()
					if InfoLogger != nil {
						InfoLogger(fmt.Sprintf("[CONN] Resoler for %s is closed.", conn.Remote()))
					}
				}()
				msgWorkspace := make([]byte, bufferSize)
				for {
					read := 0
					for read < 4 {
						r, err := recvBuffer.Read(msgWorkspace[read:4])
						if err != nil {
							conn.errorClose(err, "resolving buffered bytes")
							return
						}
						read += r
						if !defaultSafetySelect(conn) {
							return
						}
					}
					var msgLength uint32
					if USE_BIG_ENDIAN {
						msgLength = binary.BigEndian.Uint32(msgWorkspace)
					} else {
						msgLength = binary.LittleEndian.Uint32(msgWorkspace)
					}
					read = 0
					for uint32(read) < msgLength {
						r, err := recvBuffer.Read(msgWorkspace[read:msgLength])
						if err != nil {
							conn.errorClose(err, "resolving buffered bytes")
							return
						}
						read += r
						if !defaultSafetySelect(conn) {
							return
						}
					}
					select {
					case <-waitingForBuffer:
						if SpamLogger != nil {
							SpamLogger(fmt.Sprintf("[CONN] Resolver notified the buffer is resolved"))
						}
					default:
					}

					msg := make([]byte, msgLength)
					copy(msg, msgWorkspace)
					conn.recevingQueue <- msg
				}
			}()
		}
	}
	return conn.recevingQueue

}
