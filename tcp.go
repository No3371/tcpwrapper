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
	closing                *sync.WaitGroup
}

func (conn *ConnSession) Init() {
	conn.connUserClose = make(chan struct{})
	conn.internalConnErrorClose = make(chan struct{})
	conn.closing = &sync.WaitGroup{}
}

func (conn *ConnSession) SetupConn(c net.Conn) {
	conn.Conn = c
}

func (conn *ConnSession) Remote() string {
	return conn.Conn.RemoteAddr().String()
}

func (conn *ConnSession) RequestClose() {
	close(conn.connUserClose)
}

func (conn *ConnSession) SafeWaitClose() {
	conn.closing.Wait()
	if InfoLogger != nil {
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
	var pipe = make(chan []byte, chanSize)
	if !buffered {
		conn.closing.Add(1)
		go func() {
			defer func() {
				conn.closing.Done()
			}()
			if InfoLogger != nil {
				InfoLogger(fmt.Sprintf("[CONN] Sender for %s is up.\n", conn.Remote()))

				defer InfoLogger(fmt.Sprintf("[CONN] Sender for WS# %s is down!\n", conn.Remote()))
			}
			var trySendAll = func() {
			ending:
				for {
					select {
					case msg := <-pipe:
						if err := WriteBytes(conn.Conn, msg); err != nil {
							break ending
						}
					default:
						break ending
					}
				}
			}
			for {
				// Wait until the conn has a message to send or any closing operation
				select {
				case <-conn.connUserClose:
					if InfoLogger != nil {
						InfoLogger(fmt.Sprintf("[CONN] Signaled. Closing sender for %s.\n", conn.Remote()))
					}
					trySendAll()
					if OnSenderUserClosed != nil {
						OnSenderUserClosed(conn)
					}
					return
				case <-conn.internalConnErrorClose:
					if InfoLogger != nil {
						InfoLogger(fmt.Sprintf("[CONN] Internal Error Close. Closing sender for %s.\n", conn.Remote()))
					}
					trySendAll()
					if OnSenderErrorClosed != nil {
						OnSenderErrorClosed(conn)
					}
					return
				case msg := <-pipe:
					// After getting the message to send, still
					select {
					case <-conn.connUserClose:
						if InfoLogger != nil {
							InfoLogger(fmt.Sprintf("[CONN] Signaled. Closing sender for %s.\n", conn.Remote()))
						}
						trySendAll()
						if OnSenderUserClosed != nil {
							OnSenderUserClosed(conn)
						}
						return
					case <-conn.internalConnErrorClose:
						if InfoLogger != nil {
							InfoLogger(fmt.Sprintf("[CONN] Internal Error Close. Closing sender for %s.\n", conn.Remote()))
						}
						trySendAll()
						if OnSenderErrorClosed != nil {
							OnSenderErrorClosed(conn)
						}
						return
					default:
						err := WriteBytes(conn.Conn, msg)
						if err != nil {
							conn.errorClose(err, "sending")
							if OnSenderErrorClosed != nil {
								OnSenderErrorClosed(conn)
							}
							return
						}
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

				defer InfoLogger(fmt.Sprintf("[CONN] Sender for WS# %s is down!\n", conn.Remote()))
			}
			var trySendAll = func() {
			ending:
				for {
					select {
					default:
						break ending
					}
				}
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
						if InfoLogger != nil {
							InfoLogger(fmt.Sprintf("[CONN] Signaled. Closing sender for %s.\n", conn.Remote()))
						}
						trySendAll()
						if OnSenderUserClosed != nil {
							OnSenderUserClosed(conn)
						}
						return
					case <-conn.internalConnErrorClose:
						if InfoLogger != nil {
							InfoLogger(fmt.Sprintf("[CONN] Internal Error Close. Closing sender for %s.\n", conn.Remote()))
						}
						trySendAll()
						if OnSenderErrorClosed != nil {
							OnSenderErrorClosed(conn)
						}
						return
					case msg := <-pipe:
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
								conn.errorClose(err, "written bytes mismatch")
								if OnSenderErrorClosed != nil {
									OnSenderErrorClosed(conn)
								}
								return
							}
						}
					case <-timeout.C:
						if sendBuffer.Len() == 0 {
							msg := <-pipe
							written, err := sendBuffer.Write(msg)
							if err != nil {
								conn.errorClose(err, "writing to send buffer")
								if OnSenderErrorClosed != nil {
									OnSenderErrorClosed(conn)
								}
								return
							}
							if written != len(msg) {
								conn.errorClose(err, "written bytes mismatch")
								if OnSenderErrorClosed != nil {
									OnSenderErrorClosed(conn)
								}
								return
							}
						}
						break buffering
					}
				}

				err := WriteBytes(conn.Conn, sendBuffer.Next(sendBuffer.Len()))
				if err != nil {
					conn.errorClose(err, "written bytes mismatch")
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
	return pipe
}

func (conn *ConnSession) Receiver(chanSize int, buffered bool, bufferSize int) chan []byte {
	var pipe = make(chan []byte, chanSize)
	if !buffered {
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
						InfoLogger(fmt.Sprintf("[CONN > R] Signaled. Closing receiver for %s.\n", conn.Remote()))
					}
					if OnReceiverUserClosed != nil {
						OnReceiverUserClosed(conn)
					}
					return false
				case <-conn.internalConnErrorClose:
					if InfoLogger != nil {
						InfoLogger(fmt.Sprintf("[CONN > R] Internal Error Close. Closing Receiver for %s.\n", conn.Remote()))
					}
					if OnReceiverUserClosed != nil {
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
				receivedLength, err := ReadMessage(conn.Conn, recvWorkspace)
				if !shouldContinue() {
					return
				}
				msg := make([]byte, receivedLength)
				copy(msg, recvWorkspace[:receivedLength])
				select {
				case <-conn.connUserClose:
					if InfoLogger != nil {
						InfoLogger(fmt.Sprintf("[CONN > R] Signaled. Closing receiver for %s.\n", conn.Remote()))
					}
					if OnReceiverUserClosed != nil {
						OnReceiverUserClosed(conn)
					}
					return
				case <-conn.internalConnErrorClose:
					if InfoLogger != nil {
						InfoLogger(fmt.Sprintf("[CONN > R] Internal Error Close. Closing Receiver for %s.\n", conn.Remote()))
					}
					if OnReceiverUserClosed != nil {
						OnReceiverErrorClosed(conn)
					}
					return
				default:
					if err != nil {
						conn.errorClose(err, "reading message")
						if OnReceiverUserClosed != nil {
							OnReceiverErrorClosed(conn)
						}
						return
					}
					pipe <- msg
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
					if OnReceiverUserClosed != nil {
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
					if OnReceiverUserClosed != nil {
						OnReceiverErrorClosed(conn)
					}
					return
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
						conn.errorClose(err, "receiving and writing to buffer")
						if OnReceiverUserClosed != nil {
							OnReceiverErrorClosed(conn)
						}
						return
					}
					buffered += w
				}

			}
		}()

		go func() {
			conn.closing.Add(1)
			defer func() {
				conn.closing.Done()
			}()
			var shouldContinue = func() bool {
				select {
				case <-conn.connUserClose:
					if InfoLogger != nil {
						InfoLogger(fmt.Sprintf("[CONN] Signaled. Closing resolver for %s.\n", conn.Remote()))
					}
					if OnReceiverUserClosed != nil {
						OnReceiverUserClosed(conn)
					}
					return false
				case <-conn.internalConnErrorClose:
					if InfoLogger != nil {
						InfoLogger(fmt.Sprintf("[CONN] Internal Error Close. Closing resolver for %s.\n", conn.Remote()))
					}
					if OnReceiverUserClosed != nil {
						OnReceiverErrorClosed(conn)
					}
					return false
				default:
					return true
				}
			}
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
					if !shouldContinue() {
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
					if !shouldContinue() {
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
				pipe <- msg
			}
		}()
	}
	return pipe

}
