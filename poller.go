package tcpwrapper

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/smallnest/epoller"
)

type SharedEpollerConnReader struct {
	closed      chan struct{}
	pendingRead chan net.Conn
}

type SharedEpollReceiver struct {
	epoller.Poller
	externalEventChan chan interface{}
	readerErrorChan   chan *netConnError
	OnRecv            func(source *ConnSession, msg []byte)
	inverseMap        map[net.Conn]*ncProfile
	bufferSize        int
	pendingRead       chan *readOperation
	closedSignal      chan struct{}
	lock              *sync.Mutex
}

type ncProfile struct {
	inverseRef       *ConnSession
	buffer           *bytes.Buffer
	pendingMsgToRead uint32
}

func (secr *SharedEpollReceiver) startDispatchedReader(count int) {
	for c := 0; c < count; c++ {
		tmpReadBuffer := make([]byte, secr.bufferSize)
		go func() {
			for {
				select {
				case <-secr.closedSignal:
					return
				case rOp := <-secr.pendingRead:
					secr.lock.Lock()
					prof, exist := secr.inverseMap[rOp.conn]
					if !exist {
						if ErrorLogger != nil {
							ErrorLogger("Failed to get the nc profile from a pending read subject: %s", rOp.conn.RemoteAddr().String())
						}
						continue
					}
					secr.lock.Unlock()
					cBuf := prof.buffer
				read_more:
					r, err := rOp.conn.Read(tmpReadBuffer)
					if err != nil {
						secr.readerErrorChan <- &netConnError{
							conn: rOp.conn,
							err:  err,
						}
						continue
					}
					cBuf.Write(tmpReadBuffer[:r])
					if r == len(tmpReadBuffer) {
						goto read_more
					}

					if pl := prof.pendingMsgToRead; pl != 0 {
						read := 0
						for uint32(read) < pl {
							r, err := cBuf.Read(tmpReadBuffer[read:pl])
							secr.readerErrorChan <- &netConnError{
								conn: rOp.conn,
								err:  err,
							}
							read += r
						}
						msg := make([]byte, read)
						copy(msg, tmpReadBuffer[:read])
						secr.OnRecv(prof.inverseRef, msg)
					}

					for cBuf.Len() > 4 {
						r, err := cBuf.Read(tmpReadBuffer[:4])
						if err != nil {
							secr.readerErrorChan <- &netConnError{
								conn: rOp.conn,
								err:  err,
							}
							break
						}
						if r != 4 {
							secr.readerErrorChan <- &netConnError{
								conn: rOp.conn,
								err:  errors.New("read from buffer mismatch (4)"),
							}
							break
						}
						length := binary.BigEndian.Uint32(tmpReadBuffer[:4])
						if uint32(cBuf.Len()) < length {
							prof.pendingMsgToRead = length
							break
						} else {
							prof.pendingMsgToRead = 0
						}

						read := 0
						for uint32(read) < length {
							r, err := cBuf.Read(tmpReadBuffer[read:length])
							secr.readerErrorChan <- &netConnError{
								conn: rOp.conn,
								err:  err,
							}
							read += r
						}
						msg := make([]byte, read)
						copy(msg, tmpReadBuffer[:read])
						secr.OnRecv(prof.inverseRef, msg)
					}

					rOp.wg.Done()
				}
			}
		}()
	}
}

func NewSharedEpollReceiver(count int, eventChanSize int, recvChanSize int, bufferSize int, onRecv func(source *ConnSession, msg []byte)) (ew *SharedEpollReceiver, err error) {
	e, err := epoller.NewPollerWithBuffer(count)
	if err != nil {
		e, err = epoller.NewPollerWithBuffer(count)
		if err != nil {
			return nil, err
		}
	}
	secr := &SharedEpollReceiver{
		Poller:            e,
		externalEventChan: make(chan interface{}, eventChanSize),
		readerErrorChan:   make(chan *netConnError, count),
		OnRecv:            onRecv,
		inverseMap:        make(map[net.Conn]*ncProfile),
		bufferSize:        bufferSize,
		pendingRead:       make(chan *readOperation, count),
		lock:              new(sync.Mutex),
	}
	secr.startDispatchedReader(count)
	return secr, nil
}

func (ew *SharedEpollReceiver) RequestAdd(cs *ConnSession) {
	ew.externalEventChan <- &epollerCSEvent{
		cs:        cs,
		eventType: true,
	}
}

func (ew *SharedEpollReceiver) RequestRemove(cs *ConnSession) {
	ew.externalEventChan <- &epollerCSEvent{
		cs:        cs,
		eventType: false,
	}
}

func (ew *SharedEpollReceiver) Loop(onReadErrorAndRemoved func(cs *ConnSession, err error), closeSingalOverwrite <-chan struct{}) (closeSingal <-chan struct{}) {
	if closeSingalOverwrite != nil {
		closeSingal = closeSingalOverwrite
	} else {
		closeSingal = make(chan struct{})
	}
	go func() {
		for {
			select {
			case <-closeSingal:
				ew.externalEventChan = nil
				ew.inverseMap = nil
				ew.pendingRead = nil
				ew.readerErrorChan = nil
				return
			case e := <-ew.externalEventChan:
				switch e := e.(type) {
				case epollerCSEvent:
					if e.eventType {
						ew.Add(e.cs.Conn)
						ew.lock.Lock()
						defer ew.lock.Unlock()
						ew.inverseMap[e.cs.Conn] = &ncProfile{
							inverseRef:       e.cs,
							buffer:           bytes.NewBuffer(make([]byte, ew.bufferSize)),
							pendingMsgToRead: 0,
						}
						if LowSpamLogger != nil {
							LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Added a cs."))
						}
					} else {
						ew.lock.Lock()
						defer ew.lock.Unlock()
						delete(ew.inverseMap, e.cs.Conn)
						ew.Remove(e.cs.Conn)
						if LowSpamLogger != nil {
							LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Removed a cs."))
						}
					}
				}
			}
		}

	}()
	go func() {
		defer func() {
			if err := recover(); err != nil {
				if ErrorLogger != nil {
					ErrorLogger("A EpollReceiver->Loop() is exploded! Error: %s", err)
				}
			}
		}()
		for {
			select {
			case <-closeSingal:
				ew.externalEventChan = nil
				ew.inverseMap = nil
				ew.pendingRead = nil
				ew.readerErrorChan = nil
				return
			default:
				if LowSpamLogger != nil {
					LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Waiting on epoll."))
				}
				conns, err := ew.WaitWithBuffer()
				if err != nil {
					if err.Error() != "bad file descriptor" {
						if ErrorLogger != nil {
							ErrorLogger(fmt.Sprintf("failed to poll: %v", err))
						}
					}
					continue
				}
				dispatched := new(sync.WaitGroup)
				if LowSpamLogger != nil {
					LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Dispatching %d conns to be read.", len(conns)))
				}
				for _, conn := range conns {
					rOp := &readOperation{
						conn: conn,
						wg:   dispatched,
					}
					select {
					case ew.pendingRead <- rOp:
					default:
						ew.startDispatchedReader(10)
						ew.pendingRead <- rOp
					}
				}
				dispatched.Wait()
				if LowSpamLogger != nil {
					LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Workers finished reading jobs on %d conns.", len(conns)))
				}

			clearErrorLoop:
				for {
					select {
					case e := <-ew.readerErrorChan:
						if ErrorLogger != nil {
							ErrorLogger("[EP] An error occured when reading message from %s: %s", e.conn.RemoteAddr().String(), err)
						}
						ew.lock.Lock()
						defer ew.lock.Unlock()
						ew.Remove(e.conn)
						if onReadErrorAndRemoved != nil {
							onReadErrorAndRemoved(ew.inverseMap[e.conn].inverseRef, e.err)
						}
						delete(ew.inverseMap, e.conn)
					default:
						break clearErrorLoop
					}
				}
			}
		}
	}()
	return closeSingal
}

type epollerCSEvent struct {
	cs        *ConnSession
	eventType bool
}
type netConnError struct {
	conn net.Conn
	err  error
}

type readOperation struct {
	conn net.Conn
	wg   *sync.WaitGroup
}
