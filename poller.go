package tcpwrapper

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/smallnest/epoller"
)

var EpollAnalysis bool = true
var EpollWorkerRatio int = 10
var avgEpollWaitTime int64
var avgOtherTime int64
var avgWaitDrainTime int64
var avgWaitedConns float64
var avgDispatched float64
var epollAnalysisSamples int64
var analysisLock sync.RWMutex
var createdEpollers *uint32
var totalMakeMoreWorkerTries int

var GlobalReaderPool = true
var globalReaderJobQueue chan *readOperation
var GlobalReaderJobQueueCapacity int = 1024
var GlobalReaderConcurrency int = 256
var globalResolveJobQueue chan *readOperation
var GlobalResolveJobQueueCapacity int = 128
var GlobalResolverConcurrency int = 4
var GlobalOnRecv func(source *ConnSession, msg []byte, resolverAsset interface{})
var GlobalResolverAssetNew func() interface{}
var GlobalModeBufferSize int = 1024
var GlobalModeCloseSignal = make(chan struct{})

func Init() {
	if EpollAnalysis {
		go func() {
			timeout := time.Second
			t := time.NewTimer(timeout)
			lastAvgEWT := avgEpollWaitTime
			lastAvgOT := avgOtherTime
			lastAvgWDT := avgWaitDrainTime
			lastAvgWC := avgWaitedConns
			lastAvgD := avgDispatched
			createdEpollers = new(uint32)
			for {
				analysisLock.Lock()
				if lastAvgEWT != avgEpollWaitTime || lastAvgOT != avgOtherTime || lastAvgWDT != avgWaitDrainTime || lastAvgWC != avgWaitedConns || lastAvgD != avgDispatched {
					log.Printf("[EPOLL-A] AvgEpollWait: %v, AvgOtherTime: %v, AvgWaitDrainTime %v, AvgWaitedConns: %f, AvgDispatched: %f, MakeMoreWorkers: %d, (EpollReceivers: %d)",
						time.Duration(avgEpollWaitTime),
						time.Duration(avgOtherTime),
						time.Duration(avgWaitDrainTime),
						avgWaitedConns,
						avgDispatched,
						totalMakeMoreWorkerTries,
						atomic.LoadUint32(createdEpollers))
				}
				lastAvgEWT = avgEpollWaitTime
				lastAvgOT = avgOtherTime
				lastAvgWDT = avgWaitDrainTime
				lastAvgWC = avgWaitedConns
				lastAvgD = avgDispatched
				analysisLock.Unlock()
				t.Reset(timeout)
				<-t.C
			}
		}()
	}

	if GlobalReaderPool {
		globalReaderJobQueue = make(chan *readOperation, GlobalReaderJobQueueCapacity)
		globalResolveJobQueue = make(chan *readOperation, GlobalResolveJobQueueCapacity)
		startGlobalDispatchedReaders(GlobalModeCloseSignal)
		startGlobalResolvers(GlobalModeCloseSignal)
		if InfoLogger != nil {
			InfoLogger("TcpWrapper started in global worker mode.")
		}
	}
}

type SharedEpollReceiver struct {
	init bool
	size int
	epoller.Poller
	externalEventChan  chan *epollerCSEvent
	OnRecv             func(source *ConnSession, msg []byte)
	failedToDrainConns chan *ConnSession
	inverseMap         map[net.Conn]*ncProfile
	bufferSize         int
	pendingRead        chan *readOperation
	connIsOccupied     map[net.Conn]bool
	pendingReadAgain   map[net.Conn]struct{}
	finishedResolve    chan *readOperation
	lock               *sync.Mutex
}

type ncProfile struct {
	inverseRef       *ConnSession
	buffer           *bytes.Buffer
	pendingMsgToRead uint32
}

func startGlobalResolvers(closeSignal <-chan struct{}) {

	for c := 0; c < GlobalResolverConcurrency; c++ {
		go func() {
			tmpReadBuffer := make([]byte, GlobalModeBufferSize)
			var asset interface{}
			if GlobalResolverAssetNew != nil {
				asset = GlobalResolverAssetNew()
			}
		resolveLoop:
			for {
				select {
				case <-closeSignal:
					return
				case rOp := <-globalResolveJobQueue:
					if LowSpamLogger != nil {
						LowSpamLogger("Resolving ", rOp.conn.RemoteAddr().String())
					}
					if pl := rOp.profile.pendingMsgToRead; pl != 0 {
						if LowSpamLogger != nil {
							LowSpamLogger("Found there's a message length pended to be resolve.")
						}
						if uint32(rOp.profile.buffer.Len()) < pl {
							if LowSpamLogger != nil {
								LowSpamLogger("Still not enough bytes buffered, skip.")
							}

							continue resolveLoop
						}
						read := 0
						for uint32(read) < pl {
							r, err := rOp.profile.buffer.Read(tmpReadBuffer[read:pl])
							if err != nil {
								rOp.err = err
								rOp.epoller.finishedResolve <- rOp
								continue resolveLoop
							}
							read += r
						}
						msg := make([]byte, read)
						copy(msg, tmpReadBuffer[:read])
						GlobalOnRecv(rOp.profile.inverseRef, msg, asset)
						rOp.profile.pendingMsgToRead = 0
					}

					for rOp.profile.buffer.Len() > 4 {
						r, err := rOp.profile.buffer.Read(tmpReadBuffer[:4])
						if err != nil {
							rOp.err = err
							rOp.epoller.finishedResolve <- rOp
							continue resolveLoop
						}
						if r != 4 {
							rOp.err = errors.New("read from buffer mismatch (4)")
							rOp.epoller.finishedResolve <- rOp
							continue resolveLoop
						}
						length := binary.BigEndian.Uint32(tmpReadBuffer[:4])
						// if LowSpamLogger != nil {
						// 	LowSpamLogger("Resolved msg length: ", length)
						// }

						if length == 0 {
							if LowSpamLogger != nil {
								LowSpamLogger("Read a 0 len msg.")
							}
							continue resolveLoop
						}

						if uint32(rOp.profile.buffer.Len()) < length {
							rOp.profile.pendingMsgToRead = length
							if LowSpamLogger != nil {
								LowSpamLogger("Not enough byte buffered, pend to next time")
							}
							continue resolveLoop
						} else {
							rOp.profile.pendingMsgToRead = 0
						}

						read := 0
						for uint32(read) < length {
							r, err := rOp.profile.buffer.Read(tmpReadBuffer[read:length])
							if err != nil {
								rOp.err = err
								rOp.epoller.finishedResolve <- rOp
								continue resolveLoop
							}
							read += r
						}
						// if LowSpamLogger != nil {
						// 	LowSpamLogger("Read ", read, "bytes and making a message")
						// }
						msg := make([]byte, read)
						copy(msg, tmpReadBuffer[:read])
						GlobalOnRecv(rOp.profile.inverseRef, msg, asset)
					}
					rOp.epoller.finishedResolve <- rOp
					if LowSpamLogger != nil {
						LowSpamLogger("Finished Resolving ", rOp.conn.RemoteAddr().String())
					}
				}
			}
		}()
	}
}

func startGlobalDispatchedReaders(closeSignal <-chan struct{}) {
	for c := 0; c < GlobalReaderConcurrency; c++ {
		go func() {
			if err := recover(); err != nil {
				if ErrorLogger != nil {
					ErrorLogger("A epoll dispatch reader is exploded! Error: %s", err)
				}
			}
			tmpReadBuffer := make([]byte, GlobalModeBufferSize)
		dispatchWorkerLoop:
			for {
				select {
				case <-closeSignal:
					return
				case rOp := <-globalReaderJobQueue:
					if LowSpamLogger != nil {
						LowSpamLogger("Working to read ", rOp.conn.RemoteAddr().String())
					}
					readDeadlineSet := false
				read_more:
					if len(tmpReadBuffer) == 0 {
						if ErrorLogger != nil {
							ErrorLogger("tmpReadBuffer is 0 length, why?")
						}
					}
					r, err := rOp.conn.Read(tmpReadBuffer)
					if err != nil {
						if !readDeadlineSet {
							rOp.err = err
							rOp.drainWait <- struct{}{}
							rOp.epoller.finishedResolve <- rOp
							continue dispatchWorkerLoop
						}
						if nErr, ok := err.(net.Error); ok && nErr.Timeout() {
							if LowSpamLogger != nil {
								LowSpamLogger("Unable to read more...")
							}
							goto read_more_done
						}
					}
					if r == 0 {
						if ErrorLogger != nil {
							ErrorLogger("A 0 read happended!")
						}
					} else {
						rOp.profile.buffer.Write(tmpReadBuffer[:r])
						// if LowSpamLogger != nil {
						// 	LowSpamLogger("Read ", r, " bytes, wrote to buffer (", rOp.profile.buffer.Len())
						// }
					}
					if r == len(tmpReadBuffer) {
						if LowSpamLogger != nil {
							LowSpamLogger("It's a full read, try to read more, will timeout in 5ms.")
						}
						rOp.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 5))
						readDeadlineSet = true
						goto read_more
					} else {
						rOp.conn.SetReadDeadline(time.Time{})
					}
				read_more_done:
					if LowSpamLogger != nil {
						LowSpamLogger("Done reading %s, queue to resolve...", rOp.conn.RemoteAddr().String())
					}
					rOp.drainWait <- struct{}{}
					globalResolveJobQueue <- rOp
				}
			}
		}()
	}
	if SpamLogger != nil {
		SpamLogger(fmt.Sprintf("[CONN-EPOLL] Started %d global readers.", GlobalReaderConcurrency))
	}
}

func (ser *SharedEpollReceiver) startDispatchedReader(count int, closedSignal <-chan struct{}) {
	for c := 0; c < count; c++ {
		go func() {
			if err := recover(); err != nil {
				if ErrorLogger != nil {
					ErrorLogger("A epoll dispatch reader is exploded! Error: %s", err)
				}
			}
			tmpReadBuffer := make([]byte, ser.bufferSize)
		dispatchWorkerLoop:
			for {
				select {
				case <-closedSignal:
					return
				case rOp := <-ser.pendingRead:
					if LowSpamLogger != nil {
						LowSpamLogger("Working to read ", rOp.conn.RemoteAddr().String())
					}
					rdlSet := false
				read_more:
					if len(tmpReadBuffer) == 0 {
						if ErrorLogger != nil {
							ErrorLogger("tmpReadBuffer is 0 length, why?")
						}
					}
					r, err := rOp.conn.Read(tmpReadBuffer)
					if err != nil {
						if !rdlSet {
							rOp.err = err

							ser.finishedResolve <- rOp
							continue dispatchWorkerLoop
						}
						if nErr, ok := err.(net.Error); ok && nErr.Timeout() {
							if LowSpamLogger != nil {
								LowSpamLogger("Unable to read more...")
							}
							goto read_more_done
						}
					}
					if r == 0 {
						if ErrorLogger != nil {
							ErrorLogger("A 0 read happended!")
						}
					} else {
						rOp.profile.buffer.Write(tmpReadBuffer[:r])
						// if LowSpamLogger != nil {
						// 	LowSpamLogger("Read ", r, " bytes, wrote to buffer (", rOp.profile.buffer.Len())
						// }
					}
					if r == len(tmpReadBuffer) {
						if LowSpamLogger != nil {
							LowSpamLogger("It's a full read, try to read more, will timeout in 5ms.")
						}
						rOp.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 5))
						rdlSet = true
						goto read_more
					} else {
						rOp.conn.SetReadDeadline(time.Time{})
					}
				read_more_done:
					rOp.drainWait <- struct{}{}

					if pl := rOp.profile.pendingMsgToRead; pl != 0 {
						if LowSpamLogger != nil {
							LowSpamLogger("Found there's a message length pended to be read.")
						}
						if uint32(rOp.profile.buffer.Len()) < pl {
							if LowSpamLogger != nil {
								LowSpamLogger("Still not enough bytes buffered, skip.")
							}

							continue dispatchWorkerLoop
						}
						read := 0
						for uint32(read) < pl {
							r, err := rOp.profile.buffer.Read(tmpReadBuffer[read:pl])
							if err != nil {
								rOp.err = err
								ser.finishedResolve <- rOp
								continue dispatchWorkerLoop
							}
							read += r
						}
						msg := make([]byte, read)
						copy(msg, tmpReadBuffer[:read])
						ser.OnRecv(rOp.profile.inverseRef, msg)
						rOp.profile.pendingMsgToRead = 0
					}

				resolveLoop:
					for rOp.profile.buffer.Len() > 4 {
						r, err := rOp.profile.buffer.Read(tmpReadBuffer[:4])
						if err != nil {
							rOp.err = err
							ser.finishedResolve <- rOp
							continue dispatchWorkerLoop
						}
						if r != 4 {
							rOp.err = errors.New("read from buffer mismatch (4)")
							ser.finishedResolve <- rOp
							continue dispatchWorkerLoop
						}
						length := binary.BigEndian.Uint32(tmpReadBuffer[:4])
						// if LowSpamLogger != nil {
						// 	LowSpamLogger("Resolved msg length: ", length)
						// }

						if length == 0 {
							if LowSpamLogger != nil {
								LowSpamLogger("Read a 0 len msg.")
							}
							continue resolveLoop
						}

						if uint32(rOp.profile.buffer.Len()) < length {
							rOp.profile.pendingMsgToRead = length
							if LowSpamLogger != nil {
								LowSpamLogger("Not enough byte buffered, pend to next time")
							}
							break resolveLoop
						} else {
							rOp.profile.pendingMsgToRead = 0
						}

						read := 0
						for uint32(read) < length {
							r, err := rOp.profile.buffer.Read(tmpReadBuffer[read:length])
							if err != nil {
								rOp.err = err
								ser.finishedResolve <- rOp
								continue dispatchWorkerLoop
							}
							read += r
						}
						// if LowSpamLogger != nil {
						// 	LowSpamLogger("Read ", read, "bytes and making a message")
						// }
						msg := make([]byte, read)
						copy(msg, tmpReadBuffer[:read])
						ser.OnRecv(rOp.profile.inverseRef, msg)
					}
					ser.finishedResolve <- rOp
				}
			}
		}()
	}
	if SpamLogger != nil {
		SpamLogger(fmt.Sprintf("[CONN-EPOLL] Started %d dispatch worker.", count))
	}
}

func NewSharedEpollReceiver(count int, recvChanSize int, bufferSize int, onRecv func(source *ConnSession, msg []byte)) (ew *SharedEpollReceiver, err error) {
	e, err := epoller.NewPollerWithBuffer(count)
	if err != nil {
		e, err = epoller.NewPollerWithBuffer(count)
		if err != nil {
			return nil, err
		}
	}
	secr := &SharedEpollReceiver{
		init:              false,
		size:              count,
		Poller:            e,
		externalEventChan: make(chan *epollerCSEvent, count),
		OnRecv:            onRecv,
		inverseMap:        make(map[net.Conn]*ncProfile),
		bufferSize:        bufferSize,
		pendingRead:       make(chan *readOperation, count),
		pendingReadAgain:  make(map[net.Conn]struct{}),
		connIsOccupied:    make(map[net.Conn]bool),
		finishedResolve:   make(chan *readOperation, count),
		lock:              new(sync.Mutex),
	}
	if EpollAnalysis {
		atomic.AddUint32(createdEpollers, 1)
	}
	return secr, nil
}

func (ew *SharedEpollReceiver) RequestAdd(cs *ConnSession) bool {
	e := &epollerCSEvent{
		cs:        cs,
		eventType: true,
	}
	ew.externalEventChan <- e
	ew.Add(cs.Conn)
	return true
}

func (ew *SharedEpollReceiver) RequestRemove(cs *ConnSession, doClose bool) bool {
	e := &epollerCSEvent{
		cs:        cs,
		eventType: false,
		doClose:   doClose,
	}
	ew.externalEventChan <- e
	ew.Remove(cs.Conn)
	return true
}

//Loop is syncrons, go this.
func (ser *SharedEpollReceiver) Loop(onReadErrorAndRemoved func(cs *ConnSession, err error), closeSignal <-chan struct{}) {

	// if LowSpamLogger != nil {
	// 	LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Starting epoll loop."))
	// }
	defer func() {
		if err := recover(); err != nil {
			if ErrorLogger != nil {
				ErrorLogger("A EpollReceiver->innerLoop() is exploded! Error: ", err)
			}
		} else {
			if LowSpamLogger != nil {
				LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] A EpollReceiver->innerLoop() is down."))
			}
		}
	}()
	var epollWaitTime time.Duration
	var otherTime time.Duration
	var epollTimeMark time.Time
	var otherTimeMark time.Time
	var waitDrainTime time.Duration
	var waitDrainTimeMark time.Time
	var consumedAddEvents int
	var consumedRemoveEvents int
	var readDoneEvents int
	var readDoneErrors int
	var dispatched int
	var dispatchedReadAgain int
	var makeMoreWorkerTries int
	for {
		if EpollAnalysis {
			otherTimeMark = time.Now()
		}
		select {
		case <-closeSignal:
			ser.externalEventChan = nil
			ser.inverseMap = nil
			ser.pendingRead = nil
			return
		default:
		}

		if LowSpamLogger != nil {
			LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Waiting on epoll."))
		}
		if EpollAnalysis {
			otherTime += time.Since(otherTimeMark)
			epollTimeMark = time.Now()
		}
		conns, err := ser.WaitWithBuffer()
		if err != nil {
			if err.Error() != "bad file descriptor" {
				if ErrorLogger != nil {
					ErrorLogger(fmt.Sprintf("failed to poll: %v", err))
				}
			}
			log.Printf("!!!!!!!!!!!!Error on wait: %s", err)
			continue
		}
		if EpollAnalysis {
			epollWaitTime += time.Since(epollTimeMark)
			otherTimeMark = time.Now()
		}
		if LowSpamLogger != nil {
			LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Waited epoll events for %d conns.", len(conns)))
		}
		consumedAddEvents = 0
		consumedRemoveEvents = 0
	syncLoop:
		for {
			select {
			case e := <-ser.externalEventChan:
				if e.eventType {
					if !ser.init {
						ser.init = true
						if !GlobalReaderPool {
							if SpamLogger != nil {
								SpamLogger(fmt.Sprintf("[CONN-EPOLL] Initing default workers."))
							}
							ser.startDispatchedReader(ser.size/EpollWorkerRatio, closeSignal)
						}
					}
					if e.cs == nil {
						if ErrorLogger != nil {
							ErrorLogger("[CONN-EPOLL] asked to add a nil cs!")
						}
						continue syncLoop
					}
					ser.inverseMap[e.cs.Conn] = &ncProfile{
						inverseRef:       e.cs,
						buffer:           bytes.NewBuffer(make([]byte, 0, ser.bufferSize)),
						pendingMsgToRead: 0,
					}
					ser.connIsOccupied[e.cs.Conn] = false
					consumedAddEvents++
				} else {
					if e.doClose {
						close(e.cs.connUserClose)
						if err := e.cs.Conn.Close(); err != nil {

						}
					}
					delete(ser.connIsOccupied, e.cs.Conn)
					delete(ser.inverseMap, e.cs.Conn)
					if SpamLogger != nil {
						SpamLogger(fmt.Sprintf("[CONN-EPOLL] Removed %s from this poller.", e.cs.Remote()))
					}
					consumedRemoveEvents++
				}
			default:
				break syncLoop
			}
		}

		readDoneEvents = 0
		readDoneErrors = 0
		if LowSpamLogger != nil {
			LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Checking resolved conns"))
		}
	checkReadDoneLoop:
		for {
			select {
			case e := <-ser.finishedResolve:
				if !ser.connIsOccupied[e.conn] {
					log.Fatalf("The conn is not flagged being read but the poller received a finish event!")
				}
				ser.connIsOccupied[e.conn] = false
				if e.err != nil {
					if LowSpamLogger != nil {
						LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Handling worker error on %s: %s", e.conn.RemoteAddr().String(), e.err))
					}
					if ErrorLogger != nil {
						ErrorLogger("[EP] An error occured when reading message from ", e.conn.RemoteAddr().String(), ", error: ", e.err)
					}
					ser.Remove(e.conn)
					if onReadErrorAndRemoved != nil {
						onReadErrorAndRemoved(ser.inverseMap[e.conn].inverseRef, e.err)
					}
					delete(ser.inverseMap, e.conn)
					readDoneErrors++
				}
				readDoneEvents++
			default:
				break checkReadDoneLoop
			}
		}

		if LowSpamLogger != nil {
			LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Reading waited conns..."))
		}
		waitDrain := make(chan struct{}, len(conns))
		dispatched = 0
		dispatchedReadAgain = 0
	readEpolledConns:
		for _, conn := range conns { // Any epoll-waited conn is added to epoll, buy maybe still pending to be registered
			if conn == nil {
				if SpamLogger != nil {
					SpamLogger(fmt.Sprintf("[CONN-EPOLL] A nil conn waited!."))
				}
				continue readEpolledConns
			}

			if _, exist := ser.inverseMap[conn]; !exist {
				// It's possible that the conn is epoll-waited but is requested to be removed
				if LowSpamLogger != nil {
					LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Epoll-waited %s but it's not in the registry. Take it as removed.", conn.RemoteAddr().String()))
				}
				continue readEpolledConns
			}

			beingRead, registered := ser.connIsOccupied[conn] // We epoll-waited a conn while it's still bring read
			if !registered {
				log.Fatalf("Not registed beingRead!")
			}

			if beingRead {
				if _, alreadyPendingToReadAgain := ser.pendingReadAgain[conn]; !alreadyPendingToReadAgain {
					ser.pendingReadAgain[conn] = struct{}{}
					if LowSpamLogger != nil {
						LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Added %s to pendingReadAgain!", conn.RemoteAddr().String()))
					}
				}
				continue readEpolledConns
			} else {
				rOp := &readOperation{
					epoller:   ser,
					conn:      conn,
					profile:   ser.inverseMap[conn],
					drainWait: waitDrain,
				}
				if GlobalReaderPool {
					globalReaderJobQueue <- rOp
				} else {
					select {
					case ser.pendingRead <- rOp:
					default:
						makeMoreWorkerTries++
						ser.startDispatchedReader(10, closeSignal)
						ser.pendingRead <- rOp
					}
				}
				dispatched++
				// if LowSpamLogger != nil {
				// 	LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Dispatched %s to be read!", conn.RemoteAddr().String()))
				// }
				ser.connIsOccupied[conn] = true
				if _, p := ser.pendingReadAgain[conn]; p {
					// Is this possible? We epoll-waited the conn again but we have not drain it...?
					// If it's pending to be read again, this read should drain it, so we remove the flag
					delete(ser.pendingReadAgain, conn)
					if LowSpamLogger != nil {
						LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Removed %s from pendingReadAgain!", conn.RemoteAddr().String()))
					}
				}
			}
		}

		if LowSpamLogger != nil {
			LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Reading pending(read-again) conns..."))
		}
	doReadAgain:
		for pendingAgainConn := range ser.pendingReadAgain { // If it's pending to be read again but it's not epoll-waited thsi time (Already epoll-waited in previous loop), we dispatch it to be read too
			if ser.connIsOccupied[pendingAgainConn] {
				continue doReadAgain
			}
			rOp := &readOperation{
				epoller:   ser,
				conn:      pendingAgainConn,
				profile:   ser.inverseMap[pendingAgainConn],
				drainWait: waitDrain,
			}
			if GlobalReaderPool {
				globalReaderJobQueue <- rOp
			} else {
				select {
				case ser.pendingRead <- rOp:
				default:
					makeMoreWorkerTries++
					ser.startDispatchedReader(10, closeSignal)
					ser.pendingRead <- rOp
				}
			}
			dispatchedReadAgain++
			ser.connIsOccupied[pendingAgainConn] = true
			delete(ser.pendingReadAgain, pendingAgainConn)
		}

		if EpollAnalysis {
			otherTime += time.Since(otherTimeMark)
			waitDrainTimeMark = time.Now()
		}
		if LowSpamLogger != nil {
			LowSpamLogger(fmt.Sprintf("[CONN-EPOLL] Waiting for drain signals..."))
		}
		for d := 0; d < dispatched+dispatchedReadAgain; d++ {
			<-waitDrain
		}

		if EpollAnalysis {
			waitDrainTime += time.Since(waitDrainTimeMark)
			otherTimeMark = time.Now()
			if LowSpamLogger != nil {
				LowSpamLogger(
					fmt.Sprintf("[CONN-EPOLL] Total: %d, ConsumedAdd/Remove: %d/ %d, (chan: %d), read done: %d (error: %d), dispatched: %d, dispatchedReadAgain: %d",
						len(ser.inverseMap),
						consumedAddEvents,
						consumedRemoveEvents,
						len(ser.externalEventChan),
						readDoneEvents,
						readDoneErrors,
						dispatched,
						dispatchedReadAgain))
			}
			analysisLock.Lock()
			if epollAnalysisSamples == 0 {
				avgOtherTime = int64(otherTime)
				avgEpollWaitTime = int64(epollWaitTime)
			} else {
				if int64(otherTime) > avgOtherTime {
					avgOtherTime += int64(otherTime) / epollAnalysisSamples
				} else {
					avgOtherTime -= int64(otherTime) / epollAnalysisSamples
				}
				if int64(epollWaitTime) > avgEpollWaitTime {
					avgEpollWaitTime += int64(epollWaitTime) / epollAnalysisSamples
				} else {
					avgEpollWaitTime -= int64(epollWaitTime) / epollAnalysisSamples
				}
				if int64(waitDrainTime) > avgWaitDrainTime {
					avgWaitDrainTime += int64(waitDrainTime) / epollAnalysisSamples
				} else {
					avgWaitDrainTime -= int64(waitDrainTime) / epollAnalysisSamples
				}
				if float64(len(conns)) > avgWaitedConns {
					avgWaitedConns += float64(len(conns)) / float64(epollAnalysisSamples)
				} else {
					avgWaitedConns -= float64(len(conns)) / float64(epollAnalysisSamples)
				}
				if float64(dispatched+dispatchedReadAgain) > avgDispatched {
					avgDispatched += float64(dispatched+dispatchedReadAgain) / float64(epollAnalysisSamples)
				} else {
					avgDispatched -= float64(dispatched+dispatchedReadAgain) / float64(epollAnalysisSamples)
				}
			}
			epollAnalysisSamples++
			analysisLock.Unlock()
			otherTime += time.Since(otherTimeMark)
			totalMakeMoreWorkerTries += makeMoreWorkerTries
			makeMoreWorkerTries = 0
			otherTime = 0
			epollWaitTime = 0
			waitDrainTime = 0
		}

	}
}

type epollerCSEvent struct {
	cs        *ConnSession
	eventType bool
	doClose   bool
}
type netConnError struct {
	conn net.Conn
	err  error
}

type readOperation struct {
	epoller   *SharedEpollReceiver
	conn      net.Conn
	profile   *ncProfile
	drainWait chan struct{}
	err       error
}
