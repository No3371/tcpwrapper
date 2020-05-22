package tcpwrapper

import (
	"encoding/binary"
	"log"
	"sync"
	"time"
)

var SenderAnalysis bool = true
var avgWriteTime int64
var senderAnalysisLock *sync.Mutex
var SenderAnalysisSamples int64

func init() {
	if !SenderAnalysis {
		return
	}
	senderAnalysisLock = new(sync.Mutex)
	go func() {
		timeout := time.Second
		timer := time.NewTimer(timeout)
		lastWS := avgWriteTime
		for {
			timer.Reset(timeout)
			<-timer.C
			senderAnalysisLock.Lock()
			if lastWS != avgWriteTime {
				log.Printf("[S-A] AvgWriteTime: %v", time.Duration(avgWriteTime))
			}
			lastWS = avgWriteTime
			senderAnalysisLock.Unlock()
		}
	}()
}

type SharedSender struct {
	sendOpPool  *sync.Pool
	sendOpQueue chan *sendOp
	bufferSize  int
}

type sendOp struct {
	subject *ConnSession
	msg     []byte
}

func (ss *SharedSender) PendSend(subject *ConnSession, msg []byte) {
	sOp := ss.sendOpPool.Get().(*sendOp)
	sOp.subject = subject
	sOp.msg = msg
	ss.sendOpQueue <- sOp
}

func NewSharedSender(queueSize int, bufferSize int) *SharedSender {
	return &SharedSender{
		sendOpPool: &sync.Pool{
			New: func() interface{} {
				return new(sendOp)
			},
		},
		sendOpQueue: make(chan *sendOp, queueSize),
		bufferSize:  bufferSize,
	}

}

func (ss *SharedSender) Loop(onError func(cs *ConnSession, err error), closeSingalOverwrite <-chan struct{}) (closeSignal <-chan struct{}) {
	if closeSingalOverwrite != nil {
		closeSignal = closeSingalOverwrite
	} else {
		closeSignal = make(chan struct{})
	}
	buffer := make([]byte, ss.bufferSize)
	writeTimeMark := time.Time{}
	go func() {
		defer func() {
			if err := recover(); err != nil {
				if ErrorLogger != nil {
					ErrorLogger("A SharedSender->Loop() is exploded! Error: %s", err)
				}
			}
		}()
		for {
			select {
			case <-closeSignal:
				close(ss.sendOpQueue)
				ss.sendOpPool = nil
				return
			case sOp := <-ss.sendOpQueue:
				if SenderAnalysis {
					writeTimeMark = time.Now()
				}
				binary.BigEndian.PutUint32(buffer[:4], uint32(len(sOp.msg)))
				copy(buffer[4:], sOp.msg)
				err := sOp.subject.WriteBytes(buffer[:len(sOp.msg)+4], nil)
				if err != nil {
					onError(sOp.subject, err)
				}
				ss.sendOpPool.Put(sOp)
				if SenderAnalysis {
					senderAnalysisLock.Lock()
					if SenderAnalysisSamples == 0 {
						avgWriteTime = int64(time.Since(writeTimeMark))
					} else {
						elapsed := int64(time.Since(writeTimeMark))
						if elapsed > avgWriteTime {
							avgWriteTime += elapsed / SenderAnalysisSamples
						} else if elapsed < avgWaitDrainTime {
							avgWriteTime -= elapsed / SenderAnalysisSamples
						}
					}
					SenderAnalysisSamples++
					senderAnalysisLock.Unlock()
				}
			innerSend:
				for {
					select {
					case sOp := <-ss.sendOpQueue:
						if SenderAnalysis {
							writeTimeMark = time.Now()
						}
						binary.BigEndian.PutUint32(buffer[:4], uint32(len(sOp.msg)))
						copy(buffer[4:], sOp.msg)
						err := sOp.subject.WriteBytes(buffer[:len(sOp.msg)+4], nil)
						if err != nil {
							onError(sOp.subject, err)
						}
						ss.sendOpPool.Put(sOp)
						if SenderAnalysis {
							senderAnalysisLock.Lock()
							if SenderAnalysisSamples == 0 {
								avgWriteTime = int64(time.Since(writeTimeMark))
							} else {
								elapsed := int64(time.Since(writeTimeMark))
								if elapsed > avgWriteTime {
									avgWriteTime += elapsed / SenderAnalysisSamples
								} else if elapsed < avgWaitDrainTime {
									avgWriteTime -= elapsed / SenderAnalysisSamples
								}
							}
							SenderAnalysisSamples++
							senderAnalysisLock.Unlock()
						}
					default:
						break innerSend
					}
				}
			}
		}
	}()
	return closeSignal
}
