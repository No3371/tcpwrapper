package tcpwrapper

import (
	"encoding/binary"
	"sync"
)

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

func NewSharedSender(queueSize int) *SharedSender {
	ss := &SharedSender{
		sendOpPool:  &sync.Pool{},
		sendOpQueue: make(chan *sendOp, queueSize),
	}
	ss.sendOpPool.New = func() interface{} {
		return &sendOp{}
	}
	return ss
}

func (ss *SharedSender) Loop(onError func(cs *ConnSession, err error), closeSingalOverwrite <-chan struct{}) (closeSignal <-chan struct{}) {
	if closeSingalOverwrite != nil {
		closeSignal = closeSingalOverwrite
	} else {
		closeSignal = make(chan struct{})
	}
	buffer := make([]byte, ss.bufferSize)
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
				binary.BigEndian.PutUint32(buffer[:4], uint32(len(sOp.msg)))
				copy(buffer[4:], sOp.msg)
				err := sOp.subject.WriteBytes(buffer[:len(sOp.msg)+4], nil)
				if err != nil {
					onError(sOp.subject, err)
				}
			}
		}
	}()
	return closeSignal
}
