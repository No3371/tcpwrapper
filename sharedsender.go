package tcpwrapper

import (
	"encoding/binary"
	"sync"
)

type SharedSender struct {
	sendOpPool  *sync.Pool
	sendOpQueue chan *sendOp
	bufferSize  int
	onError     func(cs *ConnSession, err error)
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

func NewSharedSender(queueSize int, onError func(cs *ConnSession, err error)) *SharedSender {
	ss := &SharedSender{
		sendOpPool:  &sync.Pool{},
		sendOpQueue: make(chan *sendOp, queueSize),
		onError:     onError,
	}
	ss.sendOpPool.New = func() interface{} {
		return &sendOp{}
	}
	return ss
}

func (ss *SharedSender) Loop(closeSingalOverwrite chan struct{}) (closeSignal chan struct{}) {
	if closeSingalOverwrite != nil {
		closeSignal = closeSingalOverwrite
	} else {
		closeSignal = make(chan struct{})
	}
	buffer := make([]byte, ss.bufferSize)
	go func() {
		for {
			select {
			case <-closeSignal:
				close(ss.sendOpQueue)
				ss.sendOpPool = nil
				ss.onError = nil
				return
			case sOp := <-ss.sendOpQueue:
				binary.BigEndian.PutUint32(buffer[:4], uint32(len(sOp.msg)))
				copy(buffer[4:], sOp.msg)
				err := sOp.subject.WriteBytes(buffer[:len(sOp.msg)+4], nil)
				if err != nil {
					ss.onError(sOp.subject, err)
				}
			}
		}
	}()
	return closeSignal
}
