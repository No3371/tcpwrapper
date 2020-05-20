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

func (ss *SharedSender) Loop() (closeSingal chan struct{}) {
	buffer := make([]byte, ss.bufferSize)
	for {
		select {
		case <-closeSingal:
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
}
