package tcpwrapper

import (
	"encoding/binary"
)

var ErrorLogger func(v ...interface{})
var InfoLogger func(v ...interface{})
var SpamLogger func(v ...interface{})
var LowSpamLogger func(v ...interface{})
var OnReceiverUserClosed func(conn *ConnSession)
var OnSenderUserClosed func(conn *ConnSession)
var OnReceiverErrorClosed func(conn *ConnSession)
var OnSenderErrorClosed func(conn *ConnSession)

var BUFFERED_SEND_INTERVAL_MS uint = 5
var USE_BIG_ENDIAN = true

var sharedInterruptedByUser interrupted = interrupted{e: "interrupted by user"}
var sharedInterruptedByError interrupted = interrupted{e: "interrupted because of error"}

type interrupted struct {
	e string
}

func (e *interrupted) Error() string {
	return e.e
}

// ReadBytes needs a interruptor func to execute between Write() calls.
func (session *ConnSession) ReadBytes(dest []byte, interruptor interruptorFunc) error {
	read := 0
	for read < len(dest) {
		i, err := session.Conn.Read(dest[read:])
		if err != nil {
			if LowSpamLogger != nil {
				LowSpamLogger("ReadBytes() error: ", err)
			}
			return err
		}
		read += i
		if interruptor != nil {
			if err := interruptor(session); err != nil {
				if LowSpamLogger != nil {
					LowSpamLogger("ReadBytes() interruptor error: ", err)
				}
				return err
			}
		}
	}
	return nil
}

// WriteBytes needs a interruptor func to execute between Write() calls.
func (session *ConnSession) WriteBytes(message []byte, interruptor interruptorFunc) error {
	written := 0
	for written < len(message) {
		i, err := session.Conn.Write(message[written:])
		if err != nil {
			if LowSpamLogger != nil {
				LowSpamLogger("WriteBytes() error: ", err)
			}
			return err
		}
		written += i
		if interruptor != nil {
			if err := interruptor(session); err != nil {
				if LowSpamLogger != nil {
					LowSpamLogger("WriteBytes() interruptor error: ", err)
				}
				return err
			}
		}
	}
	return nil
}

// MakeMessage prepend the length before the message, the workspace must be at least 4 bytes long then the message
func MakeMessage(message []byte, workspace []byte) {
	binary.BigEndian.PutUint32(workspace, uint32(len(message)))
	copy(workspace[4:], message)
}

func (session *ConnSession) ReadMessage(buffer []byte, interruptor interruptorFunc) (uint32, error) {
	if err := session.ReadBytes(buffer[:4], interruptor); err != nil {
		if LowSpamLogger != nil {
			LowSpamLogger("ReadMessage() error: ", err)
		}
		return 0, err
	}
	var l uint32
	if USE_BIG_ENDIAN {
		l = binary.BigEndian.Uint32(buffer[:4])
	} else {
		l = binary.LittleEndian.Uint32(buffer[:4])
	}

	if LowSpamLogger != nil {
		LowSpamLogger("ReadBytes(): ", l)
	}
	if err := session.ReadBytes(buffer[:l], interruptor); err != nil {
		if LowSpamLogger != nil {
			LowSpamLogger("ReadMessage() interruptor error: ", err)
		}
		return 0, err
	}

	if LowSpamLogger != nil {
		LowSpamLogger("ReadBytes(): %d", l)
	}
	return l, nil
}
