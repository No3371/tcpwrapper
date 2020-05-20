package tcpwrapper

import (
	"encoding/binary"
	"errors"
	"net"
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
var RAW_STREAM = false

var sharedInterruptedByUser interrupted = interrupted{e: "interrupted by user"}
var sharedInterruptedByError interrupted = interrupted{e: "interrupted because of error"}
var messageSizeBiggerThenBuffer error = errors.New("message size resolved is bigger then using buffer")

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

func ReadBytesFromConn(conn net.Conn, dest []byte) error {
	read := 0
	for read < len(dest) {
		i, err := conn.Read(dest[read:])
		if err != nil {
			if LowSpamLogger != nil {
				LowSpamLogger("ReadBytes() error: ", err)
			}
			return err
		}
		read += i
	}
	return nil
}

func ReadMessageFromConn(conn net.Conn, buffer []byte) (uint32, error) {
	var length uint32
	if USE_BIG_ENDIAN {
		length = binary.BigEndian.Uint32(buffer[:4])
	} else {
		length = binary.LittleEndian.Uint32(buffer[:4])
	}
	if length > uint32(len(buffer)) {
		return 4, messageSizeBiggerThenBuffer
	}
	if err := ReadBytesFromConn(conn, buffer[:length]); err != nil {
		return 0, err
	}
	return length, nil
}

func (session *ConnSession) ReadMessage(buffer []byte, interruptor interruptorFunc) (uint32, error) {
	if err := session.ReadBytes(buffer[:4], interruptor); err != nil {
		if LowSpamLogger != nil {
			LowSpamLogger("ReadMessage() error: ", err)
		}
		return 0, err
	}
	var length uint32
	if USE_BIG_ENDIAN {
		length = binary.BigEndian.Uint32(buffer[:4])
	} else {
		length = binary.LittleEndian.Uint32(buffer[:4])
	}

	if length > uint32(len(buffer)) {
		return 4, messageSizeBiggerThenBuffer
	}

	if LowSpamLogger != nil {
		LowSpamLogger("ReadBytes(): ", length)
	}
	if err := session.ReadBytes(buffer[:length], interruptor); err != nil {
		if LowSpamLogger != nil {
			LowSpamLogger("ReadMessage() interruptor error: ", err)
		}
		return 0, err
	}

	if LowSpamLogger != nil {
		LowSpamLogger("ReadBytes(): %d", length)
	}
	return length, nil
}
