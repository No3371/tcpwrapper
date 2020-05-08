package tcpwrapper

import (
	"encoding/binary"
	"net"
)

var ErrorLogger func(v ...interface{})
var InfoLogger func(v ...interface{})
var SpamLogger func(v ...interface{})
var OnReceiverUserClosed func(conn *ConnSession)
var OnSenderUserClosed func(conn *ConnSession)
var OnReceiverErrorClosed func(conn *ConnSession)
var OnSenderErrorClosed func(conn *ConnSession)

var BUFFERED_SEND_INTERVAL_MS uint = 5
var USE_BIG_ENDIAN = true
var sharedInterrupted interrupted = interrupted{}

type interrupted struct {
}

func (e *interrupted) Error() string {
	return "interrupted internally"
}

func ReadBytes(conn net.Conn, dest []byte, interruptor func() bool) error {
	read := 0
	for read < len(dest) {
		i, err := conn.Read(dest[read:])
		if err != nil {
			return err
		}
		read += i
		if interruptor != nil && !interruptor() {
			return &sharedInterrupted
		}
	}
	return nil
}

func WriteBytes(conn net.Conn, message []byte, interruptor func() bool) error {
	written := 0
	for written < len(message) {
		i, err := conn.Write(message[written:])
		if err != nil {
			return err
		}
		written += i
		if interruptor != nil && !interruptor() {
			return &sharedInterrupted
		}
	}
	return nil
}

func ReadMessage(conn net.Conn, buffer []byte, interruptor func() bool) (uint32, error) {
	if err := ReadBytes(conn, buffer[:4], interruptor); err != nil {
		return 0, err
	}
	var l uint32
	if USE_BIG_ENDIAN {
		l = binary.BigEndian.Uint32(buffer[:4])
	} else {
		l = binary.LittleEndian.Uint32(buffer[:4])
	}
	if err := ReadBytes(conn, buffer[:l], interruptor); err != nil {
		return 0, err
	}
	return l, nil
}
