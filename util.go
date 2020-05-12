package tcpwrapper

import (
	"fmt"
	"net"
)

type interruptorFunc func(session *ConnSession) error
type sessionHandler func(session *ConnSession)

func trySendAllQueued(session *ConnSession, pipe <-chan []byte) {
loop:
	for {
		select {
		case msg := <-pipe:
			if err := session.WriteBytes(msg, nil); err != nil {
				break loop
			}
		default:
			break loop
		}
	}
}

func DefaultSenderInterruptor(session *ConnSession) error {
	select {
	case <-session.connUserClose:
		return &sharedInterruptedByUser
	case <-session.internalConnErrorClose:
		return &sharedInterruptedByError
	default:
		return nil
	}
}

func DefaultReceiverInterruptor(session *ConnSession) error {
	select {
	case <-session.connUserClose:
		return &sharedInterruptedByUser
	case <-session.internalConnErrorClose:
		return &sharedInterruptedByError
	default:
		return nil
	}
}

func defaultSafetySelect(session *ConnSession) bool {
	select {
	case <-session.connUserClose:
		return false
	case <-session.internalConnErrorClose:
		return false
	default:
		return true
	}
}

func defaultSafetySelectHandle(session *ConnSession, onUserClose sessionHandler, onErrorClose sessionHandler) bool {
	select {
	case <-session.connUserClose:
		onUserClose(session)
		return false
	case <-session.internalConnErrorClose:
		onErrorClose(session)
		return false
	default:
		return true
	}
}

func defaultOnUserClosingSender(session *ConnSession) {
	if InfoLogger != nil {
		InfoLogger(fmt.Sprintf("[CONN] Signaled. Closing sender for %s.\n", session.Remote()))
	}
	trySendAllQueued(session, session.sendingQueue)
	if OnSenderUserClosed != nil {
		OnSenderUserClosed(session)
	}
}

func defaultOnErrorClosingSender(session *ConnSession) {
	if InfoLogger != nil {
		InfoLogger(fmt.Sprintf("[CONN] Internal Error Close. Closing sender for %s.\n", session.Remote()))
	}
	trySendAllQueued(session, session.sendingQueue)
	if OnSenderErrorClosed != nil {
		OnSenderErrorClosed(session)
	}
}

func defaultOnUserClosingReceiver(session *ConnSession) {
	if InfoLogger != nil {
		InfoLogger(fmt.Sprintf("[CONN] Signaled. Closing receiver for %s.\n", session.Remote()))
	}
	if OnReceiverUserClosed != nil {
		OnReceiverUserClosed(session)
	}
}

func defaultOnErrorClosingReceiver(session *ConnSession) {
	if InfoLogger != nil {
		InfoLogger(fmt.Sprintf("[CONN] Internal Error Close. Closing receiver for %s.\n", session.Remote()))
	}
	if OnReceiverErrorClosed != nil {
		OnSenderErrorClosed(session)
	}
}

func handleClosingTimedout(session *ConnSession, err error, userClosingHandler sessionHandler, errorClosingHandler sessionHandler) (handled bool) {
	// We may set the deadline of the net.Conn so it unblocks from the net.Conn.Write() in order to gracefully close the session.
	// We assure here that the timeout error is caused by our logic.
	if netError, isNetError := err.(net.Error); !isNetError || !netError.Timeout() {
		return false
	}
	if LowSpamLogger != nil {
		LowSpamLogger("It's a timeout: %v", err)
	}
	if _, ok := <-session.closingRS; !ok {
		if LowSpamLogger != nil {
			LowSpamLogger("The session is closing: %s", session.Remote())
		}
		if _, ok := <-session.connUserClose; !ok {
			userClosingHandler(session)
		} else if _, ok = <-session.internalConnErrorClose; !ok {
			errorClosingHandler(session)
		} else {
			return false
		}
		return true
	}
	return false
}
