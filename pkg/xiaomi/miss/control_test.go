package miss

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

type controlConn struct {
	Conn
	commands   chan struct{}
	consumed   chan struct{}
	stopped    chan struct{}
	once       sync.Once
	closeError error
}

func (c *controlConn) ReadCommand() (uint32, []byte, error) {
	select {
	case <-c.commands:
		c.consumed <- struct{}{}
		return cmdEncoded, []byte{1}, nil
	case <-c.stopped:
		return 0, nil, io.EOF
	}
}
func (c *controlConn) Close() error { c.once.Do(func() { close(c.stopped) }); return c.closeError }

func TestClientControlLifecycle(t *testing.T) {
	for _, fail := range []bool{false, true} {
		conn := &controlConn{commands: make(chan struct{}, 10), consumed: make(chan struct{}, 1), stopped: make(chan struct{}), closeError: errors.New("close error")}
		var transport Conn = conn
		if fail {
			transport = &failedControlConn{conn}
		}
		client := newClient(transport, make([]byte, 32), "xiaomi.camera.082ac1")
		if fail {
			select {
			case <-conn.stopped:
			case <-time.After(time.Second):
				t.Fatal("control failure left media running")
			}
		} else {
			for range 32 {
				conn.commands <- struct{}{}
				select {
				case <-conn.consumed:
				case <-time.After(time.Second):
					t.Fatal("control queue stopped draining")
				}
			}
		}
		if err := client.Close(); !errors.Is(err, conn.closeError) {
			t.Fatal("lost transport close error", err)
		}
		select {
		case <-client.commandsDone:
		default:
			t.Fatal("control reader did not join")
		}
	}
}

type failedControlConn struct{ *controlConn }

func (c *failedControlConn) ReadCommand() (uint32, []byte, error) {
	return 0, nil, errors.New("control read failed")
}
