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

func TestClientConsumesPostLoginControlMessages(t *testing.T) {
	conn := &controlConn{commands: make(chan struct{}, 10), consumed: make(chan struct{}, 1), stopped: make(chan struct{})}
	client := newClient(conn, make([]byte, 32), "xiaomi.camera.082ac1")
	defer client.Close()
	// More notifications than the transport control queue can retain. The
	// media reader must not be required to call ReadCommand to make progress.
	for range 32 {
		conn.commands <- struct{}{}
		select {
		case <-conn.consumed:
		case <-time.After(time.Second):
			t.Fatal("post-login control message was never consumed")
		}
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.commandsDone:
	default:
		t.Fatal("client closed without joining its control reader")
	}
}

func TestClientClosePreservesTransportError(t *testing.T) {
	expected := errors.New("test: close error")
	conn := &controlConn{commands: make(chan struct{}, 10), consumed: make(chan struct{}, 1), stopped: make(chan struct{}), closeError: expected}
	client := newClient(conn, make([]byte, 32), "xiaomi.camera.082ac1")
	if err := client.Close(); !errors.Is(err, expected) {
		t.Fatalf("transport close error lost: %v", err)
	}
	select {
	case <-client.commandsDone:
	case <-time.After(time.Second):
		t.Fatal("control reader leaked after transport close")
	}
}
