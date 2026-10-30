package rtsp

import (
	"bufio"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

func TestStopWhileHandlingPackets(t *testing.T) {
	for range 30 {
		local, remote := net.Pipe()
		conn := &Conn{conn: local, reader: bufio.NewReader(local), state: StatePlay, mode: core.ModePassiveProducer}
		done := make(chan struct{})
		go func() { _ = conn.Handle(); close(done) }()
		// Exercise the next read-loop iteration while Stop changes the state.
		for range 3 {
			_ = remote.SetWriteDeadline(time.Now().Add(time.Second))
			if _, err := remote.Write([]byte{'$', 1, 0, 4, 0x80, 201, 0, 0}); err != nil {
				t.Fatal(err)
			}
		}
		if err := conn.Stop(); err != nil {
			t.Fatal(err)
		}
		_ = remote.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("packet reader did not exit after Stop")
		}
	}
}

// A closed session can still have buffered packets while Reconnect replaces
// the connection. Its reader must never consume the replacement session.
func TestPlaybackKeepsItsReaderAcrossReconnect(t *testing.T) {
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	next, peer := net.Pipe()
	defer next.Close()
	defer peer.Close()
	old := &phasedRTCP{ready: make(chan struct{}), resume: make(chan struct{})}
	trap := &unexpectedReader{}
	c := &Conn{conn: local, reader: bufio.NewReader(old), state: StatePlay, mode: core.ModeActiveProducer}
	done := make(chan error, 1)
	go func() { done <- c.Handle() }()
	select {
	case <-old.ready:
	case <-time.After(time.Second):
		t.Fatal("old playback did not start")
	}
	c.stateMu.Lock()
	c.conn, c.reader, c.state = next, bufio.NewReader(trap), StateSetup
	c.stateMu.Unlock()
	close(old.resume)
	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) || trap.reads.Load() != 0 {
			t.Fatal("old playback read the replacement session", err, trap.reads.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("old playback did not exit")
	}
}

type phasedRTCP struct {
	ready, resume chan struct{}
	stage         int
}

func (r *phasedRTCP) Read(p []byte) (int, error) {
	r.stage++
	switch r.stage {
	case 1:
		return copy(p, []byte{'$', 1, 0, 4}), nil
	case 2:
		close(r.ready)
		<-r.resume
		return copy(p, []byte{0x80, 201, 0, 0}), nil
	default:
		return 0, io.EOF
	}
}

type unexpectedReader struct{ reads atomic.Int32 }

func (r *unexpectedReader) Read([]byte) (int, error) {
	r.reads.Add(1)
	return 0, io.ErrUnexpectedEOF
}

func TestPacketWriteKeepsItsTransportAcrossReconnect(t *testing.T) {
	old := &countedWriteConn{ready: make(chan struct{}), resume: make(chan struct{})}
	next := &countedWriteConn{}
	c := &Conn{conn: old}
	c.playOK.Store(true)
	done := make(chan error, 1)
	go func() { done <- c.writeInterleavedData([]byte{1}) }()
	select {
	case <-old.ready:
	case <-time.After(time.Second):
		t.Fatal("packet write did not start")
	}
	c.stateMu.Lock()
	c.conn = next
	c.playOK.Store(false)
	c.stateMu.Unlock()
	close(old.resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if old.writes.Load() != 1 || next.writes.Load() != 0 {
		t.Fatal("old audio entered the replacement RTSP handshake")
	}
	if err := c.writeInterleavedData([]byte{2}); err == nil || next.writes.Load() != 0 {
		t.Fatal("packet sent before replacement PLAY response")
	}
}

type countedWriteConn struct {
	net.Conn
	ready, resume chan struct{}
	writes        atomic.Int32
}

func (c *countedWriteConn) SetWriteDeadline(time.Time) error {
	if c.ready != nil {
		close(c.ready)
		<-c.resume
	}
	return nil
}

func (c *countedWriteConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return len(p), nil
}
