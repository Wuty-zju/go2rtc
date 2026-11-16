package cs2

import (
	"io"
	"net"
	"testing"
	"time"
)

type commandAckWire struct {
	net.Conn
	ticks  <-chan time.Time
	closed chan struct{}
}

func (c *commandAckWire) Read(b []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, io.EOF
	case <-c.ticks:
		return copy(b, []byte{magic, msgDrwAck, 0, 6, magicDrw, 0, 0, 1, 0, 0}), nil
	}
}

func (c *commandAckWire) Write(b []byte) (int, error) {
	return len(b), nil
}

func TestWriteCommandConcurrentAcknowledgements(t *testing.T) {
	ticker := time.NewTicker(time.Millisecond)
	wire := &commandAckWire{ticks: ticker.C, closed: make(chan struct{})}
	c := &Conn{Conn: wire, channels: [4]*dataChannel{
		newDataChannel(0, 1), nil, newDataChannel(0, 1), nil,
	}}
	done := make(chan struct{})
	go func() {
		c.worker()
		close(done)
	}()
	t.Cleanup(func() {
		close(wire.closed)
		<-done
		ticker.Stop()
	})

	// ACKs can arrive independently of a command write, including delayed ACKs
	// for a previous command. Exercise the actual worker and command writer.
	for i := 0; i < 32; i++ {
		if err := c.WriteCommand(uint32(i), nil); err != nil {
			t.Fatal(err)
		}
	}
}
