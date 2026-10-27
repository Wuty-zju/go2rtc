package cs2

import (
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
)

type controlPackets struct {
	net.Conn
	next uint16
}

func (c *controlPackets) Read(buf []byte) (int, error) {
	if c.next > 10 {
		return 0, io.EOF
	}
	frame := make([]byte, 16)
	frame[0], frame[1], frame[4], frame[5] = magic, msgDrw, magicDrw, 0
	binary.BigEndian.PutUint16(frame[2:], 12)
	binary.BigEndian.PutUint16(frame[6:], c.next)
	binary.BigEndian.PutUint32(frame[8:], 4)
	binary.LittleEndian.PutUint32(frame[12:], 0x1001)
	c.next++
	return copy(buf, frame), nil
}
func (c *controlPackets) Write(buf []byte) (int, error) { return len(buf), nil }

func TestUndrainedControlQueueTerminatesMedia(t *testing.T) {
	c := &Conn{Conn: &controlPackets{}, channels: [4]*dataChannel{newDataChannel(0, 10), nil, newDataChannel(250, 100), nil}}
	go c.worker()
	_, _, err := c.ReadPacket()
	if err == nil || !strings.Contains(err.Error(), "channel 0: pop buffer is full") {
		t.Fatalf("undrained notifications must identify the channel that terminated the shared transport: %v", err)
	}
}

func TestReadCommandRejectsTruncatedHeader(t *testing.T) {
	c := &Conn{channels: [4]*dataChannel{newDataChannel(0, 1)}}
	c.channels[0].popBuf <- []byte{1}
	if _, _, err := c.ReadCommand(); err == nil {
		t.Fatal("truncated command accepted")
	}
}
