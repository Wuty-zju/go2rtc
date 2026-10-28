package cs2

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
)

type audioWire struct {
	net.Conn
	writes [][]byte
}

func (c *audioWire) Write(data []byte) (int, error) {
	c.writes = append(c.writes, bytes.Clone(data))
	return len(data), nil
}

func TestWritePacketPreservesHeaderAndPayload(t *testing.T) {
	wire := &audioWire{}
	conn := &Conn{Conn: wire}
	header := bytes.Repeat([]byte{0xA5}, hdrSize)
	payload := bytes.Repeat([]byte{0x3C}, 97)
	if err := conn.WritePacket(header, payload); err != nil {
		t.Fatal(err)
	}
	frame := wire.writes[0]
	if frame[5] != 3 || binary.BigEndian.Uint16(frame[6:]) != 0 {
		t.Fatal("incorrect speaker channel/sequence")
	}
	if binary.BigEndian.Uint32(frame[8:]) != uint32(len(header)+len(payload)) {
		t.Fatal("incorrect media frame size")
	}
	if !bytes.Equal(frame[12:12+hdrSize], header) || !bytes.Equal(frame[12+hdrSize:], payload) {
		t.Fatal("speaker transport corrupted the encrypted audio payload")
	}
	if err := conn.WritePacket(header, payload); err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint16(wire.writes[1][6:]) != 1 {
		t.Fatal("speaker sequence did not advance")
	}
}

func TestReadCommandRejectsTruncatedHeader(t *testing.T) {
	c := &Conn{channels: [4]*dataChannel{newDataChannel(0, 1)}}
	c.channels[0].popBuf <- []byte{1}
	if _, _, err := c.ReadCommand(); err == nil {
		t.Fatal("truncated command accepted")
	}
}
