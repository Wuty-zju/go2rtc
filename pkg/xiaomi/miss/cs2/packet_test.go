package cs2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func TestReadPacketHeaderBoundary(t *testing.T) {
	for _, size := range []int{1, hdrSize - 1, hdrSize, hdrSize + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := bytes.Repeat([]byte{0xA5}, size)
			frame := make([]byte, 4+size)
			binary.BigEndian.PutUint32(frame, uint32(size))
			copy(frame[4:], data)

			c := &Conn{channels: [4]*dataChannel{nil, nil, newDataChannel(0, 1)}}
			if err := c.channels[2].Push(frame); err != nil {
				t.Fatal(err)
			}
			hdr, payload, err := c.ReadPacket()
			if size < hdrSize {
				if err == nil || hdr != nil || payload != nil {
					t.Fatal("truncated media header accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(hdr, data[:hdrSize]) || !bytes.Equal(payload, data[hdrSize:]) {
				t.Fatal("media header or payload changed")
			}
		})
	}
}
