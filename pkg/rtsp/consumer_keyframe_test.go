package rtsp

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

type keyframeConn struct {
	net.Conn
	writes [][]byte
}

func (c *keyframeConn) Write(data []byte) (int, error) {
	c.writes = append(c.writes, append([]byte(nil), data...))
	return len(data), nil
}
func (c *keyframeConn) SetWriteDeadline(time.Time) error { return nil }

func TestConsumerWaitsForKeyframe(t *testing.T) {
	for _, test := range []struct {
		name           string
		predicted, key []byte
	}{
		{core.CodecH264, []byte{0x41, 1}, []byte{0x65, 1}},
		{core.CodecH265, []byte{0x02, 1, 1}, []byte{0x26, 1, 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			codec := &core.Codec{Name: test.name, PayloadType: core.PayloadTypeRAW}
			makeWriter := func() (*keyframeConn, core.HandlerFunc) {
				transport := &keyframeConn{}
				conn := &Conn{conn: transport}
				conn.state.Store(StatePlay)
				conn.playOK.Store(true)
				return transport, conn.packetWriter(codec, 0, 96)
			}
			send := func(writer core.HandlerFunc, nalu []byte) {
				packet := &core.Packet{}
				packet.Payload = binary.BigEndian.AppendUint32(nil, uint32(len(nalu)))
				packet.Payload = append(packet.Payload, nalu...)
				writer(packet)
			}
			first, writeFirst := makeWriter()
			send(writeFirst, test.predicted)
			if len(first.writes) != 0 {
				t.Fatal("new consumer received a dependent frame before its first keyframe")
			}
			send(writeFirst, test.key)
			send(writeFirst, test.predicted)
			if len(first.writes) != 2 {
				t.Fatalf("first consumer did not receive keyframe and following frame: %d", len(first.writes))
			}
			second, writeSecond := makeWriter()
			send(writeSecond, test.predicted)
			send(writeFirst, test.predicted)
			if len(second.writes) != 0 || len(first.writes) != 3 {
				t.Fatal("keyframe readiness leaked between consumers")
			}
			send(writeSecond, test.key)
			send(writeSecond, test.predicted)
			if len(second.writes) != 2 {
				t.Fatal("late consumer never resumed after its keyframe")
			}
		})
	}
}

func TestConsumerPreservesParameterSetsBeforeKeyframe(t *testing.T) {
	for _, test := range []struct {
		name           string
		parameter, key []byte
	}{
		{core.CodecH264, []byte{0x67, 0x11, 0x22}, []byte{0x65, 1}},
		{core.CodecH265, []byte{0x40, 1, 0x33}, []byte{0x26, 1, 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &keyframeConn{}
			conn := &Conn{conn: transport}
			conn.state.Store(StatePlay)
			conn.playOK.Store(true)
			write := conn.packetWriter(&core.Codec{Name: test.name, PayloadType: core.PayloadTypeRAW}, 0, 96)
			for _, nalu := range [][]byte{test.parameter, test.key} {
				packet := &core.Packet{}
				packet.Payload = binary.BigEndian.AppendUint32(nil, uint32(len(nalu)))
				packet.Payload = append(packet.Payload, nalu...)
				write(packet)
			}
			if !bytes.Contains(bytes.Join(transport.writes, nil), test.parameter) {
				t.Fatal("parameter sets were discarded while waiting for a keyframe")
			}
		})
	}
}
