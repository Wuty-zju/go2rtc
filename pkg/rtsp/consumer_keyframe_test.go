package rtsp

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtp"
)

type keyframeConn struct {
	net.Conn
	writes [][]byte
}

func TestConsumerWaitsForRTPKeyframe(t *testing.T) {
	for _, test := range []struct {
		name                                                        string
		predicted, partial, key, continuation, parameter, aggregate []byte
	}{
		{core.CodecH264, []byte{0x41, 1}, []byte{0x7c, 0x45, 1}, []byte{0x7c, 0x85, 1}, []byte{0x7c, 0x45, 1}, []byte{0x67, 1}, []byte{24, 0, 2, 0x65, 1}},
		{core.CodecH265, []byte{2, 1, 1}, []byte{98, 1, 0x53, 1}, []byte{98, 1, 0x93, 1}, []byte{98, 1, 0x53, 1}, []byte{64, 1, 1}, []byte{96, 1, 0, 3, 38, 1, 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			makeWriter := func() (*keyframeConn, core.HandlerFunc) {
				transport := &keyframeConn{}
				conn := &Conn{conn: transport, state: StatePlay}
				conn.playOK.Store(true)
				return transport, conn.packetWriter(&core.Codec{Name: test.name, PayloadType: 96}, 0, 96)
			}
			var sequence uint16
			send := func(write core.HandlerFunc, payload []byte, marker bool) {
				sequence++
				p := &rtp.Packet{Header: rtp.Header{Version: 2, Marker: marker, SequenceNumber: sequence, Timestamp: 1234, SSRC: 42}, Payload: append([]byte(nil), payload...)}
				before, _ := p.Marshal()
				write(p)
				after, _ := p.Marshal()
				if !bytes.Equal(before, after) {
					t.Fatal("shared input packet changed")
				}
			}
			first, write := makeWriter()
			send(write, nil, true)
			send(write, test.predicted, true)
			send(write, test.partial, true)
			if len(first.writes) != 0 {
				t.Fatal("consumer received dependent or partial keyframe RTP")
			}
			send(write, test.parameter, true)
			if len(first.writes) != 1 {
				t.Fatal("parameter packet was lost")
			}
			send(write, test.key, false)
			send(write, test.continuation, true)
			send(write, test.predicted, true)
			if len(first.writes) != 3 {
				t.Fatal("fragmented keyframe did not unlock passthrough")
			}
			second, late := makeWriter()
			send(late, test.predicted, true)
			if len(second.writes) != 0 {
				t.Fatal("readiness leaked between consumers")
			}
			send(late, test.aggregate, true)
			send(late, test.predicted, true)
			if len(second.writes) != 2 {
				t.Fatal("aggregation packet did not unlock consumer")
			}
			var decoded rtp.Packet
			if err := decoded.Unmarshal(second.writes[1][4:]); err != nil || decoded.SequenceNumber != sequence || decoded.SSRC != 42 || decoded.Timestamp != 1234 {
				t.Fatal("passthrough changed RTP identity", err)
			}
		})
	}
}

func TestRTPVideoStartRejectsMalformedPackets(t *testing.T) {
	for _, codec := range []string{core.CodecH264, core.CodecH265} {
		for _, payload := range [][]byte{nil, {0}, {24}, {24, 0, 4, 0x65, 1}, {24, 0, 2, 0x65, 1, 0}, {28, 0x85}, {96, 1, 0, 5, 38}, {98, 1, 0x93}} {
			if _, key, valid := rtpVideoStart(codec, payload); valid && key {
				t.Fatalf("%s accepted malformed keyframe: %x", codec, payload)
			}
		}
	}
}

func (c *keyframeConn) Write(data []byte) (int, error) {
	c.writes = append(c.writes, append([]byte(nil), data...))
	return len(data), nil
}
func (c *keyframeConn) SetWriteDeadline(time.Time) error { return nil }

func TestConsumerWaitsForKeyframe(t *testing.T) {
	for _, test := range []struct {
		name                      string
		predicted, key, parameter []byte
	}{
		{core.CodecH264, []byte{0x41, 1}, []byte{0x65, 1}, []byte{0x67, 0x11}},
		{core.CodecH265, []byte{0x02, 1, 1}, []byte{0x26, 1, 1}, []byte{0x40, 1, 0x33}},
	} {
		t.Run(test.name, func(t *testing.T) {
			codec := &core.Codec{Name: test.name, PayloadType: core.PayloadTypeRAW}
			makeWriter := func() (*keyframeConn, core.HandlerFunc) {
				transport := &keyframeConn{}
				conn := &Conn{conn: transport, state: StatePlay}
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
			send(writeSecond, test.parameter)
			send(writeSecond, test.key)
			send(writeSecond, test.predicted)
			if !bytes.Contains(bytes.Join(second.writes, nil), test.parameter) {
				t.Fatal("parameter sets lost")
			}
			if len(second.writes) < 2 {
				t.Fatal("late consumer never resumed after its keyframe")
			}
		})
	}
}
