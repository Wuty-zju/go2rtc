package miss

import (
	"encoding/binary"
	"errors"
	"io"
	"net/url"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/xiaomi/crypto"
)

type opusWire struct {
	Conn
	packets []*Packet
}

func (w *opusWire) Close() error                { return nil }
func (w *opusWire) SetDeadline(time.Time) error { return nil }
func (w *opusWire) ReadPacket() ([]byte, []byte, error) {
	if len(w.packets) == 0 {
		return nil, nil, io.EOF
	}
	p := w.packets[0]
	w.packets = w.packets[1:]
	h := make([]byte, 32)
	binary.LittleEndian.PutUint32(h[4:], p.CodecID)
	binary.LittleEndian.PutUint32(h[8:], p.Sequence)
	body, _ := crypto.Encode(p.Payload, make([]byte, 32))
	return h, body, nil
}

func TestPtimeOptions(t *testing.T) {
	for _, raw := range []string{"ptime=", "ptime=40", "ptime=20&ptime=20", "ptime=20&audio=0"} {
		q, _ := url.ParseQuery(raw)
		if err := ValidatePtime(q); err == nil {
			t.Fatal("accepted unsupported options", raw)
		}
	}
	for _, raw := range []string{"", "ptime=20", "ptime=20&audio=1"} {
		q, _ := url.ParseQuery(raw)
		if err := ValidatePtime(q); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPtimeProbeRejectsIncompatibleAudio(t *testing.T) {
	for _, packet := range []*Packet{{CodecID: codecPCMA, Payload: []byte{1}}, {CodecID: codecOPUS, Payload: []byte{0x10, 1}}} {
		client := &Client{Conn: &opusWire{packets: []*Packet{packet}}, key: make([]byte, 32)}
		if _, err := probe(client, true, 1, true); err == nil || !strings.Contains(err.Error(), "ptime") || errors.Is(err, io.EOF) {
			t.Fatal("audio was not rejected during its probe", err)
		}
	}
}

func TestPtimeDamagedPacketLeavesGapWithoutReconnect(t *testing.T) {
	client := &Client{Conn: &opusWire{packets: []*Packet{
		{CodecID: codecOPUS, Sequence: 1, Payload: []byte{0xb9, 1, 2}},
		{CodecID: codecOPUS, Sequence: 2, Payload: []byte{0xb9, 1}}, // Invalid CBR length.
		{CodecID: codecOPUS, Sequence: 3, Payload: []byte{0xb8, 3}},
	}}, key: make([]byte, 32)}
	p := &Producer{client: client, opus20: true}
	c := &core.Codec{Name: core.CodecOpus, ClockRate: 48000, Channels: 2}
	m := &core.Media{Kind: core.KindAudio, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{c}}
	r, _ := p.GetTrack(m, c)
	var packets []*core.Packet
	r.Input = func(packet *core.Packet) { packets = append(packets, packet) }
	if err := p.Start(); !errors.Is(err, io.EOF) {
		t.Fatal("damaged packet triggered transport failure", err)
	}
	if len(packets) != 3 || packets[0].Timestamp != 0 || packets[1].Timestamp != 960 || packets[2].Timestamp != 3840 || packets[2].SequenceNumber != 4 {
		t.Fatal("damaged packet compressed the known audio timeline", packets)
	}
}

func TestPtimePreservesSourceLossAndSequenceWrap(t *testing.T) {
	client := &Client{Conn: &opusWire{packets: []*Packet{
		{CodecID: codecOPUS, Sequence: 0xfffffffe, Payload: []byte{0xb9, 1, 2}},
		{CodecID: codecOPUS, Sequence: 0, Payload: []byte{0xb9, 3, 4}}, // Missing one 40ms packet across wrap.
		{CodecID: codecOPUS, Sequence: 1, Payload: []byte{0xb8, 5}},
		{CodecID: codecOPUS, Sequence: 3, Payload: []byte{0xb8, 6}}, // Missing one 20ms packet.
	}}, key: make([]byte, 32)}
	p := &Producer{client: client, opus20: true}
	codec := &core.Codec{Name: core.CodecOpus, ClockRate: 48000, Channels: 2}
	m := &core.Media{Kind: core.KindAudio, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{codec}}
	r, _ := p.GetTrack(m, codec)
	var ts []uint32
	var seq []uint16
	r.Input = func(packet *core.Packet) { ts = append(ts, packet.Timestamp); seq = append(seq, packet.SequenceNumber) }
	if err := p.Start(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if !slices.Equal(ts, []uint32{0, 960, 3840, 4800, 5760, 7680}) || !slices.Equal(seq, []uint16{0, 1, 4, 5, 6, 8}) {
		t.Fatal("lost interval compressed", ts, seq)
	}
}
func TestPtimeAllowsFirmwareWithoutSequenceNumbers(t *testing.T) {
	client := &Client{Conn: &opusWire{packets: []*Packet{
		{CodecID: codecOPUS, Payload: []byte{0xb8, 1}}, {CodecID: codecOPUS, Payload: []byte{0xb8, 2}},
	}}, key: make([]byte, 32)}
	p := &Producer{client: client, opus20: true}
	codec := &core.Codec{Name: core.CodecOpus, ClockRate: 48000, Channels: 2}
	m := &core.Media{Kind: core.KindAudio, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{codec}}
	r, _ := p.GetTrack(m, codec)
	count := 0
	r.Input = func(*core.Packet) { count++ }
	if err := p.Start(); !errors.Is(err, io.EOF) || count != 2 {
		t.Fatal("sequence metadata suppressed valid coded frames", err, count)
	}
}

func TestPtimeOnBothLensOutputs(t *testing.T) {
	wire := &opusWire{packets: []*Packet{{CodecID: codecOPUS, Payload: []byte{0xb9, 1, 2}}}}
	client := &Client{Conn: wire, key: make([]byte, 32)}
	video := &core.Media{Kind: core.KindVideo, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{{Name: core.CodecH265}}}
	mic := &core.Media{Kind: core.KindAudio, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{{Name: core.CodecOpus, ClockRate: 48000}}}
	p := &Producer{Connection: core.Connection{Medias: []*core.Media{video, video.Clone(), mic}}, client: client, opus20: true}
	var pair LensPair
	var output [2][]*core.Packet
	for i := range 2 {
		v, err := pair.open(i, func() (*Producer, error) { return p, nil })
		if err != nil {
			t.Fatal(err)
		}
		r, err := v.GetTrack(mic, mic.Codecs[0])
		if err != nil {
			t.Fatal(err)
		}
		r.Input = func(packet *core.Packet) { output[i] = append(output[i], packet) }
		v.state.Store(lensWaitKey)
	}
	if err := p.Start(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	for _, packets := range output {
		if len(packets) != 2 || packets[1].Timestamp-packets[0].Timestamp != 960 || packets[0].Payload[1] != 1 || packets[1].Payload[1] != 2 {
			t.Fatal("both original frames must reach each lens on the 20 ms clock", packets)
		}
	}
}

// A continuing subscriber sees the replacement producer on the same RTP timeline.
func TestOpusClockAfterProducerReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, repack := range []bool{false, true} {
			codec := &core.Codec{Name: core.CodecOpus, ClockRate: 48000, Channels: 2}
			media := &core.Media{Kind: core.KindAudio, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{codec}}
			run := func() []uint32 {
				wire := &opusWire{packets: []*Packet{
					{CodecID: codecOPUS, Sequence: 4, Payload: []byte{0xb9, 1, 2}},
					{CodecID: codecOPUS, Sequence: 5, Payload: []byte{0xb9, 3, 4}},
				}}
				p := &Producer{client: &Client{Conn: wire, key: make([]byte, 32)}, opus20: repack}
				p.Medias = []*core.Media{media}
				r, err := p.GetTrack(media, codec)
				if err != nil {
					t.Fatal(err)
				}
				var stamps []uint32
				r.Input = func(packet *core.Packet) { stamps = append(stamps, packet.Timestamp) }
				if err = p.Start(); !errors.Is(err, io.EOF) {
					t.Fatal(err)
				}
				return stamps
			}
			first := run()
			time.Sleep(4 * time.Second)
			next := run()
			size, step := 2, uint32(1920)
			if repack {
				size, step = 4, 960
			}
			if len(first) != size || len(next) != size || first[1]-first[0] != step || next[0]-first[0] != 4*48000 {
				t.Fatalf("repack=%t: timestamps before and after replacement: %v / %v", repack, first, next)
			}
		}
	})
}
