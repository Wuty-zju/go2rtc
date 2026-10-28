package h265

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264"
	"github.com/pion/rtp"
)

func TestRTPAggregationParameterSets(t *testing.T) {
	parameters := [][]byte{{64, 1, 1}, {66, 1, 2}, {68, 1, 3}}
	keyframe := []byte{38, 1, 4}
	var got []byte
	depay := RTPDepay(&core.Codec{Name: core.CodecH265}, func(packet *rtp.Packet) { got = append([]byte(nil), packet.Payload...) })
	aggregate := []byte{96, 1}
	for _, nalu := range parameters {
		aggregate = binary.BigEndian.AppendUint16(aggregate, uint16(len(nalu)))
		aggregate = append(aggregate, nalu...)
	}
	depay(&rtp.Packet{Header: rtp.Header{Version: 2, SequenceNumber: 1}, Payload: aggregate})
	depay(&rtp.Packet{Header: rtp.Header{Version: 2, SequenceNumber: 2, Marker: true}, Payload: keyframe})
	want := h264.JoinNALU(append(parameters, keyframe)...)
	if !bytes.Equal(got, want) {
		t.Fatalf("parameter aggregation was not unpacked: got %x, want %x", got, want)
	}
}

func TestRTPDepayDoesNotChangeSharedMarker(t *testing.T) {
	packet := &rtp.Packet{Header: rtp.Header{Version: 2, Marker: true}, Payload: []byte{64, 1, 1}}
	RTPDepay(&core.Codec{Name: core.CodecH265}, func(*rtp.Packet) {})(packet)
	if !packet.Marker {
		t.Fatal("depay changed the shared input marker")
	}
}

func TestRTPAggregationDONAndMalformed(t *testing.T) {
	codec := &core.Codec{Name: core.CodecH265, FmtpLine: "sprop-max-don-diff=1"}
	var got []byte
	depay := RTPDepay(codec, func(packet *rtp.Packet) { got = append([]byte(nil), packet.Payload...) })
	// Payload header, DONL, first NAL, DOND, second NAL.
	depay(&rtp.Packet{Header: rtp.Header{Version: 2, Marker: true}, Payload: []byte{96, 1, 0, 10, 0, 3, 64, 1, 1, 0, 0, 3, 38, 1, 2}})
	if !bytes.Equal(got, h264.JoinNALU([]byte{64, 1, 1}, []byte{38, 1, 2})) {
		t.Fatal("DON fields leaked into NAL data")
	}
	for _, payload := range [][]byte{{96, 1, 0}, {96, 1, 0, 0}, {96, 1, 0, 0, 0, 8, 38}, {96, 1, 0, 0, 0, 2, 96, 1}, {96, 1, 0, 0, 0, 3, 38, 1, 1, 0, 0}} {
		got = nil
		depay(&rtp.Packet{Header: rtp.Header{Version: 2, Marker: true}, Payload: payload})
		if got != nil {
			t.Fatalf("malformed AP emitted: %x", payload)
		}
	}
}
