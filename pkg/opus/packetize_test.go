package opus

import (
	"bytes"
	"testing"
)

func TestSplit20(t *testing.T) {
	for _, tc := range []struct {
		packet []byte
		frames [][]byte
	}{
		{[]byte{0xb8}, [][]byte{{0xb8}}}, // Zero-length coded frame (PLC).
		{[]byte{0x98}, [][]byte{{0x98}}},
		{[]byte{0xb8, 1, 2}, [][]byte{{0xb8, 1, 2}}},
		{[]byte{0xb9, 1, 2, 3, 4}, [][]byte{{0xb8, 1, 2}, {0xb8, 3, 4}}},
		{[]byte{0xba, 1, 1, 2, 3}, [][]byte{{0xb8, 1}, {0xb8, 2, 3}}},
		{[]byte{0xbb, 3, 1, 2, 3}, [][]byte{{0xb8, 1}, {0xb8, 2}, {0xb8, 3}}},
		{[]byte{0xbb, 0x83, 1, 2, 1, 2, 3, 4}, [][]byte{{0xb8, 1}, {0xb8, 2, 3}, {0xb8, 4}}},
		{[]byte{0xbb, 0x42, 2, 1, 2, 0, 0}, [][]byte{{0xb8, 1}, {0xb8, 2}}},
		{append([]byte{0xbb, 0x42, 255, 0, 1, 2}, make([]byte, 254)...), [][]byte{{0xb8, 1}, {0xb8, 2}}},
		{[]byte{0xbf, 2, 1, 2}, [][]byte{{0xbc, 1}, {0xbc, 2}}}, // Stereo flag survives.
	} {
		original := bytes.Clone(tc.packet)
		frames, err := Split20(tc.packet)
		if err != nil || len(frames) != len(tc.frames) {
			t.Fatal(err, len(frames))
		}
		for i, frame := range frames {
			if !bytes.Equal(frame, tc.frames[i]) {
				t.Fatalf("frame %d: got %x, want %x", i, frame, tc.frames[i])
			}
		}
		if !bytes.Equal(tc.packet, original) {
			t.Fatal("modified input packet")
		}
	}
}

func TestSplit20RejectsUnsupportedOrMalformed(t *testing.T) {
	for _, packet := range [][]byte{
		nil, {0x90}, {0x10}, {0x18}, // Missing, 10/40/60ms coded frames.
		{0xb9, 1}, {0xba}, {0xba, 3, 1}, {0xbb}, {0xbb, 0}, {0xbb, 7},
		{0xbb, 0x42, 255}, {0xbb, 0x42, 3, 1}, {0xbb, 0x83, 255},
		append([]byte{0xb8}, make([]byte, 1276)...),
	} {
		if _, err := Split20(packet); err == nil {
			t.Fatalf("accepted invalid packet %x", packet)
		}
	}
}

func FuzzSplit20(f *testing.F) {
	for _, p := range [][]byte{{0xb8}, {0xb9, 1, 2}, {0xba, 1, 1, 2, 3}, {0xbb, 0x83, 1, 2, 1, 2, 3, 4}} {
		f.Add(p)
	}
	f.Fuzz(func(t *testing.T, packet []byte) {
		original := bytes.Clone(packet)
		frames, err := Split20(packet)
		if !bytes.Equal(original, packet) {
			t.Fatal("modified input")
		}
		if err != nil {
			return
		}
		if len(frames) == 0 || len(frames) > 6 {
			t.Fatal("invalid output duration")
		}
		for _, frame := range frames {
			if len(frame) < 1 || len(frame) > 1276 || frame[0]&3 != 0 {
				t.Fatal("invalid output framing")
			}
		}
	})
}
