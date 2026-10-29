package miss

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/xiaomi/crypto"
)

type testFrame struct{ header, payload []byte }

type testConn struct {
	frames []testFrame
	writes [][]byte
	closed bool
}

func (c *testConn) Protocol() string                     { return "test" }
func (c *testConn) Version() string                      { return "test" }
func (c *testConn) ReadCommand() (uint32, []byte, error) { return 0, nil, io.EOF }
func (c *testConn) WriteCommand(_ uint32, data []byte) error {
	c.writes = append(c.writes, append([]byte(nil), data...))
	return nil
}
func (c *testConn) ReadPacket() ([]byte, []byte, error) {
	if len(c.frames) == 0 {
		return nil, nil, io.EOF
	}
	f := c.frames[0]
	c.frames = c.frames[1:]
	return f.header, f.payload, nil
}
func (c *testConn) WritePacket(_, _ []byte) error { return nil }
func (c *testConn) RemoteAddr() net.Addr          { return &net.UDPAddr{} }
func (c *testConn) SetDeadline(time.Time) error   { return nil }
func (c *testConn) Close() error                  { c.closed = true; return nil }

func TestDualLensStartMedia(t *testing.T) {
	for _, audio := range []string{"0", "1"} {
		conn := &testConn{}
		client := &Client{Conn: conn, key: make([]byte, 32), model: "xiaomi.camera.082ac1"}
		if err := client.StartMedia("all", "3", audio); err != nil {
			t.Fatal(err)
		}
		if len(conn.writes) != 1 {
			t.Fatalf("wanted one media-start command, got %d", len(conn.writes))
		}
		data, err := crypto.Decode(conn.writes[0], client.key)
		if err != nil {
			t.Fatal(err)
		}
		if binary.BigEndian.Uint32(data) != cmdVideoStart {
			t.Fatal("wrong media command")
		}
		var body map[string]int
		if err = json.Unmarshal(data[4:], &body); err != nil {
			t.Fatal(err)
		}
		if body["videoquality"] != 3 || body["videoquality2"] != 3 || body["enableaudio"] != int(audio[0]-'0') {
			t.Fatalf("unexpected dual media request: %v", body)
		}
	}
	if _, err := Dial("xiaomi://127.0.0.1?channel=all&model=unverified.camera"); err == nil || !strings.Contains(err.Error(), "not verified") {
		t.Fatalf("unverified model must fail before dialing: %v", err)
	}
}

func makeTestFrame(t *testing.T, key []byte, codec, flags, sequence uint32, data []byte) testFrame {
	t.Helper()
	payload, err := crypto.Encode(data, key)
	if err != nil {
		t.Fatal(err)
	}
	header := make([]byte, 32)
	binary.LittleEndian.PutUint32(header, uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:], codec)
	binary.LittleEndian.PutUint32(header[8:], sequence)
	binary.LittleEndian.PutUint32(header[12:], flags)
	return testFrame{header, payload}
}

func TestDualLensProbeAndRouting(t *testing.T) {
	key := make([]byte, 32)
	// Deliberately identical SPS for both lenses: resolution and codec cannot identify a lens.
	sps, _ := hex.DecodeString("000000016764001fac2484014016ec0440000003004000000c23c60c92")
	conn := &testConn{frames: []testFrame{
		makeTestFrame(t, key, codecH264, 0x001e2001, 1, sps),
		makeTestFrame(t, key, codecH264, 0x015e2001, 2, sps),
		makeTestFrame(t, key, codecOPUS, 0, 3, []byte{1}),
	}}
	client := &Client{Conn: conn, key: key, model: "xiaomi.camera.082ac1"}
	medias, err := probe(client, true, 2, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(medias) != 4 || medias[0].ID != "0" || medias[1].ID != "1" {
		t.Fatalf("expected two video tracks and shared audio/backchannel: %v", medias)
	}
	if medias[0].Codecs[0] == medias[1].Codecs[0] || medias[0].Codecs[0].FmtpLine != medias[1].Codecs[0].FmtpLine {
		t.Fatal("same-format lenses need distinct track identities")
	}
	producer := &Producer{Connection: core.Connection{Medias: medias}, client: client, dual: true}
	var received [3][]uint16
	for i := range 3 {
		receiver, err := producer.GetTrack(medias[i], medias[i].Codecs[0])
		if err != nil {
			t.Fatal(err)
		}
		receiver.Input = func(packet *core.Packet) { received[i] = append(received[i], packet.SequenceNumber) }
	}
	conn.frames = []testFrame{
		makeTestFrame(t, key, codecH264, 0x001e2000, 9, []byte{0, 0, 0, 1, 0x41, 1}),
		makeTestFrame(t, key, codecH264, 0x001e2000, 10, []byte{0, 0, 0, 1, 0x65, 1}),
		makeTestFrame(t, key, codecH264, 0x015e2000, 19, []byte{0, 0, 0, 1, 0x41, 2}),
		makeTestFrame(t, key, codecH264, 0x015e2000, 20, []byte{0, 0, 0, 1, 0x65, 2}),
		makeTestFrame(t, key, codecOPUS, 0, 30, []byte{1}),
	}
	_ = producer.Start() // finite fake transport finishes with EOF
	for i, sequence := range []uint16{10, 20, 30} {
		if len(received[i]) != 1 || received[i][0] != sequence {
			t.Fatalf("track %d received other lens or audio: %v", i, received[i])
		}
	}
}

func TestDualLensProbeRequiresBothLenses(t *testing.T) {
	key := make([]byte, 32)
	sps, _ := hex.DecodeString("000000016764001fac2484014016ec0440000003004000000c23c60c92")
	conn := &testConn{frames: []testFrame{makeTestFrame(t, key, codecH264, 0, 1, sps)}}
	_, err := probe(&Client{Conn: conn, key: key}, false, 2, false)
	if err == nil || !strings.Contains(err.Error(), "missing video channel") {
		t.Fatalf("one lens must not pass dual-lens discovery: %v", err)
	}
}
