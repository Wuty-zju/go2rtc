package miss

import (
	"bytes"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/xiaomi/crypto"
)

type speakerWire struct {
	Conn
	packets      [][]byte
	starts       int
	fail         bool
	closed       chan struct{}
	writeStarted chan struct{}
	once         sync.Once
}

func (w *speakerWire) WriteCommand(_ uint32, _ []byte) error { w.starts++; return nil }
func (w *speakerWire) WritePacket(_, data []byte) error {
	if w.writeStarted != nil {
		close(w.writeStarted)
		<-w.closed
		return net.ErrClosed
	}

	if w.fail {
		return errors.New("speaker write failed")
	}
	w.packets = append(w.packets, bytes.Clone(data))
	return nil
}
func (w *speakerWire) Close() error { w.once.Do(func() { close(w.closed) }); return nil }
func sharedSpeaker(t *testing.T, inputs int) (*Producer, *speakerWire) {
	t.Helper()
	wire := &speakerWire{closed: make(chan struct{})}
	client := &Client{Conn: wire, key: make([]byte, 32), model: "xiaomi.camera.082ac1"}
	codec := &core.Codec{Name: core.CodecPCMA, ClockRate: 8000}
	media := &core.Media{Kind: core.KindAudio, Direction: core.DirectionSendonly, Codecs: []*core.Codec{codec}}
	p := &Producer{Connection: core.Connection{Transport: client}, client: client, dual: true}
	t.Cleanup(func() { _ = p.Stop() })
	for range inputs {
		if err := p.AddTrack(media, codec, core.NewReceiver(nil, codec)); err != nil {
			t.Fatal(err)
		}
	}
	return p, wire
}
func TestSharedSpeaker(t *testing.T) {
	p, wire := sharedSpeaker(t, 2)
	p.Senders[0].Handler(&core.Packet{}) // Empty keepalive is not speaker input.
	if wire.starts != 0 {
		t.Fatal("preloading claimed the speaker")
	}
	send := func(input int, value byte, size int) {
		p.Senders[input].Handler(&core.Packet{Payload: bytes.Repeat([]byte{value}, size)})
	}
	// Either lens can speak first; a competing input must not corrupt its frame.
	send(1, 2, 160)
	send(0, 1, 320)
	send(1, 2, 160)
	if len(wire.packets) != 1 || wire.starts != 1 {
		t.Fatal("competing source was not suppressed")
	}
	raw, err := crypto.Decode(wire.packets[0], make([]byte, 32))
	if err != nil || !bytes.Equal(raw, bytes.Repeat([]byte{2}, 320)) {
		t.Fatal("mixed speaker payload", err)
	}
	send(1, 2, 160) // This partial frame must be discarded after inactivity.
	time.Sleep(1100 * time.Millisecond)
	send(0, 1, 320)
	time.Sleep(1100 * time.Millisecond)
	send(1, 3, 320)
	if len(wire.packets) != 3 || wire.starts != 3 {
		t.Fatal("idle speaker was not restarted for either input")
	}
	raw, err = crypto.Decode(wire.packets[2], make([]byte, 32))
	if err != nil || !bytes.Equal(raw, bytes.Repeat([]byte{3}, 320)) {
		t.Fatal("stale partial audio was reused", err)
	}
	_ = p.Stop()
	send(0, 4, 320)
	if len(wire.packets) != 3 {
		t.Fatal("stopped producer wrote audio")
	}
}
func TestSpeakerWriteFailureClosesTransport(t *testing.T) {
	p, wire := sharedSpeaker(t, 1)
	wire.fail = true
	p.Senders[0].Handler(&core.Packet{Payload: make([]byte, 320)})
	select {
	case <-wire.closed:
	default:
		t.Fatal("asynchronous write error left media running")
	}
}

func TestSpeakerStopJoinsBlockedWriter(t *testing.T) {
	p, wire := sharedSpeaker(t, 1)
	wire.writeStarted = make(chan struct{})
	p.GetSenderSources()[0].Input(&core.Packet{Payload: make([]byte, 320)})
	select {
	case <-wire.writeStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not start")
	}
	done := make(chan struct{})
	go func() { _ = p.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop deadlocked behind audio write")
	}
	if p.Senders[0].State() != "closed" {
		t.Fatal("writer was not closed")
	}
}
