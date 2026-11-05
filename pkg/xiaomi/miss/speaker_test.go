package miss

import (
	"bytes"
	"encoding/binary"
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
	stops        int
	fail         bool
	failCommand  uint32
	closed       chan struct{}
	writeStarted chan struct{}
	once         sync.Once
}

func (w *speakerWire) WriteCommand(_ uint32, data []byte) error {
	command, err := crypto.Decode(data, make([]byte, 32))
	if err != nil {
		return err
	}
	cmd := binary.BigEndian.Uint32(command)
	if cmd == w.failCommand {
		return errors.New("speaker command failed")
	}
	switch cmd {
	case cmdSpeakerStartReq:
		w.starts++
	case cmdSpeakerStop:
		w.stops++
	}
	return nil
}
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
	p.speakerMu.Lock()
	stops := wire.stops
	p.speakerMu.Unlock()
	if stops != 2 {
		t.Fatal("idle speaker stop command was not sent", stops)
	}
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

func TestSharedSpeakerReleasesDetachedInputs(t *testing.T) {
	p, _ := sharedSpeaker(t, 0)
	codec := &core.Codec{Name: core.CodecPCMA, ClockRate: 8000}
	media := &core.Media{Kind: core.KindAudio, Direction: core.DirectionSendonly, Codecs: []*core.Codec{codec}}
	for range 100 {
		input := core.NewReceiver(nil, codec)
		if err := p.AddTrack(media, codec, input); err != nil {
			t.Fatal(err)
		}
		if len(p.Senders) != 1 {
			t.Fatal("long-lived camera retained detached speaker inputs", len(p.Senders))
		}
		input.Close()
		if p.Senders[0].State() != "closed" {
			t.Fatal("detached input kept its sender worker alive")
		}
		p.Senders[0].Wait()
	}
}

func TestSharedSpeakerIdleReleaseAndResume(t *testing.T) {
	p, wire := sharedSpeaker(t, 2)
	packet := &core.Packet{Payload: bytes.Repeat([]byte{1}, 160)}
	p.Senders[0].Handler(packet)
	accepted := p.speakerTime
	p.Senders[1].Handler(packet)
	p.stopSpeaker() // An old timer must not stop fresh activity.
	if p.speakerTime != accepted || wire.stops != 0 {
		t.Fatal("competing input renewed ownership or stale timer stopped speech")
	}
	p.speakerMu.Lock()
	p.speakerTime = time.Now().Add(-2 * time.Second)
	p.speakerMu.Unlock()
	p.stopSpeaker()
	p.stopSpeaker()
	if wire.stops != 1 {
		t.Fatal("idle release was not one-shot", wire.stops)
	}
	select {
	case <-wire.closed:
		t.Fatal("idle speaker release closed forward media")
	default:
	}
	p.Senders[1].Handler(&core.Packet{Payload: bytes.Repeat([]byte{2}, 320)})
	if wire.starts != 2 || len(wire.packets) != 1 {
		t.Fatal("speaker did not restart with a fresh frame")
	}
	_ = p.Stop()
	p.stopSpeaker()
	if wire.stops != 1 {
		t.Fatal("timer wrote after producer shutdown")
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

func TestSpeakerIdleReleaseFailureClosesTransport(t *testing.T) {
	p, wire := sharedSpeaker(t, 1)
	p.Senders[0].Handler(&core.Packet{Payload: make([]byte, 320)})
	p.speakerMu.Lock()
	wire.failCommand = cmdSpeakerStop
	p.speakerTime = time.Now().Add(-2 * time.Second)
	p.speakerMu.Unlock()
	p.stopSpeaker()
	select {
	case <-wire.closed:
	default:
		t.Fatal("failed device release left an unknown speaker lease running")
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
