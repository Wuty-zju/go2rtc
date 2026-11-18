package xiaomi

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/core"
)

type reuseAudio struct {
	input  *core.Receiver
	packet *core.Packet
}

type reuseCamera struct {
	core.Connection
	done        chan struct{}
	started     chan struct{}
	once        sync.Once
	audio       chan reuseAudio
	failSpeaker bool
}

func (c *reuseCamera) Start() error {
	close(c.started)
	<-c.done
	return io.EOF
}
func (c *reuseCamera) Stop() error {
	c.once.Do(func() { close(c.done) })
	err := c.Connection.Stop()
	for _, s := range c.Senders {
		s.Wait()
	}
	return err
}
func (c *reuseCamera) AddTrack(m *core.Media, _ *core.Codec, r *core.Receiver) error {
	if c.failSpeaker {
		return errors.New("test speaker unavailable")
	}
	s := core.NewSender(m, r.Codec)
	s.Handler = func(p *core.Packet) { c.audio <- reuseAudio{r, p} }
	s.HandleRTP(r)
	c.Senders = append(c.Senders, s)
	return nil
}
func newReuseCamera(videos int, failSpeaker bool) *reuseCamera {
	c := &reuseCamera{done: make(chan struct{}), started: make(chan struct{}), audio: make(chan reuseAudio, 16), failSpeaker: failSpeaker}
	for i := 0; i < videos; i++ {
		c.Medias = append(c.Medias, &core.Media{ID: string(rune('0' + i)), Kind: core.KindVideo, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{{Name: core.CodecH264, ClockRate: 90000}}})
	}
	// The selected microphone may be converted Opus while the speaker is native PCMA.
	c.Medias = append(c.Medias, &core.Media{Kind: core.KindAudio, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{{Name: core.CodecOpus, ClockRate: 48000, Channels: 2}}}, &core.Media{Kind: core.KindAudio, Direction: core.DirectionSendonly, Codecs: []*core.Codec{{Name: core.CodecPCMA, ClockRate: 8000}}})
	for _, m := range c.Medias {
		if m.Direction == core.DirectionRecvonly {
			c.GetTrack(m, m.Codecs[0])
		}
	}
	return c
}

func reuseFixture(t *testing.T, videos int, failSpeaker bool, replacements ...*reuseCamera) (*reuseProducer, *reuseCamera, error) {
	t.Helper()
	c := newReuseCamera(videos, failSpeaker)
	cameras := append([]*reuseCamera{c}, replacements...)
	streams.HandleFunc("reuse-test", func(string) (core.Producer, error) {
		if len(cameras) == 0 {
			return nil, errors.New("test: unexpected camera dial")
		}
		camera := cameras[0]
		cameras = cameras[1:]
		return camera, nil
	})
	if _, err := streams.New("reuse-owner", "reuse-test:camera"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { streams.Delete("reuse-owner"); c.Stop() })
	p, err := newReuseProducer("reuse-owner")
	if err != nil {
		return nil, c, err
	}
	t.Cleanup(func() { p.Stop() })
	return p.(*reuseProducer), c, nil
}
func TestReuseProducerLifetimeAndBackchannel(t *testing.T) {
	p, c, err := reuseFixture(t, 2, false)
	if err != nil {
		t.Fatal(err)
	}
	var video, back *core.Media
	for _, m := range p.Medias {
		if m.Kind == core.KindVideo {
			video = m
		}
		if m.Direction == core.DirectionSendonly {
			back = m
		}
	}
	if video == nil || video.ID != "1" || back == nil || back.Codecs[0].Name != core.CodecPCMA {
		t.Fatal("lost lens identity or conflated microphone and speaker codecs")
	}
	output, err := p.GetTrack(video, video.Codecs[0])
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		got := make(chan *core.Packet, 1)
		s := core.NewSender(video, output.Codec)
		s.Handler = func(packet *core.Packet) { got <- packet }
		s.HandleRTP(output)
		packet := &core.Packet{Payload: []byte{1, 2, 3}}
		c.Receivers[1].Input(packet)
		select {
		case actual := <-got:
			if actual != packet {
				t.Fatal("forward path copied or changed the packet")
			}
		case <-time.After(time.Second):
			t.Fatal("video lost after subscriber rejoin")
		}
		s.Close()
		s.Wait()
	}
	bad := core.NewReceiver(nil, &core.Codec{Name: core.CodecOpus, ClockRate: 48000, Channels: 2})
	if err = p.AddTrack(back, back.Codecs[0], bad); err == nil {
		t.Fatal("microphone codec accepted as speaker input")
	}
	bad.Close()
	// Closing an input retires its worker; a persistent logical stream retains no history.
	for range 50 {
		input := core.NewReceiver(back, back.Codecs[0])
		if err = p.AddTrack(back, back.Codecs[0], input); err != nil {
			t.Fatal(err)
		}
		if len(p.Senders) != 1 {
			t.Fatal("retained detached return senders")
		}
		input.Close()
		p.Senders[0].Wait()
	}
	input := core.NewReceiver(back, back.Codecs[0])
	defer input.Close()
	if err = p.AddTrack(back, back.Codecs[0], input); err != nil {
		t.Fatal(err)
	}
	input.Input(&core.Packet{Payload: []byte{7, 8}})
	select {
	case actual := <-c.audio:
		if !bytes.Equal(actual.packet.Payload, []byte{7, 8}) {
			t.Fatal("speaker payload changed")
		}
	case <-time.After(time.Second):
		t.Fatal("speaker input did not reach the owner")
	}
	p.Stop()
	p.Stop()
	select {
	case <-c.done:
	default:
		t.Fatal("last projection did not release its device owner")
	}
	if err = p.AddTrack(back, back.Codecs[0], input); err == nil {
		t.Fatal("stopped projection accepted another input")
	}
}
func TestReuseProducerRejectsMissingLensAndSpeaker(t *testing.T) {
	if _, _, err := reuseFixture(t, 1, false); err == nil {
		t.Fatal("missing second lens accepted")
	}
	p, _, err := reuseFixture(t, 2, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range p.Medias {
		if m.Direction == core.DirectionSendonly {
			t.Fatal("failed speaker attachment was advertised")
		}
	}
}

func TestReuseSpeakerInputsKeepTheirIdentity(t *testing.T) {
	next := newReuseCamera(2, false)
	p, camera, err := reuseFixture(t, 2, false, next)
	if err != nil {
		t.Fatal(err)
	}
	var back *core.Media
	for _, m := range p.Medias {
		if m.Direction == core.DirectionSendonly {
			back = m
		}
	}
	if back == nil {
		t.Fatal("missing speaker")
	}
	attach := func() *core.Receiver {
		input := core.NewReceiver(back, back.Codecs[0])
		if err := p.AddTrack(back, back.Codecs[0], input); err != nil {
			t.Fatal(err)
		}
		return input
	}
	ownerInput := func(input *core.Receiver) *core.Receiver {
		packet := &core.Packet{Payload: []byte{3}}
		input.Input(packet)
		select {
		case got := <-camera.audio:
			if got.packet != packet {
				t.Fatal("changed return packet")
			}
			return got.input
		case <-time.After(time.Second):
			t.Fatal("missing return packet")
			return nil
		}
	}
	first, second := attach(), attach()
	if len(camera.audio) != 0 {
		t.Fatal("idle return tracks wrote audio")
	}
	firstOwner, secondOwner := ownerInput(first), ownerInput(second)
	if firstOwner == secondOwner {
		t.Fatal("two clients appeared as one input to the physical speaker")
	}
	first.Close()
	deadline := time.Now().Add(time.Second)
	for len(firstOwner.Senders()) > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(firstOwner.Senders()) != 0 || ownerInput(second) != secondOwner {
		t.Fatal("disconnect did not release only its own speaker input")
	}
	// A subscriber on the primary remains attached through both reconnect and projection stop.
	direct := &reuseConsumer{output: p}
	direct.Medias = []*core.Media{{Kind: core.KindAudio, Direction: core.DirectionRecvonly, Codecs: back.Codecs}}
	if err = p.source.AddConsumer(direct); err != nil {
		t.Fatal(err)
	}
	defer p.source.RemoveConsumer(direct)
	primary := direct.Receivers[0]
	if got := ownerInput(primary); got != primary || got == secondOwner {
		t.Fatal("direct and projected inputs share an identity")
	}

	camera.once.Do(func() { close(camera.done) })
	select {
	case <-next.started:
	case <-time.After(time.Second):
		t.Fatal("camera did not reconnect")
	}
	camera = next
	if len(next.GetSenderSources()) != 2 || ownerInput(second) != secondOwner || ownerInput(primary) != primary {
		t.Fatal("reconnect did not preserve exactly the two live inputs")
	}
	p.Stop()
	if ownerInput(primary) != primary || len(next.GetSenderSources()) != 1 {
		t.Fatal("projection stop disturbed the primary speaker")
	}
	if len(secondOwner.Senders()) != 0 || len(second.Senders()) != 0 {
		t.Fatal("projection stop retained a speaker subscription")
	}
}
