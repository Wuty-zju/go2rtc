package streams

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

type ownedBackchannel struct {
	*reconnectProducer
	inputs   []*core.Receiver
	received chan uint16
}

func newOwnedBackchannel() *ownedBackchannel {
	media := &core.Media{Kind: core.KindAudio, Direction: core.DirectionSendonly, Codecs: []*core.Codec{{Name: core.CodecOpus, ClockRate: 48000, Channels: 2}}}
	return &ownedBackchannel{reconnectProducer: newReconnectProducer(media), received: make(chan uint16, 8)}
}
func (p *ownedBackchannel) AddTrack(media *core.Media, codec *core.Codec, input *core.Receiver) error {
	if len(p.inputs) > 0 {
		return fmt.Errorf("test: speaker already owned")
	}
	sender := core.NewSender(media, codec)
	sender.Handler = func(packet *core.Packet) { p.received <- packet.SequenceNumber }
	sender.WithParent(input)
	sender.Start()
	p.Senders = append(p.Senders, sender)
	p.inputs = append(p.inputs, input)
	return nil
}
func (p *ownedBackchannel) GetSenderSources() []*core.Node {
	var nodes []*core.Node
	for _, input := range p.inputs {
		nodes = append(nodes, &input.Node)
	}
	return nodes
}
func (p *ownedBackchannel) release() {
	for _, sender := range p.Senders {
		sender.Close()
		sender.Wait()
	}
	p.Senders = nil
	p.inputs = nil
}

func TestReconnectOnlyRestoresOwnedBackchannel(t *testing.T) {
	old, next := newOwnedBackchannel(), newOwnedBackchannel()
	const scheme = "test-owned-backchannel"
	HandleFunc(scheme, func(string) (core.Producer, error) { return next, nil })
	defer delete(handlers, scheme)
	producer := NewProducer(scheme + ":speaker")
	producer.conn, producer.state = old, stateMedias
	t.Cleanup(func() { producer.stop(); old.release(); next.release() })
	media := old.Medias[0]
	stale := core.NewReceiver(nil, media.Codecs[0])
	active := core.NewReceiver(nil, media.Codecs[0])
	if err := producer.AddTrack(media, media.Codecs[0], stale); err != nil {
		t.Fatal(err)
	}
	old.release()
	// Another destination still consumes the old source; global child counts
	// cannot establish ownership by this camera connection.
	other := core.NewSender(nil, stale.Codec)
	other.WithParent(stale)
	defer other.Close()
	if err := producer.AddTrack(media, media.Codecs[0], active); err != nil {
		t.Fatal(err)
	}
	producer.start()
	awaitReconnectStart(t, old.reconnectProducer)
	old.fail()
	awaitReconnectStart(t, next.reconnectProducer)
	packet := &core.Packet{}
	packet.SequenceNumber = 42
	active.Input(packet)
	select {
	case sequence := <-next.received:
		if sequence != 42 {
			t.Fatal("wrong source resumed")
		}
	case <-time.After(time.Second):
		t.Fatal("stale source captured the replacement speaker")
	}
	if len(producer.senders) != 1 {
		t.Fatal("stale sender source was retained")
	}
}

func TestReleasedBackchannelDoesNotKeepDeviceAlive(t *testing.T) {
	conn := newOwnedBackchannel()
	producer := NewProducer("test-owned-backchannel:idle")
	producer.conn, producer.state = conn, stateMedias
	media := conn.Medias[0]
	source := core.NewReceiver(nil, media.Codecs[0])
	if err := producer.AddTrack(media, media.Codecs[0], source); err != nil {
		t.Fatal(err)
	}
	conn.release()
	other := core.NewSender(nil, source.Codec)
	other.WithParent(source)
	defer other.Close()
	stream := &Stream{producers: []*Producer{producer}}
	stream.stopProducers()
	if conn.stopped.Load() != 1 {
		producer.stop()
		t.Fatal("another destination's subscriber kept this camera open")
	}
}

// Like an embedded core.Connection, this endpoint reports its sender parents
// independently of how many other destinations use the same input.
type previewEndpoint struct {
	*reconnectProducer
}

func (p *previewEndpoint) AddTrack(media *core.Media, codec *core.Codec, input *core.Receiver) error {
	p.Senders = append(p.Senders, core.NewSender(media, codec).WithParent(input))
	return nil
}

func TestPreviewReleasesDepartedBackchannels(t *testing.T) {
	video := reconnectMedia("video", core.CodecH264)
	speaker := reconnectMedia("speaker", core.CodecOpus)
	speaker.Direction = core.DirectionSendonly
	camera := &previewEndpoint{newReconnectProducer(video, speaker)}
	producer := &Producer{conn: camera, state: stateMedias, url: "test:camera"}
	stream := &Stream{producers: []*Producer{producer}}
	defer producer.stop()

	watch := video.Clone()
	watch.Direction = core.DirectionSendonly
	preview := &previewEndpoint{newReconnectProducer(watch)}
	if err := stream.AddConsumer(preview); err != nil {
		t.Fatal(err)
	}
	defer stream.RemoveConsumer(preview)

	mic := speaker.Clone()
	mic.Direction = core.DirectionRecvonly
	for range 64 {
		talk := &previewEndpoint{newReconnectProducer(mic)}
		if err := stream.AddConsumer(talk); err != nil {
			t.Fatal(err)
		}
		stream.RemoveConsumer(talk)
	}
	t.Logf("64 departures: attached return sources=%d, tracked inputs=%d", len(camera.GetSenderSources()), len(producer.senders))
	if n := len(camera.GetSenderSources()); n != 0 {
		t.Fatalf("camera still owns %d return inputs", n)
	}
	if len(preview.GetSenderSources()) != 1 || camera.stopped.Load() != 0 {
		t.Fatal("long-lived preview was interrupted")
	}
	data, err := json.Marshal(stream)
	if err != nil {
		t.Fatal(err)
	}
	var info struct{ Consumers []json.RawMessage }
	if err = json.Unmarshal(data, &info); err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(preview)
	if err != nil || len(info.Consumers) != 1 || !bytes.Equal(info.Consumers[0], want) {
		t.Fatalf("preview metadata changed: %s (%v)", info.Consumers, err)
	}
	if n := len(producer.senders); n != 0 {
		t.Fatalf("producer retained %d departed return inputs", n)
	}
}

// The input is owned elsewhere and may be supplied by more than one consumer.
// Stopping one registration must not close the shared receiver.
type borrowedMicrophone struct {
	*previewEndpoint
	input *core.Receiver
}

func (c *borrowedMicrophone) GetTrack(*core.Media, *core.Codec) (*core.Receiver, error) {
	return c.input, nil
}

// Binding happens later, after AddTrack has returned.
type deferredEndpoint struct{ *previewEndpoint }

func (p *deferredEndpoint) AddTrack(*core.Media, *core.Codec, *core.Receiver) error { return nil }

func TestDepartedBackchannelWithReusedInput(t *testing.T) {
	video := reconnectMedia("video", core.CodecH264)
	speaker := reconnectMedia("speaker", core.CodecOpus)
	speaker.Direction = core.DirectionSendonly
	newCamera := func() *previewEndpoint {
		return &previewEndpoint{newReconnectProducer(video, speaker)}
	}
	first, second := &deferredEndpoint{newCamera()}, &deferredEndpoint{newCamera()}
	last := newCamera()
	const scheme = "test-borrowed-input"
	next := []core.Producer{second, last}
	HandleFunc(scheme, func(string) (core.Producer, error) {
		camera := next[0]
		next = next[1:]
		return camera, nil
	})
	defer delete(handlers, scheme)
	producer := &Producer{conn: first, state: stateMedias, url: scheme + ":camera"}
	stream := &Stream{producers: []*Producer{producer}}
	defer producer.stop()

	watch := video.Clone()
	watch.Direction = core.DirectionSendonly
	preview := &previewEndpoint{newReconnectProducer(watch)}
	if err := stream.AddConsumer(preview); err != nil {
		t.Fatal(err)
	}
	defer stream.RemoveConsumer(preview)
	awaitReconnectStart(t, first.reconnectProducer)

	mic := speaker.Clone()
	mic.Direction = core.DirectionRecvonly
	input := core.NewReceiver(mic, mic.Codecs[0])
	subscribe := func() *borrowedMicrophone {
		t.Helper()
		c := &borrowedMicrophone{&previewEndpoint{newReconnectProducer(mic)}, input}
		if err := stream.AddConsumer(c); err != nil {
			t.Fatal(err)
		}
		return c
	}
	old := subscribe()
	first.fail()
	awaitReconnectStart(t, second.reconnectProducer)

	active := subscribe()
	defer stream.RemoveConsumer(active)
	stream.RemoveConsumer(old) // its registration disappeared during reconnect
	if len(producer.senders) != 1 {
		t.Fatal("old consumer removed a later registration of the same input")
	}
	// An unrelated departure between successful AddTrack and eventual bind
	// must not discard an input just because its sender parent is still nil.
	other := subscribe()
	stream.RemoveConsumer(other)
	if err := second.previewEndpoint.AddTrack(speaker, input.Codec, input); err != nil {
		t.Fatal(err)
	}
	second.fail()
	awaitReconnectStart(t, last.reconnectProducer)
	if sources := last.GetSenderSources(); len(sources) != 1 || sources[0] != &input.Node {
		t.Fatal("replacement lost the surviving consumer's input")
	}
}

func TestRenegotiatedBackchannelOwnership(t *testing.T) {
	video := reconnectMedia("video", core.CodecH264)
	speaker := reconnectMedia("speaker", core.CodecOpus)
	speaker.Direction = core.DirectionSendonly
	camera := &previewEndpoint{newReconnectProducer(video, speaker)}
	p := &Producer{conn: camera, state: stateMedias, url: "test:camera"}
	stream := &Stream{producers: []*Producer{p}}
	watch := video.Clone()
	watch.Direction = core.DirectionSendonly
	viewer := &previewEndpoint{newReconnectProducer(watch)}
	if err := stream.AddConsumer(viewer); err != nil {
		t.Fatal(err)
	}
	mic := speaker.Clone()
	mic.Direction = core.DirectionRecvonly
	if err := stream.AddConsumerMedias(viewer, []*core.Media{mic}); err != nil {
		t.Fatal(err)
	}
	if len(stream.consumers) != 1 || len(p.senders) != 1 {
		t.Fatal("renegotiation did not reuse the registered consumer")
	}
	stream.RemoveConsumer(viewer)
	if len(p.senders) != 0 {
		t.Fatal("renegotiated input survived removal")
	}
	if err := stream.AddConsumerMedias(viewer, []*core.Media{mic}); err == nil {
		t.Fatal("removed consumer was reattached")
	}
}
