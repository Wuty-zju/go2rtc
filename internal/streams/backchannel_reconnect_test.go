package streams

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

func TestReleasedBackchannelOwnership(t *testing.T) {
	codec := &core.Codec{Name: core.CodecOpus, ClockRate: 48000}
	stale, active := core.NewReceiver(nil, codec), core.NewReceiver(nil, codec)
	other := core.NewSender(nil, codec)
	other.WithParent(stale)
	defer other.Close()
	conn := newReconnectProducer()
	sender := core.NewSender(nil, codec)
	sender.WithParent(active)
	conn.Senders = []*core.Sender{sender}
	producer := NewProducer("test:owned-speaker")
	producer.conn, producer.state = conn, stateMedias
	producer.senders = []*backchannel{{Receiver: stale, producer: producer}, {Receiver: active, producer: producer}}
	defer producer.stop()
	sources := producer.activeSenders()
	if len(sources) != 1 || sources[0].Receiver != active {
		t.Fatal("another destination was counted as this connection's speaker")
	}
	sender.Close()
	stream := &Stream{producers: []*Producer{producer}}
	stream.stopProducers()
	if conn.stopped.Load() != 1 {
		t.Fatal("released speaker kept device alive")
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
