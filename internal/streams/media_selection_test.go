package streams

import (
	"io"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

type selectionSource struct {
	core.Connection
	started chan struct{}
	done    chan struct{}
	once    sync.Once
	stops   atomic.Int32
}

func (p *selectionSource) Start() error { close(p.started); <-p.done; return io.EOF }
func (p *selectionSource) Stop() error {
	p.stops.Add(1)
	p.once.Do(func() { close(p.done) })
	return p.Connection.Stop()
}
func newSelectionSource() *selectionSource {
	p := &selectionSource{started: make(chan struct{}), done: make(chan struct{})}
	for _, id := range []string{"front", "rear-Lens"} {
		p.Medias = append(p.Medias, &core.Media{ID: id, Kind: core.KindVideo, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{{Name: core.CodecH265, ClockRate: 90000, PayloadType: core.PayloadTypeRAW}}})
	}
	p.Medias = append(p.Medias, &core.Media{ID: "mic", Kind: core.KindAudio, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{{Name: core.CodecOpus, ClockRate: 48000, Channels: 2}}})
	return p
}
func (p *selectionSource) emit(t *testing.T, id string, sequence uint16) {
	t.Helper()
	for _, media := range p.Medias {
		if media.ID != id {
			continue
		}
		for _, track := range p.Receivers {
			if track.Codec == media.Codecs[0] {
				packet := &core.Packet{}
				packet.SequenceNumber = sequence
				track.Input(packet)
				return
			}
		}
	}
	t.Fatalf("no subscribed source track for %s", id)
}

type selectionConsumer struct {
	core.Connection
	packets chan uint16
}

func newSelectionConsumer(t *testing.T, query string) *selectionConsumer {
	t.Helper()
	q, err := url.ParseQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	c := &selectionConsumer{packets: make(chan uint16, 16)}
	c.Medias = core.ParseQuery(q)
	// These are local transport IDs, unrelated to the source track selector.
	for _, media := range c.Medias {
		media.ID = "consumer-control"
	}
	return c
}
func (c *selectionConsumer) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	sender := core.NewSender(media, codec)
	sender.Handler = func(p *core.Packet) { c.packets <- p.SequenceNumber }
	sender.WithParent(track)
	sender.Start()
	c.Senders = append(c.Senders, sender)
	return nil
}
func (c *selectionConsumer) Stop() error {
	err := c.Connection.Stop()
	for _, sender := range c.Senders {
		sender.Wait()
	}
	return err
}
func (c *selectionConsumer) expect(t *testing.T, sequences ...uint16) {
	t.Helper()
	wanted := make(map[uint16]int)
	for _, sequence := range sequences {
		wanted[sequence]++
	}
	for range sequences {
		select {
		case actual := <-c.packets:
			if wanted[actual] == 0 {
				t.Fatalf("unexpected packet %d; wanted %v", actual, sequences)
			}
			wanted[actual]--
		case <-time.After(2 * time.Second):
			t.Fatalf("packets %v not delivered", wanted)
		}
	}
}

func TestSourceMediaSelectionSharedLifecycle(t *testing.T) {
	source := newSelectionSource()
	var dials atomic.Int32
	const scheme = "test-source-selection"
	HandleFunc(scheme, func(string) (core.Producer, error) { dials.Add(1); return source, nil })
	defer delete(handlers, scheme)
	stream := NewStream(scheme + ":device")
	front := newSelectionConsumer(t, "video=h265@front&audio")
	rear := newSelectionConsumer(t, "video=copy@rear-Lens&audio")
	if err := stream.AddConsumer(front); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.RemoveConsumer(front); stream.RemoveConsumer(rear) })
	select {
	case <-source.started:
	case <-time.After(2 * time.Second):
		t.Fatal("source did not start")
	}
	source.emit(t, "front", 10)
	source.emit(t, "mic", 11)
	front.expect(t, 10, 11)
	// A consumer joining the second lens must attach to the existing producer.
	if err := stream.AddConsumer(rear); err != nil {
		t.Fatal(err)
	}
	source.emit(t, "front", 20)
	source.emit(t, "rear-Lens", 21)
	source.emit(t, "mic", 22)
	front.expect(t, 20, 22)
	rear.expect(t, 21, 22)
	if dials.Load() != 1 {
		t.Fatalf("late join opened %d device sessions", dials.Load())
	}
	stream.RemoveConsumer(front)
	if source.stops.Load() != 0 {
		t.Fatal("closing one outlet stopped the shared source")
	}
	source.emit(t, "rear-Lens", 30)
	rear.expect(t, 30)
	stream.RemoveConsumer(rear)
	if source.stops.Load() != 1 {
		t.Fatal("last outlet did not close the device session")
	}
}

func TestSourceMediaSelectionQueryContract(t *testing.T) {
	for _, test := range []struct {
		name, query string
		want        []string
	}{
		{"default", "video", []string{"front"}},
		{"all", "video=all", []string{"front", "rear-Lens"}},
		{"exact-id", "video=h265@rear-Lens", []string{"rear-Lens"}},
		{"any-id", "video=@rear-Lens", []string{"rear-Lens"}},
		{"codec-list", "video=h264,h265@rear-Lens", []string{"rear-Lens"}},
		{"unknown-id", "video=copy@unknown", nil},
		{"wrong-codec", "video=h264@rear-Lens", nil},
		{"empty-id", "video=h265@", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := newSelectionSource()
			const scheme = "test-selection-query"
			HandleFunc(scheme, func(string) (core.Producer, error) { return source, nil })
			defer delete(handlers, scheme)
			stream := NewStream(scheme + ":camera")
			consumer := newSelectionConsumer(t, test.query)
			err := stream.AddConsumer(consumer)
			defer stream.RemoveConsumer(consumer)
			if test.want == nil {
				if err == nil {
					t.Fatal("invalid selection silently matched")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(consumer.Senders) != len(test.want) {
				t.Fatalf("wanted %d tracks, got %d", len(test.want), len(consumer.Senders))
			}
			for i, id := range test.want {
				// Sender codec may be ANY; its actual parent must still be the selected source.
				source.emit(t, id, 100+uint16(i))
				consumer.expect(t, 100+uint16(i))
			}
		})
	}
}
