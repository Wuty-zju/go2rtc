package streams

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

type reconnectProducer struct {
	core.Connection
	done      chan struct{}
	started   chan struct{}
	once      sync.Once
	stopped   atomic.Int32
	failTrack string
}

func (p *reconnectProducer) Start() error { close(p.started); <-p.done; return io.EOF }
func (p *reconnectProducer) Stop() error {
	p.stopped.Add(1)
	p.once.Do(func() { close(p.done) })
	return p.Connection.Stop()
}
func (p *reconnectProducer) fail() { p.once.Do(func() { close(p.done) }) }
func (p *reconnectProducer) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	if p.failTrack != "" && media.ID == p.failTrack {
		return nil, errors.New("test: track unavailable")
	}
	return p.Connection.GetTrack(media, codec)
}

func reconnectMedia(id string, names ...string) *core.Media {
	media := &core.Media{ID: id, Kind: core.KindVideo, Direction: core.DirectionRecvonly}
	for _, name := range names {
		codec := &core.Codec{Name: name, ClockRate: 90000, PayloadType: core.PayloadTypeRAW}
		if name == core.CodecOpus {
			media.Kind = core.KindAudio
			codec.ClockRate = 48000
			codec.Channels = 2
		}
		media.Codecs = append(media.Codecs, codec)
	}
	return media
}
func newReconnectProducer(medias ...*core.Media) *reconnectProducer {
	return &reconnectProducer{Connection: core.Connection{Medias: medias}, done: make(chan struct{}), started: make(chan struct{})}
}

type reconnectConsumer struct {
	sender  *core.Sender
	packets chan uint16
}

func newReconnectConsumer(track *core.Receiver) *reconnectConsumer {
	c := &reconnectConsumer{packets: make(chan uint16, 16)}
	c.sender = core.NewSender(nil, track.Codec)
	c.sender.Handler = func(pkt *core.Packet) { c.packets <- pkt.SequenceNumber }
	c.sender.WithParent(track)
	c.sender.Start()
	return c
}
func (c *reconnectConsumer) close() { c.sender.Close(); c.sender.Wait() }
func (c *reconnectConsumer) expect(t *testing.T, sequence uint16) {
	t.Helper()
	select {
	case actual := <-c.packets:
		if actual != sequence {
			t.Fatalf("wanted packet %d, got %d", sequence, actual)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("packet %d did not reach the original consumer", sequence)
	}
}
func awaitReconnectStart(t *testing.T, p *reconnectProducer) {
	t.Helper()
	select {
	case <-p.started:
	case <-time.After(3 * time.Second):
		t.Fatal("replacement producer did not start")
	}
}

func attachReconnect(t *testing.T, previous *reconnectProducer, next ...*reconnectProducer) (*Producer, []*reconnectConsumer) {
	t.Helper()
	const scheme = "test-reconnect-tracks"
	var mu sync.Mutex
	remaining := next
	HandleFunc(scheme, func(string) (core.Producer, error) {
		mu.Lock()
		defer mu.Unlock()
		if len(remaining) == 0 {
			return nil, errors.New("test: no replacement")
		}
		p := remaining[0]
		remaining = remaining[1:]
		return p, nil
	})
	producer := NewProducer(scheme + ":camera")
	producer.conn, producer.state = previous, stateMedias
	var consumers []*reconnectConsumer
	for _, media := range previous.Medias {
		for _, codec := range media.Codecs {
			track, err := producer.GetTrack(media, codec)
			if err != nil {
				t.Fatal(err)
			}
			consumers = append(consumers, newReconnectConsumer(track))
		}
	}
	producer.start()
	awaitReconnectStart(t, previous)
	t.Cleanup(func() {
		producer.stop()
		for _, cons := range consumers {
			cons.close()
		}
		delete(handlers, scheme)
	})
	return producer, consumers
}

func TestReconnectDistinctSameCodecTracks(t *testing.T) {
	for _, test := range []struct {
		name  string
		ids   []string
		order []int
	}{
		{"reordered", []string{"1", "0"}, []int{1, 0}},
		{"absent", []string{"", ""}, []int{0, 1}},
		{"changed", []string{"new-first", "new-second"}, []int{0, 1}},
		{"partially-retained", []string{"new-first", "0"}, []int{1, 0}},
	} {
		t.Run(test.name, func(t *testing.T) {
			previous := newReconnectProducer(reconnectMedia("0", core.CodecH265), reconnectMedia("1", core.CodecH265))
			next := newReconnectProducer(reconnectMedia(test.ids[0], core.CodecH265), reconnectMedia(test.ids[1], core.CodecH265))
			producer, consumers := attachReconnect(t, previous, next)
			previous.fail()
			awaitReconnectStart(t, next)
			if previous.stopped.Load() != 1 {
				t.Fatal("old producer was not stopped")
			}
			emitReconnect(t, next, 100)
			for i, cons := range consumers {
				cons.expect(t, 100+uint16(test.order[i]))
			}
			if producer.receivers[0] == producer.receivers[1] {
				t.Fatal("replacement receiver reused")
			}
		})
	}
}

func TestReconnectMultipleCodecsAndConsumers(t *testing.T) {
	previous := newReconnectProducer(reconnectMedia("video", core.CodecH264, core.CodecH265), reconnectMedia("audio", core.CodecOpus))
	next := newReconnectProducer(reconnectMedia("audio", core.CodecOpus), reconnectMedia("video", core.CodecH265, core.CodecH264))
	producer, consumers := attachReconnect(t, previous, next)
	extra := newReconnectConsumer(producer.receivers[0])
	defer extra.close()
	previous.fail()
	awaitReconnectStart(t, next)
	emitReconnect(t, next, 100)
	consumers[0].expect(t, 102)
	consumers[1].expect(t, 101)
	consumers[2].expect(t, 100)
	extra.expect(t, 102)
}

func TestReconnectMissingTrackReturns(t *testing.T) {
	previous := newReconnectProducer(reconnectMedia("0", core.CodecH265), reconnectMedia("1", core.CodecH265))
	missing := newReconnectProducer(reconnectMedia("0", core.CodecH265))
	restored := newReconnectProducer(reconnectMedia("1", core.CodecH265), reconnectMedia("0", core.CodecH265))
	_, consumers := attachReconnect(t, previous, missing, restored)
	previous.fail()
	awaitReconnectStart(t, missing)
	packet := &core.Packet{}
	packet.SequenceNumber = 100
	missing.Receivers[0].Input(packet)
	consumers[0].expect(t, 100)
	if consumers[1].sender.State() == "closed" {
		t.Fatal("temporarily missing track closed the original consumer")
	}
	missing.fail()
	awaitReconnectStart(t, restored)
	emitReconnect(t, restored, 200)
	consumers[0].expect(t, 201)
	consumers[1].expect(t, 200)
}

func TestReconnectFailedStableTrackIsNotReassigned(t *testing.T) {
	previous := newReconnectProducer(reconnectMedia("0", core.CodecH265), reconnectMedia("1", core.CodecH265))
	next := newReconnectProducer(reconnectMedia("new-first", core.CodecH265), reconnectMedia("0", core.CodecH265))
	next.failTrack = "0"
	restored := newReconnectProducer(reconnectMedia("0", core.CodecH265), reconnectMedia("new-first", core.CodecH265))
	_, consumers := attachReconnect(t, previous, next, restored)
	previous.fail()
	awaitReconnectStart(t, next)
	track, err := next.GetTrack(next.Medias[0], next.Medias[0].Codecs[0])
	if err != nil {
		t.Fatal(err)
	}
	packet := &core.Packet{}
	packet.SequenceNumber = 100
	track.Input(packet)
	consumers[1].expect(t, 100)
	if consumers[0].sender.State() == "closed" {
		t.Fatal("GetTrack failure closed a stable consumer")
	}
	next.fail()
	awaitReconnectStart(t, restored)
	emitReconnect(t, restored, 200)
	consumers[0].expect(t, 200)
	consumers[1].expect(t, 201)
}

func TestReconnectChangedIDsMatchCodec(t *testing.T) {
	previous := newReconnectProducer(reconnectMedia("old-h264", core.CodecH264), reconnectMedia("old-h265", core.CodecH265))
	next := newReconnectProducer(reconnectMedia("new-h265", core.CodecH265), reconnectMedia("new-h264", core.CodecH264))
	_, consumers := attachReconnect(t, previous, next)
	previous.fail()
	awaitReconnectStart(t, next)
	emitReconnect(t, next, 100)
	consumers[0].expect(t, 101)
	consumers[1].expect(t, 100)
}

func TestReconnectDialFailureKeepsConsumers(t *testing.T) {
	previous := newReconnectProducer(reconnectMedia("0", core.CodecH265))
	next := newReconnectProducer(reconnectMedia("0", core.CodecH265))
	_, consumers := attachReconnect(t, previous, next)
	handler := handlers["test-reconnect-tracks"]
	var attempts atomic.Int32
	handlers["test-reconnect-tracks"] = func(source string) (core.Producer, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("test: authentication unavailable")
		}
		return handler(source)
	}
	previous.fail()
	awaitReconnectStart(t, next)
	if attempts.Load() != 2 || previous.stopped.Load() != 1 {
		t.Fatal("dial retry or old-connection cleanup failed")
	}
	packet := &core.Packet{}
	packet.SequenceNumber = 100
	next.Receivers[0].Input(packet)
	consumers[0].expect(t, 100)
}

func emitReconnect(t *testing.T, p *reconnectProducer, sequence uint16) {
	t.Helper()
	for _, media := range p.Medias {
		for _, codec := range media.Codecs {
			track, err := p.GetTrack(media, codec)
			if err != nil {
				t.Fatal(err)
			}
			packet := &core.Packet{}
			packet.SequenceNumber = sequence
			track.Input(packet)
			sequence++
		}
	}
}
