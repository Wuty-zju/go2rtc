package xiaomi

import (
	"errors"
	"slices"
	"sync"

	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/core"
)

// A logical view of another Stream; only the primary producer owns the camera.
type reuseProducer struct {
	core.Connection
	source  *streams.Stream
	input   *reuseConsumer
	speaker *core.Receiver
	done    chan struct{}
	once    sync.Once
}
type reuseConsumer struct {
	core.Connection
	output *reuseProducer
}

func newReuseProducer(name string) (core.Producer, error) {
	source := streams.Get(name)
	if source == nil {
		return nil, errors.New("xiaomi: missing reuse stream")
	}
	p := &reuseProducer{source: source, done: make(chan struct{})}
	p.Connection = core.Connection{ID: core.NewID(), FormatName: "xiaomi/reuse", Source: name}
	c := &reuseConsumer{output: p}
	c.Connection = core.Connection{ID: core.NewID(), FormatName: "xiaomi/reuse", Source: name, Medias: core.ParseQuery(map[string][]string{"video": {"copy@1"}, "audio": {""}})}
	c.Medias = append(c.Medias, &core.Media{Kind: core.KindAudio, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{
		{Name: core.CodecOpus, ClockRate: 48000, Channels: 2}, {Name: core.CodecPCMA, ClockRate: 8000}, {Name: core.CodecPCMA, ClockRate: 16000},
	}})
	p.input = c
	if err := source.AddConsumer(c); err != nil {
		p.Stop()
		return nil, err
	}
	for _, track := range c.Receivers {
		if track.Media.Direction == core.DirectionRecvonly && len(track.Senders()) > 0 {
			p.speaker = track
			p.Medias = append(p.Medias, &core.Media{Kind: core.KindAudio, Direction: core.DirectionSendonly, Codecs: []*core.Codec{track.Codec}})
			break
		}
	}
	for _, m := range p.Medias {
		if m.Kind == core.KindVideo {
			return p, nil
		}
	}
	p.Stop()
	return nil, errors.New("xiaomi: reuse source has no second video")
}

func (c *reuseConsumer) AddTrack(m *core.Media, _ *core.Codec, track *core.Receiver) error {
	media := &core.Media{ID: m.SourceID, Kind: m.Kind, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{track.Codec}}
	receiver := core.NewReceiver(media, track.Codec)
	c.output.Medias = append(c.output.Medias, media)
	c.output.Receivers = append(c.output.Receivers, receiver)
	// The output is a root receiver: losing its last subscriber must not
	// auto-detach the input owned by this logical stream.
	link := core.NewReceiver(m, track.Codec)
	link.Input = receiver.Input
	link.Node.WithParent(&track.Node)
	c.Receivers = append(c.Receivers, link)
	return nil
}

func (c *reuseConsumer) Start() error { return c.output.Start() }

func (p *reuseProducer) AddTrack(m *core.Media, _ *core.Codec, track *core.Receiver) error {
	select {
	case <-p.done:
		return errors.New("xiaomi: reuse is stopped")
	default:
	}
	target := p.speaker
	if target == nil {
		return errors.New("xiaomi: reuse source has no speaker")
	}
	if !target.Codec.Match(track.Codec) {
		return errors.New("xiaomi: incompatible reuse speaker codec")
	}
	p.Senders = slices.DeleteFunc(p.Senders, func(s *core.Sender) bool { return s.State() == "closed" })
	s := core.NewSender(m, track.Codec)
	s.Handler = target.Input
	s.HandleRTP(track)
	p.Senders = append(p.Senders, s)
	return nil
}

func (p *reuseProducer) Start() error { <-p.done; return nil }

func (p *reuseProducer) Stop() error {
	p.once.Do(func() {
		_ = p.Connection.Stop()
		p.source.RemoveConsumer(p.input)
		close(p.done)
	})
	return nil
}
