package streams

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

type state byte

const (
	stateNone state = iota
	stateMedias
	stateTracks
	stateStart
	stateExternal
	stateInternal
)

type Producer struct {
	core.Listener

	url      string
	template string

	conn           core.Producer
	receivers      []*core.Receiver
	receiverMedias []*core.Media
	senders        []*core.Receiver

	state    state
	mu       sync.Mutex
	workerID int
}

const SourceTemplate = "{input}"

func NewProducer(source string) *Producer {
	if strings.Contains(source, SourceTemplate) {
		return &Producer{template: source}
	}

	return &Producer{url: source}
}

func (p *Producer) SetSource(s string) {
	if p.template == "" {
		p.url = s
	} else {
		p.url = strings.Replace(p.template, SourceTemplate, s, 1)
	}
}

func (p *Producer) Dial() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == stateNone {
		conn, err := GetProducer(p.url)
		if err != nil {
			return err
		}

		p.conn = conn
		p.state = stateMedias
	}

	return nil
}

func (p *Producer) GetMedias() []*core.Media {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn == nil {
		return nil
	}

	return p.conn.GetMedias()
}

func (p *Producer) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == stateNone {
		return nil, errors.New("get track from none state")
	}

	for _, track := range p.receivers {
		if track.Codec == codec {
			return track, nil
		}
	}

	track, err := p.conn.GetTrack(media, codec)
	if err != nil {
		return nil, err
	}

	p.receivers = append(p.receivers, track)
	p.receiverMedias = append(p.receiverMedias, media)

	if p.state == stateMedias {
		p.state = stateTracks
	}

	return track, nil
}

func (p *Producer) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == stateNone {
		return errors.New("add track from none state")
	}

	if err := p.conn.(core.Consumer).AddTrack(media, codec, track); err != nil {
		return err
	}

	p.senders = append(p.senders, track)

	if p.state == stateMedias {
		p.state = stateTracks
	}

	return nil
}

func (p *Producer) MarshalJSON() ([]byte, error) {
	if conn := p.conn; conn != nil {
		return json.Marshal(conn)
	}
	info := map[string]string{"url": p.url}
	return json.Marshal(info)
}

// internals

func (p *Producer) start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state != stateTracks {
		return
	}

	log.Debug().Msgf("[streams] start producer url=%s", p.url)

	p.state = stateStart
	p.workerID++

	go p.worker(p.conn, p.workerID)
}

func (p *Producer) worker(conn core.Producer, workerID int) {
	if err := conn.Start(); err != nil {
		p.mu.Lock()
		closed := p.workerID != workerID
		p.mu.Unlock()

		if closed {
			return
		}

		log.Warn().Err(err).Str("url", p.url).Caller().Send()
	}

	p.reconnect(workerID, 0)
}

func (p *Producer) reconnect(workerID, retry int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.workerID != workerID {
		log.Trace().Msgf("[streams] stop reconnect url=%s", p.url)
		return
	}

	log.Debug().Msgf("[streams] retry=%d to url=%s", retry, p.url)

	conn, err := GetProducer(p.url)
	if err != nil {
		log.Debug().Msgf("[streams] producer=%s", err)

		timeout := time.Minute
		if retry < 5 {
			timeout = time.Second
		} else if retry < 10 {
			timeout = time.Second * 5
		} else if retry < 20 {
			timeout = time.Second * 10
		}

		time.AfterFunc(timeout, func() {
			p.reconnect(workerID, retry+1)
		})
		return
	}

	medias := conn.GetMedias()
	used := make([]bool, len(p.receivers))
	targets := make(map[*core.Receiver]bool)
	type mediaID struct{ kind, id string }
	stable := make(map[mediaID]bool)
	for _, media := range medias {
		if media.Direction == core.DirectionRecvonly && media.ID != "" {
			stable[mediaID{media.Kind, media.ID}] = true
		}
	}

	// Reserve stable identities even when obtaining one of their tracks fails.
	for pass := 0; pass < 2; pass++ {
		for _, media := range medias {
			if media.Direction != core.DirectionRecvonly {
				continue
			}
			for i, receiver := range p.receivers {
				previous := p.receiverMedias[i]
				if used[i] || previous.Kind != media.Kind {
					continue
				}
				if pass == 0 {
					if media.ID == "" || previous.ID != media.ID {
						continue
					}
				} else if stable[mediaID{previous.Kind, previous.ID}] {
					continue
				}
				codec := media.MatchCodec(receiver.Codec)
				if codec == nil {
					continue
				}
				track, err := conn.GetTrack(media, codec)
				if err != nil || targets[track] {
					continue
				}
				receiver.Replace(track)
				p.receivers[i] = track
				p.receiverMedias[i] = media
				used[i], targets[track] = true, true
			}
		}
	}

	// Keep unavailable tracks attached to the stream, not the connection we stop.
	// A later reconnect can restore them without losing their consumers or IDs.
	for i, receiver := range p.receivers {
		if !used[i] {
			track := core.NewReceiver(p.receiverMedias[i], receiver.Codec)
			receiver.Replace(track)
			p.receivers[i] = track
		}
	}

	p.senders = p.activeSenders()
	for _, media := range medias {
		if media.Direction == core.DirectionSendonly {
			for _, sender := range p.senders {
				codec := media.MatchCodec(sender.Codec)
				if codec == nil {
					continue
				}

				_ = conn.(core.Consumer).AddTrack(media, codec, sender)
			}
		}
	}

	// stop previous connection after moving tracks (fix ghost exec/ffmpeg)
	_ = p.conn.Stop()
	// swap connections
	p.conn = conn

	go p.worker(conn, workerID)
}

// activeSenders must be called with p.mu held. A source may have subscribers
// belonging to other destinations, so its global child count is insufficient.
func (p *Producer) activeSenders() []*core.Receiver {
	var active []*core.Receiver
	if conn, ok := p.conn.(interface{ GetSenderSources() []*core.Node }); ok {
		sources := conn.GetSenderSources()
		for _, receiver := range p.senders {
			for _, source := range sources {
				if source == &receiver.Node {
					active = append(active, receiver)
					break
				}
			}
		}
	} else {
		for _, receiver := range p.senders {
			if len(receiver.Senders()) > 0 {
				active = append(active, receiver)
			}
		}
	}
	return active
}

func (p *Producer) hasConsumers() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, receiver := range p.receivers {
		if len(receiver.Senders()) > 0 {
			return true
		}
	}
	return len(p.activeSenders()) > 0
}

func (p *Producer) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch p.state {
	case stateExternal:
		log.Trace().Msgf("[streams] skip stop external producer")
		return
	case stateNone:
		log.Trace().Msgf("[streams] skip stop none producer")
		return
	case stateStart:
		p.workerID++
	}

	log.Debug().Msgf("[streams] stop producer url=%s", p.url)

	for _, receiver := range p.receivers {
		receiver.Close()
	}
	if p.conn != nil {
		_ = p.conn.Stop()
		p.conn = nil
	}

	p.state = stateNone
	p.receivers = nil
	p.receiverMedias = nil
	p.senders = nil
}
