package streams

import (
	"errors"
	"slices"
	"time"
	"weak"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

func (s *Stream) Play(urlOrProd any) error {
	s.mu.Lock()
	producers := slices.Clone(s.producers)
	s.mu.Unlock()

	// A producer may remove itself from the stream when stopped.
	type target struct {
		url  string
		conn core.Producer
	}
	targets := make([]target, 0, len(producers))
	for _, producer := range producers {
		producer.mu.Lock()
		dst := target{producer.url, producer.conn}
		internal := producer.state == stateInternal
		producer.mu.Unlock()
		if internal && dst.conn != nil {
			_ = dst.conn.Stop()
		}
		targets = append(targets, dst)
	}

	var source string
	var src core.Producer

	switch urlOrProd.(type) {
	case string:
		if source = urlOrProd.(string); source == "" {
			return nil
		}
	case core.Producer:
		src = urlOrProd.(core.Producer)
	}

	for _, dst := range targets {
		cons, ok := dst.conn.(core.Consumer)
		if !ok {
			continue
		}

		if src == nil {
			var err error
			if src, err = GetProducer(source); err != nil {
				return err
			}
		}

		track := matchMedia(src, cons)
		if track == nil {
			continue
		}

		s.AddInternalProducer(src, track)

		go func() {
			_ = src.Start()
			s.RemoveProducer(src)
		}()

		return nil
	}

	for _, target := range targets {
		// start new client
		dst, err := GetProducer(target.url)
		if err != nil {
			continue
		}

		// check if client support consumer interface
		cons, ok := dst.(core.Consumer)
		if !ok {
			_ = dst.Stop()
			continue
		}

		// start new producer
		if src == nil {
			if src, err = GetProducer(source); err != nil {
				_ = dst.Stop()
				return err
			}
		}

		track := matchMedia(src, cons)
		if track == nil {
			_ = dst.Stop()
			continue
		}

		s.AddInternalProducer(src, track)
		s.AddInternalConsumer(cons)

		go func() {
			_ = dst.Start()
			_ = src.Stop()
			s.RemoveInternalConsumer(cons)
		}()

		go func() {
			_ = src.Start()
			// little timeout before stop dst, so the buffer can be transferred
			time.Sleep(time.Second)
			_ = dst.Stop()
			s.RemoveProducer(src)
		}()

		return nil
	}

	if source != "" && src != nil {
		_ = src.Stop()
	}
	return errors.New("can't find consumer")
}

func (s *Stream) AddInternalProducer(conn core.Producer, track *core.Receiver) {
	producer := &Producer{conn: conn, receivers: []*core.Receiver{track}, state: stateInternal, url: "internal"}
	s.mu.Lock()
	s.producers = append(s.producers, producer)
	s.mu.Unlock()
}

func (s *Stream) AddInternalConsumer(conn core.Consumer) {
	s.mu.Lock()
	s.consumers = append(s.consumers, streamConsumer{Consumer: conn})
	s.mu.Unlock()
}

func (s *Stream) RemoveInternalConsumer(conn core.Consumer) {
	var backchannels []weak.Pointer[backchannel]
	s.mu.Lock()
	for i, consumer := range s.consumers {
		if consumer.Consumer == conn {
			backchannels = consumer.backchannels
			s.consumers = slices.Delete(s.consumers, i, i+1)
			break
		}
	}
	s.mu.Unlock()
	for _, ref := range backchannels {
		if back := ref.Value(); back != nil {
			back.remove()
		}
	}
}

func matchMedia(prod core.Producer, cons core.Consumer) *core.Receiver {
	for _, consMedia := range cons.GetMedias() {
		for _, prodMedia := range prod.GetMedias() {
			if prodMedia.Direction != core.DirectionRecvonly {
				continue
			}

			prodCodec, consCodec := prodMedia.MatchMedia(consMedia)
			if prodCodec == nil {
				continue
			}

			track, err := prod.GetTrack(prodMedia, prodCodec)
			if err != nil {
				log.Warn().Err(err).Msg("[streams] can't get track")
				continue
			}

			if err = cons.AddTrack(consMedia, consCodec, track); err != nil {
				log.Warn().Err(err).Msg("[streams] can't add track")
				continue
			}

			return track
		}
	}

	return nil
}
