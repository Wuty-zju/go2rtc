package streams

import (
	"io"
	"sync"
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

type reconnectProducer struct {
	core.Connection
	done chan struct{}
	once sync.Once
}

func (p *reconnectProducer) Start() error { <-p.done; return io.EOF }
func (p *reconnectProducer) Stop() error {
	p.once.Do(func() { close(p.done) })
	return p.Connection.Stop()
}

func newReconnectProducer(ids ...string) *reconnectProducer {
	p := &reconnectProducer{done: make(chan struct{})}
	for _, id := range ids {
		p.Medias = append(p.Medias, &core.Media{
			ID: id, Kind: core.KindVideo, Direction: core.DirectionRecvonly,
			Codecs: []*core.Codec{{Name: core.CodecH265, ClockRate: 90000, PayloadType: core.PayloadTypeRAW}},
		})
	}
	return p
}

func TestReconnectDistinctSameCodecTracks(t *testing.T) {
	for _, ids := range [][]string{{"1", "0"}, {"", ""}, {"new-first", "new-second"}, {"new-first", "0"}} {
		t.Run(ids[0]+"-"+ids[1], func(t *testing.T) {
			previous := newReconnectProducer("0", "1")
			next := newReconnectProducer(ids...)
			const scheme = "test-reconnect-tracks"
			HandleFunc(scheme, func(string) (core.Producer, error) { return next, nil })
			defer delete(handlers, scheme)
			producer := NewProducer(scheme + ":camera")
			producer.conn, producer.state, producer.workerID = previous, stateStart, 1
			for _, media := range previous.Medias {
				track, err := previous.GetTrack(media, media.Codecs[0])
				if err != nil {
					t.Fatal(err)
				}
				producer.receivers = append(producer.receivers, track)
			}
			producer.reconnect(1, 0)
			defer producer.stop()
			if producer.receivers[0] == producer.receivers[1] {
				t.Fatal("two tracks must not reconnect to the same receiver")
			}
			for _, track := range producer.receivers {
				found := false
				for _, current := range next.Receivers {
					found = found || current == track
				}
				if !found {
					t.Fatal("receiver left attached to the stopped connection")
				}
			}
			for i, id := range []string{"0", "1"} {
				for _, current := range next.Medias {
					if current.ID == id && producer.receivers[i].Media.ID != id {
						t.Fatalf("stable media ID changed across reconnect: %s", producer.receivers[i].Media.ID)
					}
				}
			}
		})
	}
}
