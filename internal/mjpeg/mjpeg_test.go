package mjpeg

import (
	"context"
	"io"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/core"
)

type silentCamera struct {
	core.Connection
	ready, done chan struct{}
	once        sync.Once
}

func (p *silentCamera) Start() error { <-p.done; return io.EOF }
func (p *silentCamera) Stop() error {
	p.once.Do(func() { close(p.done) })
	return p.Connection.Stop()
}
func (p *silentCamera) GetTrack(m *core.Media, c *core.Codec) (*core.Receiver, error) {
	r, err := p.Connection.GetTrack(m, c)
	close(p.ready)
	return r, err
}

func TestCancelledSnapshotWithoutKeyframes(t *testing.T) {
	p := &silentCamera{ready: make(chan struct{}), done: make(chan struct{})}
	p.Medias = []*core.Media{{Kind: core.KindVideo, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{{Name: core.CodecH264}}}}
	streams.HandleFunc("silent", func(string) (core.Producer, error) { return p, nil })
	if _, err := streams.New("snapshot-cancel", "silent:camera"); err != nil {
		t.Fatal(err)
	}
	defer streams.Delete("snapshot-cancel")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest("GET", "/api/frame.jpeg?src=snapshot-cancel", nil).WithContext(ctx)
	res := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { handlerKeyframe(res, req); close(done) }()
	select {
	case <-p.ready:
	case <-time.After(time.Second):
		t.Fatal("snapshot did not subscribe")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled request still waits for a keyframe")
	}
	if len(p.Receivers[0].Senders()) != 0 || res.Body.Len() != 0 {
		t.Fatal("cancelled request retained its subscription or returned an image")
	}
	select {
	case <-p.done:
	default:
		t.Fatal("last snapshot consumer did not release the producer")
	}
}
