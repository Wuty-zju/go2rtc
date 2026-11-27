package mjpeg

import (
	"context"
	"io"
	"net/http/httptest"
	"net/url"
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

func TestFrameCacheKey(t *testing.T) {
	tests := []struct {
		name  string
		query url.Values
		want  string
	}{
		{
			name:  "includes source and output options",
			query: url.Values{"src": {"camera"}, "width": {"640"}, "height": {"480"}, "rotate": {"90"}},
			want:  "height=480&rotate=90&src=camera&width=640",
		},
		{
			name:  "includes short output option names",
			query: url.Values{"src": {"camera"}, "w": {"640"}, "h": {"480"}},
			want:  "h=480&src=camera&w=640",
		},
		{
			name:  "excludes cache lifetime and unrelated options",
			query: url.Values{"src": {"camera"}, "width": {"640"}, "cache": {"1s"}, "quality": {"80"}},
			want:  "src=camera&width=640",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cacheKey(tt.query); got != tt.want {
				t.Fatalf("frameCacheKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFrameCacheKeySeparatesOutputVariants(t *testing.T) {
	base := cacheKey(url.Values{"src": {"camera"}})
	width := cacheKey(url.Values{"src": {"camera"}, "width": {"640"}})
	rotate := cacheKey(url.Values{"src": {"camera"}, "rotate": {"90"}})
	otherSource := cacheKey(url.Values{"src": {"other-camera"}})

	if base == width || base == rotate || base == otherSource {
		t.Fatalf("different frame requests must have different cache keys: base=%q width=%q rotate=%q source=%q", base, width, rotate, otherSource)
	}
}

func TestFrameCacheKeyIgnoresCacheLifetime(t *testing.T) {
	short := cacheKey(url.Values{"src": {"camera"}, "cache": {"1s"}})
	long := cacheKey(url.Values{"src": {"camera"}, "cache": {"1m"}})

	if short != long {
		t.Fatalf("cache lifetime must not change the frame cache key: short=%q long=%q", short, long)
	}
}
