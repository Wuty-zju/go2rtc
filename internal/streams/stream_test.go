package streams

import (
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/stretchr/testify/require"
)

func TestRecursion(t *testing.T) {
	HandleFunc("test", func(url string) (core.Producer, error) { return nil, nil }) // bypass HasProducer

	// create stream with some source
	stream1, err := New("from_yaml", "test:source")
	require.NoError(t, err)
	require.Len(t, streams, 1)

	// ask another unnamed stream that links go2rtc
	query, err := url.ParseQuery("src=rtsp://localhost:8554/from_yaml?video")
	require.NoError(t, err)
	stream2, err := GetOrPatch(query)
	require.NoError(t, err)

	// check stream is same
	require.Equal(t, stream1, stream2)
	// check stream urls is same
	require.Equal(t, stream1.producers[0].url, stream2.producers[0].url)
	require.Len(t, streams, 2)
}

func TestTempate(t *testing.T) {
	HandleFunc("rtsp", func(url string) (core.Producer, error) { return nil, nil }) // bypass HasProducer
	HandleFunc("ffmpeg", func(url string) (core.Producer, error) { return nil, nil })

	// config from yaml
	stream1, err := New("camera.from_hass", "ffmpeg:{input}#video=copy")
	require.NoError(t, err)
	// request from hass
	stream2, err := Patch("camera.from_hass", "rtsp://example.com")
	require.NoError(t, err)

	require.Equal(t, stream1, stream2)
	require.Equal(t, "ffmpeg:rtsp://example.com#video=copy", stream1.producers[0].url)
}

type playDestination struct {
	core.Connection
	closed bool
}

func (p *playDestination) Start() error { return nil }
func (p *playDestination) Stop() error  { p.closed = true; return nil }
func (p *playDestination) AddTrack(*core.Media, *core.Codec, *core.Receiver) error {
	return nil
}

func TestPlayClosesDestinationOnSourceError(t *testing.T) {
	dst := new(playDestination)
	HandleFunc("playdst", func(string) (core.Producer, error) { return dst, nil })
	HandleFunc("playerror", func(string) (core.Producer, error) { return nil, io.EOF })
	require.ErrorIs(t, NewStream("playdst:").Play("playerror:"), io.EOF)
	require.True(t, dst.closed)
}

type removingPlaySource struct {
	core.Connection
	stream *Stream
}

func (p *removingPlaySource) Start() error { return nil }

func (p *removingPlaySource) Stop() error {
	p.stream.RemoveProducer(p)
	return nil
}

func TestPlayStopsOutsideStreamLock(t *testing.T) {
	stream := new(Stream)
	p := &removingPlaySource{stream: stream}
	stream.producers = []*Producer{{conn: p, state: stateInternal}}
	done := make(chan error, 1)
	go func() { done <- stream.Play("") }()
	select {
	case err := <-done:
		require.NoError(t, err)
		require.Empty(t, stream.producers)
	case <-time.After(time.Second):
		t.Fatal("stopping playback blocked producer removal")
	}
}
