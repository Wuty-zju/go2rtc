package xiaomi

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/AlexxIT/go2rtc/internal/streams"
)

func TestPtimeRejectedBeforeCloudDiscovery(t *testing.T) {
	transport := http.DefaultTransport
	attempts := 0
	http.DefaultTransport = blockedCloud{attempts: &attempts}
	defer func() { http.DefaultTransport = transport }()
	Init()
	for _, options := range []string{"ptime=40", "ptime=20&audio=0", "ptime=20&model=isa.camera.isc5"} {
		_, err := streams.GetProducer("xiaomi://user:cn@192.0.2.1?" + options)
		if err == nil || !strings.Contains(err.Error(), "ptime") {
			t.Fatal("invalid options reached cloud discovery", options, err)
		}
	}
	if attempts != 0 {
		t.Fatal("invalid options attempted cloud discovery", attempts)
	}
}

type blockedCloud struct{ attempts *int }

func (b blockedCloud) RoundTrip(*http.Request) (*http.Response, error) {
	*b.attempts++
	return nil, errors.New("test blocks cloud access")
}
