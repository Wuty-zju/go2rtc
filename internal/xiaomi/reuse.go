package xiaomi

import (
	"errors"
	"net/url"
	"slices"
	"strings"

	"github.com/AlexxIT/go2rtc/pkg/xiaomi/miss"
)

// The lookup contains configured source URLs only. A pair holds no credentials
// or device connection while both streams are idle.
type lensSource struct {
	pair      *miss.LensPair
	primary   string
	owner     string
	secondary bool
}

func lensSources(streams map[string][]string) map[string]lensSource {
	sources := make(map[string]lensSource)
	counts := make(map[string]int)
	for _, list := range streams {
		for _, raw := range list {
			counts[raw]++
		}
	}
	for name, list := range streams {
		for _, raw := range list {
			u, err := url.Parse(raw)
			if err != nil || u.Scheme != "xiaomi" {
				continue
			}
			q, err := url.ParseQuery(u.RawQuery)
			owner := q.Get("reuse")
			if err != nil || owner == "" || owner == name || len(q["reuse"]) != 1 || len(q["channel"]) != 1 || q.Get("channel") != "2" {
				continue
			}
			var primary string
			for _, candidate := range streams[owner] {
				if strings.HasPrefix(candidate, "xiaomi:") {
					if primary != "" {
						primary = ""
						break
					}
					primary = candidate
				}
			}
			if counts[primary] != 1 {
				continue
			}
			p, err := url.Parse(primary)
			if err != nil || p.User == nil || u.User == nil || p.User.String() != u.User.String() || p.Host != u.Host {
				continue
			}
			pq, err := url.ParseQuery(p.RawQuery)
			if err != nil || pq.Has("reuse") || (pq.Get("channel") != "" && pq.Get("channel") != "0") || q.Get("did") == "" || q.Get("model") != "xiaomi.camera.082ac1" {
				continue
			}
			// Both lenses share the owner's media settings. A secondary may omit them.
			delete(q, "reuse")
			delete(q, "channel")
			matches := true
			for key, values := range q {
				if !slices.Equal(values, pq[key]) {
					matches = false
					break
				}
			}
			if !matches {
				continue
			}
			entry, ok := sources[primary]
			if !ok {
				entry = lensSource{pair: new(miss.LensPair), primary: primary, owner: owner}
				sources[primary] = entry
			}
			entry.secondary = true
			sources[raw] = entry
		}
	}
	return sources
}

var errReuse = errors.New("xiaomi: reuse requires a configured channel=2 source and a matching primary xiaomi.camera.082ac1 stream")
