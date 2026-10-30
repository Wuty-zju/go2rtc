package xiaomi

import (
	"errors"
	"maps"
	"net/url"
	"slices"
	"strings"
)

// Invalid declarations for another camera must not disable unrelated streams.
func reuseScope(source string, sources map[string][]string) map[string][]string {
	owners := make(map[string]bool)
	for name, list := range sources {
		if slices.Contains(list, source) {
			owners[name] = true
		}
	}
	if u, err := url.Parse(source); err == nil && u.Query().Get("reuse") != "" {
		owners[u.Query().Get("reuse")] = true
	}
	scope := make(map[string][]string)
	for name, list := range sources {
		if owners[name] {
			scope[name] = list
		}
		for _, candidate := range list {
			if u, err := url.Parse(candidate); err == nil && u.Scheme == "xiaomi" && owners[u.Query().Get("reuse")] {
				scope[name] = list
			}
		}
	}
	return scope
}

type reuseSource struct {
	url    string
	stream string
}

// Plan before any producer is dialed. The primary Stream owns the device;
// the second URL consumes its other video through the existing media graph.
func planReuse(sources map[string][]string) (map[string]reuseSource, error) {
	plan := make(map[string]reuseSource)
	for name, list := range sources {
		for _, source := range list {
			if !strings.HasPrefix(source, "xiaomi:") {
				continue
			}
			u, err := url.Parse(source)
			if err != nil {
				return nil, errors.New("xiaomi: invalid source URL")
			}
			q, err := url.ParseQuery(u.RawQuery)
			if err != nil {
				return nil, errors.New("xiaomi: invalid source query")
			}
			if !q.Has("reuse") {
				continue
			}
			owner := q.Get("reuse")
			if len(q["reuse"]) != 1 || owner == "" || owner == name || len(q["channel"]) != 1 || q.Get("channel") != "2" {
				return nil, errors.New("xiaomi: reuse requires another stream and channel=2")
			}
			var primary string
			for _, candidate := range sources[owner] {
				if strings.HasPrefix(candidate, "xiaomi:") {
					if primary != "" {
						return nil, errors.New("xiaomi: reuse target has multiple Xiaomi sources")
					}
					primary = candidate
				}
			}
			p, err := url.Parse(primary)
			if err != nil || primary == "" {
				return nil, errors.New("xiaomi: reuse target needs a Xiaomi source")
			}
			pq, err := url.ParseQuery(p.RawQuery)
			if err != nil || pq.Has("reuse") || (pq.Get("channel") != "" && pq.Get("channel") != "0") {
				return nil, errors.New("xiaomi: reuse target must be a primary channel, without reuse")
			}
			if u.User == nil || p.User == nil || u.User.String() != p.User.String() || u.Host != p.Host || q.Get("did") == "" || q.Get("did") != pq.Get("did") {
				return nil, errors.New("xiaomi: reuse requires the same device, account and region")
			}
			if q.Get("model") != "xiaomi.camera.082ac1" || q.Get("model") != pq.Get("model") {
				return nil, errors.New("xiaomi: reuse is only verified for xiaomi.camera.082ac1")
			}
			for key, values := range q {
				if key != "channel" && key != "reuse" && !slices.Equal(values, pq[key]) {
					return nil, errors.New("xiaomi: reuse options must match the primary source")
				}
			}
			for other, candidates := range sources {
				if other != owner && slices.Contains(candidates, primary) {
					return nil, errors.New("xiaomi: reuse target source belongs to multiple streams")
				}
			}
			pq.Set("channel", "all")
			p.RawQuery = pq.Encode()
			plan[primary] = reuseSource{url: p.String()}
			plan[source] = reuseSource{stream: owner}
		}
	}
	return plan, nil
}

func resolveReuse(source string, planned map[string]reuseSource, sources map[string][]string) (reuseSource, error) {
	current, err := planReuse(sources)
	if err != nil {
		return reuseSource{}, err
	}
	// Do not upgrade an already running single-video producer or use a stale
	// owner after an API edit. Restart with the desired declarations instead.
	if current[source] != planned[source] || (planned[source] != (reuseSource{}) && !maps.Equal(current, planned)) {
		return reuseSource{}, errors.New("xiaomi: reuse declarations changed; restart to apply them")
	}
	if location := current[source]; location != (reuseSource{}) {
		return location, nil
	}
	u, err := url.Parse(source)
	if err != nil {
		return reuseSource{}, errors.New("xiaomi: invalid source URL")
	}
	if u.Query().Has("reuse") {
		return reuseSource{}, errors.New("xiaomi: reuse must be declared in streams before startup")
	}
	return reuseSource{url: source}, nil
}
