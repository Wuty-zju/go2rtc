package streams

import (
	"net/url"
	"slices"
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

func TestSourceMediaSelectionQueryContract(t *testing.T) {
	for _, test := range []struct {
		query string
		want  []string
	}{
		{"video", []string{"front"}},
		{"video=all", []string{"front", "rear-Lens"}},
		{"video=h265@rear-Lens", []string{"rear-Lens"}},
		{"video=@rear-Lens", []string{"rear-Lens"}},
		{"video=h264,h265@rear-Lens", []string{"rear-Lens"}},
		{"video=copy@unknown", nil},
		{"video=h264@rear-Lens", nil},
		{"video=h265@", nil},
	} {
		t.Run(test.query, func(t *testing.T) {
			query, err := url.ParseQuery(test.query)
			if err != nil {
				t.Fatal(err)
			}
			media := core.ParseQuery(query)[0]
			media.ID = "consumer-control" // Transport ID must not replace source identity.
			var got []string
			for _, id := range []string{"front", "rear-Lens"} {
				source := &core.Media{ID: id, Kind: core.KindVideo, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{{Name: core.CodecH265, ClockRate: 90000}}}
				if codec, _ := source.MatchMedia(media); codec != nil {
					got = append(got, id)
					if !media.MatchAll() {
						break
					}
				}
			}
			if !slices.Equal(got, test.want) {
				t.Fatalf("got %v, want %v", got, test.want)
			}
		})
	}
}
