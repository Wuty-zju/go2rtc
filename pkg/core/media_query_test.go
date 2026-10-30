package core

import "testing"

func TestMediaQueryOrder(t *testing.T) {
	query := map[string][]string{
		"audio": {"opus", "pcma"}, "video": {"h264", "h265"}, "ignored": {"value"},
	}
	for range 200 {
		medias := ParseQuery(query)
		if len(medias) != 4 {
			t.Fatal("lost media alternatives")
		}
		for i, codec := range []string{CodecH264, CodecH265, CodecOpus, CodecPCMA} {
			kind := KindVideo
			if i >= 2 {
				kind = KindAudio
			}
			if medias[i].Kind != kind || medias[i].Codecs[0].Name != codec {
				t.Fatal("identical query changed media order and RTSP control identity", i, medias[i])
			}
		}
	}
}
