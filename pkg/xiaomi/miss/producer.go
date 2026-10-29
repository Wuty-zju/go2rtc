package miss

import (
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264"
	"github.com/AlexxIT/go2rtc/pkg/h264/annexb"
	"github.com/AlexxIT/go2rtc/pkg/h265"
	"github.com/AlexxIT/go2rtc/pkg/opus"
	"github.com/pion/rtp"
)

type Producer struct {
	core.Connection
	client      *Client
	dual        bool
	opus20      bool
	speakerMu   sync.Mutex
	speaker     *core.Sender
	stopped     atomic.Bool
	speakerTime time.Time
}

func Dial(rawURL string) (core.Producer, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	query := u.Query()
	if err = ValidatePtime(query); err != nil {
		return nil, err
	}
	dual := query.Get("channel") == "all"
	if dual && query.Get("model") != "xiaomi.camera.082ac1" {
		return nil, fmt.Errorf("miss: dual-lens streaming is not verified for %s", query.Get("model"))
	}

	client, err := NewClient(rawURL)
	if err != nil {
		return nil, err
	}

	err = client.StartMedia(query.Get("channel"), query.Get("subtype"), query.Get("audio"))
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	videoCount := 1
	if dual {
		videoCount = 2
	}
	medias, err := probe(client, query.Get("audio") != "0", videoCount, query.Has("ptime"))
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	producer := &Producer{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "xiaomi/miss",
			Protocol:   client.Protocol(),
			RemoteAddr: client.RemoteAddr().String(),
			UserAgent:  client.Version(),
			Medias:     medias,
			Transport:  client,
		},
		client: client,
		dual:   dual,
		opus20: query.Has("ptime"),
	}
	if dual {
		// Consumers can select another lens after the packet reader has started.
		// Prepare all receiver identities before that reader accesses the list.
		for _, media := range medias {
			if media.Direction == core.DirectionRecvonly {
				_, _ = producer.Connection.GetTrack(media, media.Codecs[0])
			}
		}
	}
	return producer, nil
}

// ValidatePtime is also used before cloud discovery, so invalid options never
// cause an authentication request or a physical connection attempt.
func ValidatePtime(query url.Values) error {
	if query.Has("ptime") && (len(query["ptime"]) != 1 || query.Get("ptime") != "20" || query.Get("audio") == "0") {
		return fmt.Errorf("xiaomi: ptime requires ptime=20 and enabled Opus audio")
	}
	return nil
}

func probe(client *Client, audio bool, videoCount int, opus20 bool) ([]*core.Media, error) {
	_ = client.SetDeadline(time.Now().Add(15 * time.Second))

	vcodecs := make([]*core.Codec, videoCount)
	var acodec *core.Codec
	var videos int

	for {
		pkt, err := client.ReadPacket()
		if err != nil {
			if videos == videoCount {
				err = fmt.Errorf("no audio")
			} else if videos != 0 {
				err = fmt.Errorf("missing video channel")
			} else if acodec != nil {
				err = fmt.Errorf("no video")
			}
			return nil, fmt.Errorf("xiaomi: probe: %w", err)
		}

		channel := 0
		if videoCount > 1 {
			channel = int(pkt.videoChannel())
		}
		switch pkt.CodecID {
		case codecH264:
			if vcodecs[channel] == nil {
				buf := annexb.EncodeToAVCC(pkt.Payload)
				if h264.NALUType(buf) == h264.NALUTypeSPS {
					vcodecs[channel] = h264.AVCCToCodec(buf)
					videos++
				}
			}
		case codecH265:
			if vcodecs[channel] == nil {
				buf := annexb.EncodeToAVCC(pkt.Payload)
				if h265.NALUType(buf) == h265.NALUTypeVPS {
					vcodecs[channel] = h265.AVCCToCodec(buf)
					videos++
				}
			}
		case codecPCMA:
			if opus20 {
				return nil, fmt.Errorf("xiaomi: ptime requires Opus audio")
			}
			if acodec == nil {
				acodec = &core.Codec{Name: core.CodecPCMA, ClockRate: pkt.SampleRate()}
			}
		case codecOPUS:
			if opus20 {
				if _, err := opus.Split20(pkt.Payload); err != nil {
					return nil, fmt.Errorf("xiaomi: ptime probe: %w", err)
				}
			}
			if acodec == nil {
				acodec = &core.Codec{Name: core.CodecOpus, ClockRate: 48000, Channels: 2}
			}
		}

		if videos == videoCount && (acodec != nil || !audio) {
			break
		}
	}

	_ = client.SetDeadline(time.Time{})

	medias := make([]*core.Media, 0, videoCount+2)
	for channel, codec := range vcodecs {
		media := &core.Media{
			Kind:      core.KindVideo,
			Direction: core.DirectionRecvonly,
			Codecs:    []*core.Codec{codec},
		}
		if videoCount > 1 {
			media.ID = strconv.Itoa(channel)
		}
		medias = append(medias, media)
	}

	if acodec != nil {
		medias = append(medias, &core.Media{
			Kind:      core.KindAudio,
			Direction: core.DirectionRecvonly,
			Codecs:    []*core.Codec{acodec},
		})

		medias = append(medias, &core.Media{
			Kind:      core.KindAudio,
			Direction: core.DirectionSendonly,
			Codecs:    []*core.Codec{acodec.Clone()},
		})
	}

	return medias, nil
}

const timestamp40ms = 48000 * 0.040

func (p *Producer) Start() error {
	var audioTS uint32
	var audioSeq uint16
	opusSamples := uint32(timestamp40ms)
	var sourceSeq uint32
	var haveSourceSeq bool
	if p.dual {
		// A replacement producer feeds the existing consumers. Give its audio
		// clock a local time origin instead of restarting their timeline at zero.
		for _, media := range p.Medias {
			if media.Kind == core.KindAudio && media.Direction == core.DirectionRecvonly {
				audioTS = TimeToRTP(uint64(time.Now().UnixMilli()), uint64(media.Codecs[0].ClockRate))
				break
			}
		}
	}
	var ready [2]bool

	for {
		_ = p.client.SetDeadline(time.Now().Add(10 * time.Second))
		pkt, err := p.client.ReadPacket()
		if err != nil {
			return err
		}

		p.Recv += len(pkt.Payload)

		// TODO: rewrite this
		var name string
		var pkt2 *core.Packet
		var audioFrames [][]byte
		var videoCodec *core.Codec

		switch pkt.CodecID {
		case codecH264, codecH265:
			payload := annexb.EncodeToAVCC(pkt.Payload)
			if p.dual {
				channel := pkt.videoChannel()
				videoCodec = p.Medias[channel].Codecs[0]
				if !ready[channel] {
					if pkt.CodecID == codecH264 {
						ready[channel] = h264.IsKeyframe(payload)
					} else {
						ready[channel] = h265.IsKeyframe(payload)
					}
					if !ready[channel] {
						continue
					}
				}
			}
			pkt2 = &core.Packet{
				Header: rtp.Header{
					SequenceNumber: uint16(pkt.Sequence),
					Timestamp:      TimeToRTP(pkt.Timestamp, 90000),
				},
				Payload: payload,
			}
			if pkt.CodecID == codecH264 {
				name = core.CodecH264
			} else {
				name = core.CodecH265
			}
		case codecPCMA:
			name = core.CodecPCMA
			pkt2 = &core.Packet{
				Header: rtp.Header{
					Version:        2,
					Marker:         true,
					SequenceNumber: uint16(pkt.Sequence),
					Timestamp:      audioTS,
				},
				Payload: pkt.Payload,
			}
			audioTS += uint32(len(pkt.Payload))
		case codecOPUS:
			if p.opus20 {
				// Preserve a missing interval in an ordered source instead of hiding
				// packet loss behind the new output sequence. Its duration is estimated
				// from the previous valid packet; transport owns duplicate filtering.
				if !haveSourceSeq {
					sourceSeq = pkt.Sequence
					haveSourceSeq = true
				} else if delta := pkt.Sequence - sourceSeq; delta > 0 && delta < 1<<31 {
					missing := delta - 1
					audioTS += missing * opusSamples
					audioSeq += uint16(missing * (opusSamples / 960))
					sourceSeq = pkt.Sequence
				}
			}
			name = core.CodecOpus
			pkt2 = &core.Packet{
				Header: rtp.Header{
					Version:        2,
					Marker:         true,
					SequenceNumber: uint16(pkt.Sequence),
					Timestamp:      audioTS,
				},
				Payload: pkt.Payload,
			}
			if p.opus20 {
				audioFrames, err = opus.Split20(pkt.Payload)
				if err != nil {
					// A damaged or changed packet must not trigger a reconnect and
					// another cloud login. Estimate its gap from the last valid packet.
					audioTS += opusSamples
					audioSeq += uint16(opusSamples / 960)
					continue
				}
				pkt2.SequenceNumber = audioSeq
				audioSeq += uint16(len(audioFrames))
				opusSamples = uint32(len(audioFrames)) * 960
				audioTS += opusSamples
			} else {
				// Known cameras send 40ms packets; preserve default behavior.
				audioTS += timestamp40ms
			}
		}

		for _, recv := range p.Receivers {
			if recv.Codec.Name == name && (videoCodec == nil || recv.Codec == videoCodec) {
				if audioFrames == nil {
					recv.WriteRTP(pkt2)
				} else {
					for i, payload := range audioFrames {
						clone := *pkt2
						clone.Payload = payload
						clone.Timestamp += uint32(i) * 960
						clone.SequenceNumber += uint16(i)
						recv.WriteRTP(&clone)
					}
				}
				break
			}
		}
	}
}

func (p *Producer) Stop() error {
	if p.dual {
		if p.stopped.Swap(true) {
			return nil
		}
		_ = p.client.StopMedia()
		// Close transport before joining writers: an audio write may be blocked.
		err := p.client.Close()
		p.speakerMu.Lock()
		_ = p.Connection.Stop()
		senders := p.Senders
		p.speakerMu.Unlock()
		for _, sender := range senders {
			sender.Wait()
		}
		return err
	}
	_ = p.client.StopMedia()
	return p.Connection.Stop()
}

// TimeToRTP convert time in milliseconds to RTP time
func TimeToRTP(timeMS, clockRate uint64) uint32 {
	return uint32(timeMS * clockRate / 1000)
}
