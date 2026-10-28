package miss

import (
	"fmt"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/opus"
	"github.com/AlexxIT/go2rtc/pkg/pcm"
	"github.com/pion/rtp"
)

func (p *Producer) AddTrack(media *core.Media, _ *core.Codec, track *core.Receiver) error {
	p.speakerMu.Lock()
	defer p.speakerMu.Unlock()
	if p.stopped.Load() {
		return fmt.Errorf("miss: producer is stopped")
	}
	if track.Codec.Name != core.CodecPCMA && track.Codec.Name != core.CodecOpus {
		return fmt.Errorf("miss: unsupported speaker codec %s", track.Codec.Name)
	}
	if !p.dual {
		if err := p.client.StartSpeaker(); err != nil {
			return err
		}
		time.Sleep(time.Second)
	}

	sender := core.NewSender(media, track.Codec)

	var buf []byte
	write := func(codec uint32, payload []byte) {
		if err := p.client.WriteAudio(codec, payload); err != nil {
			if p.dual {
				_ = p.client.Conn.Close() // Wake the media reader and reconnect.
			}
			return
		}
		p.Send += len(payload)
	}
	switch track.Codec.Name {
	case core.CodecPCMA:
		if p.client.SpeakerCodec() == codecPCM {
			dst := &core.Codec{Name: core.CodecPCML, ClockRate: 8000}
			transcode := pcm.Transcode(dst, track.Codec)

			sender.Handler = func(pkt *rtp.Packet) {
				buf = append(buf, transcode(pkt.Payload)...)
				const size = 2 * 8000 * 0.040 // 16bit 40ms
				for len(buf) >= size {
					write(codecPCM, buf[:size])
					buf = buf[size:]
				}
			}
		} else {
			sender.Handler = func(pkt *rtp.Packet) {
				buf = append(buf, pkt.Payload...)
				const size = 8000 * 0.040 // 8bit 40 ms
				for len(buf) >= size {
					write(codecPCMA, buf[:size])
					buf = buf[size:]
				}
			}
		}
	case core.CodecOpus:
		if p.client.SpeakerCodec() == codecOPUS {
			sender.Handler = func(pkt *rtp.Packet) {
				if buf == nil {
					buf = pkt.Payload
				} else {
					// convert two 20ms to one 40ms
					buf = opus.JoinFrames(buf, pkt.Payload)
					write(codecOPUS, buf)
					buf = nil
				}
			}
		} else {
			sender.Handler = func(pkt *rtp.Packet) {
				write(codecOPUS, pkt.Payload)
			}
		}
	}

	if p.dual {
		handler := sender.Handler
		sender.Handler = func(packet *rtp.Packet) {
			p.speakerMu.Lock()
			defer p.speakerMu.Unlock()
			if p.stopped.Load() || len(packet.Payload) == 0 {
				return
			}
			idle := time.Since(p.speakerTime) >= time.Second
			if p.speaker != sender && !idle {
				return
			}
			if p.speaker != sender || idle {
				// A preload is not a talk session. Claim the shared device only
				// on audio, and discard partial frames when ownership changes.
				buf = nil
				if err := p.client.StartSpeaker(); err != nil {
					_ = p.client.Conn.Close()
					return
				}
				time.Sleep(time.Second) // Same device startup delay as single-lens mode.
				p.speaker = sender
			}
			p.speakerTime = time.Now()
			handler(packet)
		}
	}
	sender.HandleRTP(track)
	p.Senders = append(p.Senders, sender)
	return nil
}
