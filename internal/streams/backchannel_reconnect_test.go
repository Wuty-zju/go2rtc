package streams

import (
	"fmt"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

type ownedBackchannel struct {
	*reconnectProducer
	inputs   []*core.Receiver
	received chan uint16
}

func newOwnedBackchannel() *ownedBackchannel {
	media := &core.Media{Kind: core.KindAudio, Direction: core.DirectionSendonly, Codecs: []*core.Codec{{Name: core.CodecOpus, ClockRate: 48000, Channels: 2}}}
	return &ownedBackchannel{reconnectProducer: newReconnectProducer(media), received: make(chan uint16, 8)}
}
func (p *ownedBackchannel) AddTrack(media *core.Media, codec *core.Codec, input *core.Receiver) error {
	if len(p.inputs) > 0 {
		return fmt.Errorf("test: speaker already owned")
	}
	sender := core.NewSender(media, codec)
	sender.Handler = func(packet *core.Packet) { p.received <- packet.SequenceNumber }
	sender.WithParent(input)
	sender.Start()
	p.Senders = append(p.Senders, sender)
	p.inputs = append(p.inputs, input)
	return nil
}
func (p *ownedBackchannel) GetSenderSources() []*core.Node {
	var nodes []*core.Node
	for _, input := range p.inputs {
		nodes = append(nodes, &input.Node)
	}
	return nodes
}
func (p *ownedBackchannel) release() {
	for _, sender := range p.Senders {
		sender.Close()
		sender.Wait()
	}
	p.Senders = nil
	p.inputs = nil
}

func TestReconnectOnlyRestoresOwnedBackchannel(t *testing.T) {
	old, next := newOwnedBackchannel(), newOwnedBackchannel()
	const scheme = "test-owned-backchannel"
	HandleFunc(scheme, func(string) (core.Producer, error) { return next, nil })
	defer delete(handlers, scheme)
	producer := NewProducer(scheme + ":speaker")
	producer.conn, producer.state = old, stateMedias
	t.Cleanup(func() { producer.stop(); old.release(); next.release() })
	media := old.Medias[0]
	stale := core.NewReceiver(nil, media.Codecs[0])
	active := core.NewReceiver(nil, media.Codecs[0])
	if err := producer.AddTrack(media, media.Codecs[0], stale); err != nil {
		t.Fatal(err)
	}
	old.release()
	// Another destination still consumes the old source; global child counts
	// cannot establish ownership by this camera connection.
	other := core.NewSender(nil, stale.Codec)
	other.WithParent(stale)
	defer other.Close()
	if err := producer.AddTrack(media, media.Codecs[0], active); err != nil {
		t.Fatal(err)
	}
	producer.start()
	awaitReconnectStart(t, old.reconnectProducer)
	old.fail()
	awaitReconnectStart(t, next.reconnectProducer)
	packet := &core.Packet{}
	packet.SequenceNumber = 42
	active.Input(packet)
	select {
	case sequence := <-next.received:
		if sequence != 42 {
			t.Fatal("wrong source resumed")
		}
	case <-time.After(time.Second):
		t.Fatal("stale source captured the replacement speaker")
	}
	if len(producer.senders) != 1 {
		t.Fatal("stale sender source was retained")
	}
}

func TestReleasedBackchannelDoesNotKeepDeviceAlive(t *testing.T) {
	conn := newOwnedBackchannel()
	producer := NewProducer("test-owned-backchannel:idle")
	producer.conn, producer.state = conn, stateMedias
	media := conn.Medias[0]
	source := core.NewReceiver(nil, media.Codecs[0])
	if err := producer.AddTrack(media, media.Codecs[0], source); err != nil {
		t.Fatal(err)
	}
	conn.release()
	other := core.NewSender(nil, source.Codec)
	other.WithParent(source)
	defer other.Close()
	stream := &Stream{producers: []*Producer{producer}}
	stream.stopProducers()
	if conn.stopped.Load() != 1 {
		producer.stop()
		t.Fatal("another destination's subscriber kept this camera open")
	}
}
