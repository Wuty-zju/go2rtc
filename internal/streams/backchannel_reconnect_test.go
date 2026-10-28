package streams

import (
	"github.com/AlexxIT/go2rtc/pkg/core"
	"testing"
)

func TestReleasedBackchannelOwnership(t *testing.T) {
	codec := &core.Codec{Name: core.CodecOpus, ClockRate: 48000}
	stale, active := core.NewReceiver(nil, codec), core.NewReceiver(nil, codec)
	other := core.NewSender(nil, codec)
	other.WithParent(stale)
	defer other.Close()
	conn := newReconnectProducer()
	sender := core.NewSender(nil, codec)
	sender.WithParent(active)
	conn.Senders = []*core.Sender{sender}
	producer := NewProducer("test:owned-speaker")
	producer.conn, producer.state = conn, stateMedias
	producer.senders = []*core.Receiver{stale, active}
	defer producer.stop()
	sources := producer.activeSenders()
	if len(sources) != 1 || sources[0] != active {
		t.Fatal("another destination was counted as this connection's speaker")
	}
	sender.Close()
	stream := &Stream{producers: []*Producer{producer}}
	stream.stopProducers()
	if conn.stopped.Load() != 1 {
		t.Fatal("released speaker kept device alive")
	}
}
