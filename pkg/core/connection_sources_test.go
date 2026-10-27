package core

import "testing"

func TestConnectionSenderSources(t *testing.T) {
	codec := &Codec{Name: CodecOpus, ClockRate: 48000}
	old, next := NewReceiver(nil, codec), NewReceiver(nil, codec)
	sender := NewSender(nil, codec)
	sender.WithParent(old)
	conn := &Connection{Senders: []*Sender{sender}}
	other := NewSender(nil, codec)
	other.WithParent(old)
	defer other.Close()
	old.Replace(next)
	sources := conn.GetSenderSources()
	if len(sources) != 1 || sources[0] != &next.Node {
		t.Fatal("connection did not follow its reparented sender")
	}
	sender.Close()
	if len(conn.GetSenderSources()) != 0 || len(next.Senders()) == 0 {
		t.Fatal("another connection's source was counted as owned")
	}
}
