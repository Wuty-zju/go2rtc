package core

import (
	"testing"
	"time"
)

func TestClosingSourceStopsSender(t *testing.T) {
	codec := &Codec{Name: CodecOpus, ClockRate: 48000}
	source := NewReceiver(nil, codec)
	sender := NewSender(nil, codec)
	sender.Handler = func(*Packet) {}
	sender.HandleRTP(source)
	defer func() { sender.Close(); sender.Wait() }()
	source.Close()
	if sender.State() != "closed" {
		t.Fatal("detached sender keeps its queue and worker alive")
	}
	sender.Wait()
}

func TestMovingSourcePreservesSenderUntilReplacementCloses(t *testing.T) {
	codec := &Codec{Name: CodecOpus, ClockRate: 48000}
	old, next := NewReceiver(nil, codec), NewReceiver(nil, codec)
	sender := NewSender(nil, codec)
	received := make(chan struct{}, 1)
	sender.Handler = func(*Packet) { received <- struct{}{} }
	sender.HandleRTP(old)
	defer func() { sender.Close(); sender.Wait() }()
	old.Replace(next)
	old.Close()
	next.Input(&Packet{Payload: []byte{1}})
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("closing the replaced source killed its moved subscriber")
	}
	next.Close()
	if sender.State() != "closed" {
		t.Fatal("replacement source left its sender running")
	}
	sender.Wait()
}
