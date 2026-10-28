package core

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReceiverCallbackCanDetach(t *testing.T) {
	receiver := NewReceiver(nil, &Codec{})
	var calls [3]int
	for i := range calls {
		child := &Node{}
		child.Input = func(*Packet) {
			calls[i]++
			if i == 0 {
				child.Close()
			}
		}
		child.WithParent(&receiver.Node)
	}
	done := make(chan struct{})
	go func() { receiver.Input(&Packet{}); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fanout held the graph lock during a callback")
	}
	if calls != [3]int{1, 1, 1} {
		t.Fatalf("detaching one child skipped another: %v", calls)
	}
	receiver.Close()
}

func TestReceiverCloseDetachesEveryChild(t *testing.T) {
	receiver := NewReceiver(nil, &Codec{})
	for range 3 {
		(&Node{}).WithParent(&receiver.Node)
	}
	receiver.Close()
	if len(receiver.Senders()) != 0 {
		t.Fatal("closing a receiver left a child attached")
	}
}

func TestReceiverConcurrentFanout(t *testing.T) {
	receiver := NewReceiver(nil, &Codec{})
	var delivered atomic.Int64
	sink := func() *Sender {
		sender := NewSender(nil, &Codec{})
		sender.Handler = func(*Packet) { delivered.Add(1) }
		sender.WithParent(receiver)
		sender.Start()
		return sender
	}
	first := sink()
	defer func() { first.Close(); first.Wait() }()
	done := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(100 * time.Microsecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				receiver.Input(&Packet{})
			}
		}
	}()
	for range 100 {
		sender := sink()
		time.Sleep(time.Millisecond)
		sender.Close()
		sender.Wait()
	}
	close(done)
	workers.Wait()
	if delivered.Load() == 0 {
		t.Fatal("no packet reached a real sender")
	}
}

func TestMoveNodeRetainsExistingTargetConsumers(t *testing.T) {
	source, target := NewReceiver(nil, &Codec{}), NewReceiver(nil, &Codec{})
	var sourcePackets, targetPackets int
	(&Node{Input: func(*Packet) { sourcePackets++ }}).WithParent(&source.Node)
	(&Node{Input: func(*Packet) { targetPackets++ }}).WithParent(&target.Node)
	source.Replace(target)
	target.Input(&Packet{})
	if sourcePackets != 1 || targetPackets != 1 {
		t.Fatalf("moving consumers discarded an existing target subscriber: %d/%d", sourcePackets, targetPackets)
	}
	source.Close()
	target.Close()
}

func TestMoveNodeConcurrentConsumerClose(t *testing.T) {
	for iteration := range 1000 {
		source, target := NewReceiver(nil, &Codec{}), NewReceiver(nil, &Codec{})
		sender := NewSender(nil, &Codec{})
		sender.WithParent(source)
		start := make(chan struct{})
		var operations sync.WaitGroup
		operations.Add(2)
		go func() { defer operations.Done(); <-start; source.Replace(target) }()
		go func() { defer operations.Done(); <-start; sender.Close() }()
		close(start)
		operations.Wait()
		if len(source.Senders()) != 0 || len(target.Senders()) != 0 {
			t.Fatalf("closed consumer reattached during reparent at iteration %d", iteration)
		}
	}
}

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
