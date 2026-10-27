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
