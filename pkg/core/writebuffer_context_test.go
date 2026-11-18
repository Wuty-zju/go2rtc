package core

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestWriteBufferCancellation(t *testing.T) {
	for _, before := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		buffer := NewWriteBuffer(nil)
		var writer io.Writer = io.Discard
		ready := make(signalWriter)
		if before {
			cancel()
		} else {
			_, _ = buffer.Write([]byte("priming frame"))
			writer = ready
		}
		done := make(chan error, 1)
		go func() { _, err := buffer.WriteToContext(ctx, writer); done <- err }()
		if !before {
			select {
			case <-ready:
			case <-time.After(time.Second):
				t.Fatal("writer did not start")
			}
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("cancelled reader waits for a missing frame")
		}
		if _, err := buffer.Write([]byte("late frame")); !errors.Is(err, context.Canceled) {
			t.Fatal("late frame reached cancelled writer", err)
		}
	}
}

type signalWriter chan struct{}

func (w signalWriter) Write(p []byte) (int, error) {
	close(w)
	return len(p), nil
}
