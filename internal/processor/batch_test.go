package processor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hnlbs/collector/internal/model"
)

func TestBatchProcessor_FlushOnSize(t *testing.T) {
	var mu sync.Mutex
	var got []model.Span

	emit := func(_ context.Context, b model.Batch) error {
		mu.Lock()
		got = append(got, b.Spans...)
		mu.Unlock()
		return nil
	}

	p := NewBatch(3, 10*time.Second, emit)

	ctx := context.Background()
	for range 3 {
		p.Process(ctx, model.Batch{Spans: []model.Span{{Name: "s"}}})
	}

	mu.Lock()
	n := len(got)
	mu.Unlock()
	if n != 3 {
		t.Errorf("expected 3 spans flushed on size, got %d", n)
	}
}

func TestBatchProcessor_FlushOnTimeout(t *testing.T) {
	var mu sync.Mutex
	var got []model.Span

	emit := func(_ context.Context, b model.Batch) error {
		mu.Lock()
		got = append(got, b.Spans...)
		mu.Unlock()
		return nil
	}

	p := NewBatch(100, 20*time.Millisecond, emit)
	ctx := context.Background()
	p.Process(ctx, model.Batch{Spans: []model.Span{{Name: "s"}}})

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	n := len(got)
	mu.Unlock()
	if n != 1 {
		t.Errorf("expected 1 span flushed by timeout, got %d", n)
	}
}

func TestBatchProcessor_FlushOnClose(t *testing.T) {
	var mu sync.Mutex
	var got []model.Span

	emit := func(_ context.Context, b model.Batch) error {
		mu.Lock()
		got = append(got, b.Spans...)
		mu.Unlock()
		return nil
	}

	p := NewBatch(100, time.Minute, emit)
	ctx := context.Background()
	p.Process(ctx, model.Batch{Spans: []model.Span{{Name: "a"}, {Name: "b"}}})

	mu.Lock()
	n := len(got)
	mu.Unlock()
	if n != 0 {
		t.Error("should not flush before close")
	}

	p.Close()
	mu.Lock()
	n = len(got)
	mu.Unlock()
	if n != 2 {
		t.Errorf("expected 2 spans flushed on Close, got %d", n)
	}
}

func TestBatchProcessor_ProcessReturnsEmpty(t *testing.T) {
	emit := func(_ context.Context, b model.Batch) error { return nil }
	p := NewBatch(10, time.Second, emit)
	got, err := p.Process(context.Background(), model.Batch{Spans: []model.Span{{Name: "x"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Spans) != 0 {
		t.Errorf("Process should return empty batch, got %d spans", len(got.Spans))
	}
}
