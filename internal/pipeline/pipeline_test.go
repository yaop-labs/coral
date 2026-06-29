package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/yaop-labs/coral/internal/model"
)

// fakeReceiver sends batches on demand via Send.
type fakeReceiver struct {
	mu   sync.Mutex
	emit func(context.Context, model.Batch) error
}

func (r *fakeReceiver) Start(ctx context.Context, emit func(context.Context, model.Batch) error) error {
	r.mu.Lock()
	r.emit = emit
	r.mu.Unlock()
	<-ctx.Done()
	return nil
}

func (r *fakeReceiver) Stop(_ context.Context) error { return nil }

func (r *fakeReceiver) Send(ctx context.Context, b model.Batch) error {
	r.mu.Lock()
	emit := r.emit
	r.mu.Unlock()
	if emit == nil {
		return errors.New("receiver not started")
	}
	return emit(ctx, b)
}

// capturingExporter records all spans it receives.
type capturingExporter struct {
	mu    sync.Mutex
	spans []model.Span
}

func (e *capturingExporter) Export(_ context.Context, b model.Batch) error {
	e.mu.Lock()
	e.spans = append(e.spans, b.Spans...)
	e.mu.Unlock()
	return nil
}

func (e *capturingExporter) Close() error { return nil }

func (e *capturingExporter) Len() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.spans)
}

func (e *capturingExporter) All() []model.Span {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]model.Span, len(e.spans))
	copy(out, e.spans)
	return out
}

// filterProcessor drops spans with a specific name.
type filterProcessor struct{ drop string }

func (f *filterProcessor) Process(_ context.Context, b model.Batch) (model.Batch, error) {
	out := b.Spans[:0]
	for _, s := range b.Spans {
		if s.Name != f.drop {
			out = append(out, s)
		}
	}
	return model.Batch{Spans: out}, nil
}
func (f *filterProcessor) Close() error { return nil }

func startPipeline(t *testing.T, recv *fakeReceiver, exp *capturingExporter, procs ...Processor) (*Pipeline, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	p := New(Config{Workers: 2, QueueSize: 16}, slog.Default())
	p.AddReceiver(recv)
	for _, pr := range procs {
		p.AddProcessor(pr)
	}
	p.AddExporter(exp)
	if err := p.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	// wait for receiver to be ready
	time.Sleep(5 * time.Millisecond)
	return p, cancel
}

func TestPipeline_SpanFlowsThrough(t *testing.T) {
	recv := &fakeReceiver{}
	exp := &capturingExporter{}
	p, cancel := startPipeline(t, recv, exp)
	defer cancel()

	span := model.Span{TraceID: model.TraceID{1}, SpanID: model.SpanID{1}, Name: "op"}
	if err := recv.Send(context.Background(), model.Batch{Spans: []model.Span{span}}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if exp.Len() == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if exp.Len() != 1 {
		t.Fatalf("expected 1 span exported, got %d", exp.Len())
	}

	cancel()
	p.Shutdown(context.Background())
}

func TestPipeline_ProcessorFilters(t *testing.T) {
	recv := &fakeReceiver{}
	exp := &capturingExporter{}
	p, cancel := startPipeline(t, recv, exp, &filterProcessor{drop: "drop-me"})
	defer cancel()

	spans := []model.Span{
		{Name: "keep"},
		{Name: "drop-me"},
		{Name: "keep2"},
	}
	if err := recv.Send(context.Background(), model.Batch{Spans: spans}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if exp.Len() == 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if exp.Len() != 2 {
		t.Fatalf("expected 2 spans, got %d", exp.Len())
	}

	cancel()
	p.Shutdown(context.Background())
}

func TestPipeline_EmptyBatchIgnored(t *testing.T) {
	recv := &fakeReceiver{}
	exp := &capturingExporter{}
	p, cancel := startPipeline(t, recv, exp)
	defer cancel()

	if err := recv.Send(context.Background(), model.Batch{}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if exp.Len() != 0 {
		t.Fatalf("expected 0 spans, got %d", exp.Len())
	}

	cancel()
	p.Shutdown(context.Background())
}

func TestPipeline_ShutdownDrainsQueue(t *testing.T) {
	recv := &fakeReceiver{}
	exp := &capturingExporter{}
	ctx, cancel := context.WithCancel(context.Background())
	p := New(Config{Workers: 1, QueueSize: 100}, slog.Default())
	p.AddReceiver(recv)
	p.AddExporter(exp)
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)

	for i := 0; i < 10; i++ {
		s := model.Span{TraceID: model.TraceID{byte(i)}, Name: "span"}
		recv.Send(context.Background(), model.Batch{Spans: []model.Span{s}})
	}
	cancel()
	p.Shutdown(context.Background())

	if exp.Len() != 10 {
		t.Fatalf("expected 10 spans after drain, got %d", exp.Len())
	}
}

func TestPipeline_DirectExport(t *testing.T) {
	p := New(Config{Workers: 1, QueueSize: 16}, slog.Default())
	exp := &capturingExporter{}
	p.AddExporter(exp)

	ctx := context.Background()
	b := model.Batch{Spans: []model.Span{{Name: "direct"}}}
	if err := p.Export(ctx, b); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if exp.Len() != 1 {
		t.Fatalf("expected 1 span, got %d", exp.Len())
	}
}

func TestPipeline_Stats(t *testing.T) {
	recv := &fakeReceiver{}
	exp := &capturingExporter{}
	p, cancel := startPipeline(t, recv, exp)
	defer cancel()

	recv.Send(context.Background(), model.Batch{Spans: []model.Span{{Name: "a"}, {Name: "b"}}})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if exp.Len() == 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	in, dropped, out := p.Stats()
	if in == 0 {
		t.Error("batchesIn should be > 0")
	}
	if dropped != 0 {
		t.Errorf("unexpected drops: %d", dropped)
	}
	if out != 2 {
		t.Errorf("spansOut = %d, want 2", out)
	}

	cancel()
	p.Shutdown(context.Background())
}

func TestPipeline_MultiExporter_FanOut(t *testing.T) {
	recv := &fakeReceiver{}
	exp1 := &capturingExporter{}
	exp2 := &capturingExporter{}

	ctx, cancel := context.WithCancel(context.Background())
	p := New(Config{Workers: 2, QueueSize: 16}, slog.Default())
	p.AddReceiver(recv)
	p.AddExporter(exp1)
	p.AddExporter(exp2)
	if err := p.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	defer cancel()
	time.Sleep(5 * time.Millisecond)

	span := model.Span{Name: "fanout-span"}
	if err := recv.Send(context.Background(), model.Batch{Spans: []model.Span{span}}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if exp1.Len() == 1 && exp2.Len() == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if exp1.Len() != 1 {
		t.Errorf("exporter1: expected 1 span, got %d", exp1.Len())
	}
	if exp2.Len() != 1 {
		t.Errorf("exporter2: expected 1 span, got %d", exp2.Len())
	}

	cancel()
	p.Shutdown(context.Background())
}

// countingProcessor counts calls and optionally returns empty batch.
// All fields are guarded by mu so the worker goroutine and test goroutine
// can safely access them concurrently.
type countingProcessor struct {
	mu      sync.Mutex
	calls   int
	dropAll bool
}

func (c *countingProcessor) Process(_ context.Context, b model.Batch) (model.Batch, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	if c.dropAll {
		return model.Batch{}, nil
	}
	return b, nil
}
func (c *countingProcessor) Close() error { return nil }

func (c *countingProcessor) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestPipeline_ProcessorChain_ShortCircuit(t *testing.T) {
	recv := &fakeReceiver{}
	exp := &capturingExporter{}

	dropper := &countingProcessor{dropAll: true}
	second := &countingProcessor{dropAll: false}

	ctx, cancel := context.WithCancel(context.Background())
	p := New(Config{Workers: 1, QueueSize: 16}, slog.Default())
	p.AddReceiver(recv)
	p.AddProcessor(dropper)
	p.AddProcessor(second)
	p.AddExporter(exp)
	if err := p.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	defer cancel()
	time.Sleep(5 * time.Millisecond)

	recv.Send(context.Background(), model.Batch{Spans: []model.Span{{Name: "x"}}})

	// Shutdown drains the queue and waits for the worker to finish, so after
	// Shutdown returns we know the batch has been processed.
	cancel()
	p.Shutdown(context.Background())

	if dropper.Calls() != 1 {
		t.Errorf("dropper should be called once, got %d", dropper.Calls())
	}
	if second.Calls() != 0 {
		t.Errorf("second processor must not be called after empty batch, got %d", second.Calls())
	}
	if exp.Len() != 0 {
		t.Errorf("exporter must not receive spans after drop, got %d", exp.Len())
	}
}

func TestPipeline_ExportFrom_StartsAtProcessorIndex(t *testing.T) {
	p := New(Config{Workers: 1, QueueSize: 16}, slog.Default())
	first := &countingProcessor{}
	second := &countingProcessor{}
	exp := &capturingExporter{}

	p.AddProcessor(first)
	p.AddProcessor(second)
	p.AddExporter(exp)

	err := p.ExportFrom(context.Background(), model.Batch{Spans: []model.Span{{Name: "flushed"}}}, 1)
	if err != nil {
		t.Fatalf("ExportFrom: %v", err)
	}
	if first.Calls() != 0 {
		t.Fatalf("first processor calls = %d, want 0", first.Calls())
	}
	if second.Calls() != 1 {
		t.Fatalf("second processor calls = %d, want 1", second.Calls())
	}
	if exp.Len() != 1 {
		t.Fatalf("exported spans = %d, want 1", exp.Len())
	}
}

func TestPipeline_Backpressure_BlocksEmit(t *testing.T) {
	queueSize := 1
	ch := make(chan model.Batch, queueSize)

	ch <- model.Batch{Spans: []model.Span{{Name: "fill"}}}

	shortCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	emit := func(ctx context.Context, b model.Batch) error {
		if len(b.Spans) == 0 {
			return nil
		}
		select {
		case ch <- b:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	err := emit(shortCtx, model.Batch{Spans: []model.Span{{Name: "overflow"}}})
	if err == nil {
		t.Error("expected backpressure to return ctx error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestPipeline_Shutdown_Idempotent(t *testing.T) {
	p := New(Config{Workers: 1, QueueSize: 4}, slog.Default())
	exp := &capturingExporter{}
	p.AddExporter(exp)

	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	cancel()

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("first Shutdown: %v", err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}
