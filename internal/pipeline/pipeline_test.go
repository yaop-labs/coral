package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yaop-labs/coral/internal/delivery"
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

type gateFailingExporter struct{ err error }

func (e gateFailingExporter) Export(context.Context, model.Batch) error { return e.err }
func (e gateFailingExporter) Close() error                              { return nil }

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

func startPipeline(t *testing.T, recv *fakeReceiver, exp *capturingExporter, procs ...Processor[model.Batch]) (*Pipeline[model.Batch], context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	p := New[model.Batch](Config{Workers: 2, QueueSize: 16}, slog.Default())
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
		if p.DeliveryStats().ItemsDelivered == 2 {
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
	p := New[model.Batch](Config{Workers: 1, QueueSize: 100}, slog.Default())
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
	p := New[model.Batch](Config{Workers: 1, QueueSize: 16}, slog.Default())
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

func TestPipeline_DurableCompletionRequiresEveryRequiredExporter(t *testing.T) {
	p := New[model.Batch](Config{Workers: 1, QueueSize: 16}, slog.Default())
	p.AddRequiredExporter(&capturingExporter{})
	p.AddRequiredExporter(gateFailingExporter{err: errors.New("required unavailable")})
	p.AddExporter(&capturingExporter{})
	confirmed := make(chan delivery.Metadata, 1)
	p.SetDeliveryObserver(func(meta delivery.Metadata) { confirmed <- meta })

	batch := model.Batch{Spans: []model.Span{{Name: "durable", JournalRecordID: "record-1"}}}
	if err := p.Export(context.Background(), batch); err == nil {
		t.Fatal("required exporter failure was hidden")
	}
	select {
	case meta := <-confirmed:
		t.Fatalf("premature durable completion: %+v", meta)
	default:
	}
}

func TestPipeline_OptionalFailureDoesNotBlockDurableCompletion(t *testing.T) {
	p := New[model.Batch](Config{Workers: 1, QueueSize: 16}, slog.Default())
	p.AddRequiredExporter(&capturingExporter{})
	p.AddExporter(gateFailingExporter{err: errors.New("optional unavailable")})
	confirmed := make(chan delivery.Metadata, 1)
	p.SetDeliveryObserver(func(meta delivery.Metadata) { confirmed <- meta })

	batch := model.Batch{Spans: []model.Span{
		{Name: "one", JournalRecordID: "record-1"},
		{Name: "two", JournalRecordID: "record-1"},
	}}
	if err := p.Export(context.Background(), batch); err == nil {
		t.Fatal("optional exporter failure should remain visible to the caller")
	}
	select {
	case meta := <-confirmed:
		if len(meta.Records) != 1 || meta.Records[0].RecordID != "record-1" || meta.Records[0].Units != 2 {
			t.Fatalf("completion metadata = %+v", meta)
		}
	case <-time.After(time.Second):
		t.Fatal("required exporter success did not complete durable work")
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
		if p.DeliveryStats().ItemsDelivered == 2 {
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
		t.Errorf("itemsProcessed = %d, want 2", out)
	}
	delivery := p.DeliveryStats()
	if delivery.ItemsProcessed != 2 || delivery.ItemsDelivered != 2 {
		t.Errorf("DeliveryStats() = %+v, want 2 processed and delivered items", delivery)
	}
	if delivery.BatchesDispatched != 1 || delivery.BatchesDelivered != 1 {
		t.Errorf("DeliveryStats() = %+v, want one dispatched and delivered batch", delivery)
	}

	cancel()
	p.Shutdown(context.Background())
}

func TestPipeline_MultiExporter_FanOut(t *testing.T) {
	recv := &fakeReceiver{}
	exp1 := &capturingExporter{}
	exp2 := &capturingExporter{}

	ctx, cancel := context.WithCancel(context.Background())
	p := New[model.Batch](Config{Workers: 2, QueueSize: 16}, slog.Default())
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

type blockingExporter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *blockingExporter) Export(ctx context.Context, _ model.Batch) error {
	e.once.Do(func() { close(e.started) })
	select {
	case <-e.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *blockingExporter) Close() error { return nil }

func TestPipeline_SlowExporterDoesNotBlockFanOut(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := New[model.Batch](Config{Workers: 1, QueueSize: 4}, slog.Default())
	slow := &blockingExporter{started: make(chan struct{}), release: make(chan struct{})}
	fast := &capturingExporter{}
	p.AddExporter(slow)
	p.AddExporter(fast)
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Enqueue(context.Background(), model.Batch{Spans: []model.Span{{Name: "fanout"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-slow.started:
	case <-time.After(time.Second):
		t.Fatal("slow exporter was not called")
	}
	deadline := time.Now().Add(time.Second)
	for fast.Len() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if fast.Len() != 1 {
		t.Fatal("fast exporter was blocked by the slow exporter")
	}
	close(slow.release)
	cancel()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPipeline_ShutdownUnblocksEnqueueWithoutPanic(t *testing.T) {
	p := New[model.Batch](Config{Workers: 1, QueueSize: 1}, slog.Default())
	b := model.Batch{Spans: []model.Span{{Name: "queued"}}}
	if err := p.Enqueue(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan error, 1)
	go func() { blocked <- p.Enqueue(context.Background(), b) }()
	time.Sleep(5 * time.Millisecond)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-blocked; !errors.Is(err, errPipelineStopped) {
		t.Fatalf("blocked Enqueue error = %v, want pipeline stopped", err)
	}
	if err := p.Enqueue(context.Background(), b); !errors.Is(err, errPipelineStopped) {
		t.Fatalf("Enqueue after shutdown error = %v, want pipeline stopped", err)
	}
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
	p := New[model.Batch](Config{Workers: 1, QueueSize: 16}, slog.Default())
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
	p := New[model.Batch](Config{Workers: 1, QueueSize: 16}, slog.Default())
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

func TestPipeline_Enqueue_CountsDropOnBackpressure(t *testing.T) {
	// A queue of size 1 with no running workers: the first Enqueue buffers, the
	// second blocks until its context expires and must be counted as a drop.
	p := New[model.Batch](Config{Workers: 1, QueueSize: 1}, slog.Default())

	if err := p.Enqueue(context.Background(), model.Batch{Spans: []model.Span{{Name: "buffered"}}}); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Enqueue(ctx, model.Batch{Spans: []model.Span{{Name: "overflow"}}}); err == nil {
		t.Fatal("expected backpressure drop to return ctx error")
	}

	in, dropped, _ := p.Stats()
	if in != 2 {
		t.Errorf("batchesIn = %d, want 2", in)
	}
	if dropped != 1 {
		t.Errorf("batchesDropped = %d, want 1", dropped)
	}
}

func TestPipeline_QueueDepth(t *testing.T) {
	p := New[model.Batch](Config{Workers: 1, QueueSize: 2}, slog.Default())
	if depth, capacity := p.QueueDepth(); depth != 0 || capacity != 2 {
		t.Fatalf("empty QueueDepth() = (%d, %d), want (0, 2)", depth, capacity)
	}
	if err := p.Enqueue(context.Background(), model.Batch{Spans: []model.Span{{Name: "queued"}}}); err != nil {
		t.Fatal(err)
	}
	if depth, capacity := p.QueueDepth(); depth != 1 || capacity != 2 {
		t.Fatalf("filled QueueDepth() = (%d, %d), want (1, 2)", depth, capacity)
	}
}

func TestPipeline_Shutdown_Idempotent(t *testing.T) {
	p := New[model.Batch](Config{Workers: 1, QueueSize: 4}, slog.Default())
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
	drain := p.DrainStats()
	if drain.Outcome != "success" || drain.InProgress || drain.Forced {
		t.Fatalf("DrainStats() = %+v, want completed graceful drain", drain)
	}
}

type contextCheckingExporter struct {
	mu        sync.Mutex
	delivered int
}

func (e *contextCheckingExporter) Export(ctx context.Context, b model.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	e.delivered += b.Len()
	e.mu.Unlock()
	return nil
}

func (*contextCheckingExporter) Close() error { return nil }

func (e *contextCheckingExporter) Len() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.delivered
}

func TestPipeline_RunCancellationDoesNotCancelDrain(t *testing.T) {
	runCtx, cancelRun := context.WithCancel(context.Background())
	p := New[model.Batch](Config{Workers: 1, QueueSize: 32}, slog.Default())
	exp := &contextCheckingExporter{}
	p.AddExporter(exp)
	if err := p.Start(runCtx); err != nil {
		t.Fatal(err)
	}

	const batches = 20
	for i := 0; i < batches; i++ {
		if err := p.Enqueue(context.Background(), model.Batch{
			Spans: []model.Span{{Name: "queued-before-signal"}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	cancelRun()

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if exp.Len() != batches {
		t.Fatalf("delivered items = %d, want %d after run-context cancellation", exp.Len(), batches)
	}
	if got := p.DeliveryStats().ItemsDelivered; got != batches {
		t.Fatalf("ItemsDelivered = %d, want %d", got, batches)
	}
}

func TestPipeline_ShutdownDeadlineCancelsBlockedExporter(t *testing.T) {
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	p := New[model.Batch](Config{Workers: 1, QueueSize: 4}, slog.Default())
	exp := &blockingExporter{started: make(chan struct{}), release: make(chan struct{})}
	p.AddExporter(exp)
	if err := p.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	if err := p.Enqueue(context.Background(), model.Batch{
		Spans: []model.Span{{Name: "blocked"}},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-exp.started:
	case <-time.After(time.Second):
		t.Fatal("exporter did not start")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stopCancel()
	started := time.Now()
	err := p.Shutdown(stopCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("Shutdown exceeded its deadline by too much: %v", elapsed)
	}

	waitUntil := time.Now().Add(time.Second)
	for p.DrainStats().InProgress && time.Now().Before(waitUntil) {
		time.Sleep(time.Millisecond)
	}
	drain := p.DrainStats()
	if drain.Outcome != "deadline" || !drain.Forced {
		t.Fatalf("DrainStats() = %+v, want forced deadline outcome", drain)
	}
	if err := p.Shutdown(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("repeated Shutdown error = %v, want terminal deadline error", err)
	}
}

type failingExporter struct{ err error }

func (e *failingExporter) Export(context.Context, model.Batch) error { return e.err }
func (*failingExporter) Close() error                                { return nil }

type metadataErasingProcessor struct{ err error }

func (p metadataErasingProcessor) Process(context.Context, model.Batch) (model.Batch, error) {
	return model.Batch{}, p.err
}
func (metadataErasingProcessor) Close() error { return nil }

func TestPipeline_ProcessorFailureRetainsInputDeliveryMetadata(t *testing.T) {
	p := New[model.Batch](Config{Workers: 1, QueueSize: 4}, slog.Default())
	p.AddProcessor(metadataErasingProcessor{err: errors.New("processor failed")})
	failures := make(chan delivery.Metadata, 1)
	p.SetDeliveryFailureObserver(func(meta delivery.Metadata, _ error) { failures <- meta })
	err := p.Export(context.Background(), model.Batch{Spans: []model.Span{{
		Name: "owned", JournalRecordID: "record-1", DeliveryAttempt: 7,
	}}})
	if err == nil {
		t.Fatal("processor failure was hidden")
	}
	select {
	case meta := <-failures:
		if len(meta.Records) != 1 || meta.Records[0].RecordID != "record-1" || meta.Records[0].Attempt != 7 {
			t.Fatalf("failure metadata = %+v", meta)
		}
	case <-time.After(time.Second):
		t.Fatal("processor failure lost its input delivery metadata")
	}
}

func TestPipeline_ShutdownReportsDeliveryFailure(t *testing.T) {
	runCtx, cancelRun := context.WithCancel(context.Background())
	p := New[model.Batch](Config{Workers: 1, QueueSize: 4}, slog.Default())
	p.AddExporter(&failingExporter{err: errors.New("injected storage failure")})
	if err := p.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	if err := p.Enqueue(context.Background(), model.Batch{
		Spans: []model.Span{{Name: "lost"}},
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for p.DeliveryStats().ExporterFailures == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancelRun()

	err := p.Shutdown(context.Background())
	if err == nil || !strings.Contains(err.Error(), "exporter_failures=1") {
		t.Fatalf("Shutdown error = %v, want delivery failure summary", err)
	}
	if got := p.DrainStats().Outcome; got != "failed" {
		t.Fatalf("drain outcome = %q, want failed", got)
	}
}

func TestPipeline_ShutdownDoesNotReportDurablyRetriedFailureAsLoss(t *testing.T) {
	runCtx, cancelRun := context.WithCancel(context.Background())
	p := New[model.Batch](Config{Workers: 1, QueueSize: 4}, slog.Default())
	p.AddRequiredExporter(&failingExporter{err: errors.New("temporary Amber failure")})
	p.SetDeliveryObserver(func(delivery.Metadata) {})
	failures := make(chan delivery.Metadata, 1)
	p.SetDeliveryFailureObserver(func(meta delivery.Metadata, _ error) { failures <- meta })
	if err := p.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	if err := p.Enqueue(context.Background(), model.Batch{Spans: []model.Span{{
		Name: "retained", JournalRecordID: "record-1", DeliveryAttempt: 1,
	}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-failures:
	case <-time.After(time.Second):
		t.Fatal("required failure was not returned to durable delivery")
	}
	cancelRun()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("durably retained failure reported as terminal loss: %v", err)
	}
}

type closeTrackingExporter struct{ closed bool }

func (*closeTrackingExporter) Export(context.Context, model.Batch) error { return nil }
func (e *closeTrackingExporter) Close() error {
	e.closed = true
	return nil
}

func TestPipeline_CloseUnstartedReleasesExporter(t *testing.T) {
	p := New[model.Batch](Config{Workers: 1, QueueSize: 4}, slog.Default())
	exp := &closeTrackingExporter{}
	p.AddExporter(exp)
	if err := p.CloseUnstarted(); err != nil {
		t.Fatalf("CloseUnstarted: %v", err)
	}
	if !exp.closed {
		t.Fatal("exporter was not closed")
	}
}
