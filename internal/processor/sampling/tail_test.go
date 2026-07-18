package sampling

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yaop-labs/coral/internal/model"
)

func traceSpan(traceID byte, spanID byte, status model.SpanStatus) model.Span {
	return model.Span{
		TraceID: model.TraceID{traceID},
		SpanID:  model.SpanID{spanID},
		Status:  status,
	}
}

func makeExport(mu *sync.Mutex, out *[]model.Span) func(context.Context, model.Batch) error {
	return func(_ context.Context, b model.Batch) error {
		mu.Lock()
		*out = append(*out, b.Spans...)
		mu.Unlock()
		return nil
	}
}

func TestTailSampler_ErrorRuleKeeps(t *testing.T) {
	var mu sync.Mutex
	var exported []model.Span

	ts := NewTail(100*time.Millisecond, 1000, 0.0, []Rule{ErrorRule{}}, makeExport(&mu, &exported))
	now := time.Now()
	ts.now = func() time.Time { return now }

	ctx := context.Background()
	s := traceSpan(1, 1, model.StatusError)
	s.StartTime = now.Add(-200 * time.Millisecond)
	ts.Process(ctx, model.Batch{Spans: []model.Span{s}})

	// Advance clock past decisionWait and tick.
	ts.tickAt(ctx, now.Add(200*time.Millisecond))

	mu.Lock()
	n := len(exported)
	mu.Unlock()
	if n != 1 {
		t.Errorf("expected 1 exported span (error rule), got %d", n)
	}
}

func TestTailSampler_DropAtZeroRate(t *testing.T) {
	var mu sync.Mutex
	var exported []model.Span

	ts := NewTail(100*time.Millisecond, 1000, 0.0, nil, makeExport(&mu, &exported))
	ts.now = func() time.Time { return time.Now() }

	ctx := context.Background()
	s := traceSpan(2, 1, model.StatusOK)
	s.StartTime = time.Now().Add(-300 * time.Millisecond)
	s.EndTime = s.StartTime.Add(10 * time.Millisecond)
	ts.Process(ctx, model.Batch{Spans: []model.Span{s}})
	ts.tickAt(ctx, time.Now().Add(200*time.Millisecond))

	mu.Lock()
	n := len(exported)
	mu.Unlock()
	if n != 0 {
		t.Errorf("expected 0 exported spans at 0%% rate, got %d", n)
	}
}

func TestTailSampler_DebugTagRule(t *testing.T) {
	var mu sync.Mutex
	var exported []model.Span

	ts := NewTail(50*time.Millisecond, 1000, 0.0, []Rule{DebugTagRule{}}, makeExport(&mu, &exported))
	now := time.Now()
	ts.now = func() time.Time { return now }

	ctx := context.Background()
	s := model.Span{
		TraceID: model.TraceID{3},
		SpanID:  model.SpanID{1},
		Attrs:   []model.Attribute{model.StringAttr("debug", "true")},
	}
	s.StartTime = now.Add(-200 * time.Millisecond)
	ts.Process(ctx, model.Batch{Spans: []model.Span{s}})
	ts.tickAt(ctx, now.Add(200*time.Millisecond))

	mu.Lock()
	n := len(exported)
	mu.Unlock()
	if n != 1 {
		t.Errorf("expected 1 exported span (debug tag), got %d", n)
	}
}

func TestTailSampler_DurationRule(t *testing.T) {
	var mu sync.Mutex
	var exported []model.Span

	ts := NewTail(50*time.Millisecond, 1000, 0.0,
		[]Rule{DurationRule{Threshold: 500 * time.Millisecond}},
		makeExport(&mu, &exported))
	now := time.Now()
	ts.now = func() time.Time { return now }

	ctx := context.Background()

	// Long trace.
	long := model.Span{
		TraceID:   model.TraceID{4},
		SpanID:    model.SpanID{1},
		StartTime: now.Add(-700 * time.Millisecond),
		EndTime:   now.Add(-100 * time.Millisecond),
	}
	// Short trace.
	short := model.Span{
		TraceID:   model.TraceID{5},
		SpanID:    model.SpanID{1},
		StartTime: now.Add(-200 * time.Millisecond),
		EndTime:   now.Add(-150 * time.Millisecond),
	}
	ts.Process(ctx, model.Batch{Spans: []model.Span{long, short}})
	ts.tickAt(ctx, now.Add(200*time.Millisecond))

	mu.Lock()
	n := len(exported)
	mu.Unlock()
	if n != 1 {
		t.Errorf("expected 1 kept span (duration rule), got %d", n)
	}
}

func TestTailSampler_LateSpanForDecidedTrace_Keep(t *testing.T) {
	var mu sync.Mutex
	var exported []model.Span

	ts := NewTail(50*time.Millisecond, 1000, 0.0, []Rule{ErrorRule{}}, makeExport(&mu, &exported))
	now := time.Now()
	ts.now = func() time.Time { return now }
	ctx := context.Background()

	// First span triggers keep decision.
	s1 := traceSpan(6, 1, model.StatusError)
	s1.StartTime = now.Add(-200 * time.Millisecond)
	ts.Process(ctx, model.Batch{Spans: []model.Span{s1}})
	ts.tickAt(ctx, now.Add(200*time.Millisecond))

	// Late span for same trace.
	s2 := traceSpan(6, 2, model.StatusOK)
	ts.Process(ctx, model.Batch{Spans: []model.Span{s2}})

	mu.Lock()
	n := len(exported)
	mu.Unlock()
	if n < 2 {
		t.Errorf("expected ≥2 exported spans (late span for kept trace), got %d", n)
	}
}

func TestTailSampler_Close_FlushesAll(t *testing.T) {
	var mu sync.Mutex
	var exported []model.Span

	ts := NewTail(time.Minute, 1000, 0.0, nil, makeExport(&mu, &exported))
	ctx := context.Background()

	// decisionWait has not elapsed.
	ts.Process(ctx, model.Batch{Spans: []model.Span{
		traceSpan(7, 1, model.StatusOK),
		traceSpan(8, 1, model.StatusOK),
	}})

	mu.Lock()
	n := len(exported)
	mu.Unlock()
	if n != 0 {
		t.Error("should not export before Close")
	}

	ts.Close()

	mu.Lock()
	n = len(exported)
	mu.Unlock()
	if n != 2 {
		t.Errorf("expected 2 spans flushed on Close, got %d", n)
	}
}

func TestTailSampler_MultipleSpansPerTrace(t *testing.T) {
	var mu sync.Mutex
	var exported []model.Span

	ts := NewTail(50*time.Millisecond, 1000, 1.0, nil, makeExport(&mu, &exported))
	now := time.Now()
	ts.now = func() time.Time { return now }
	ctx := context.Background()

	spans := []model.Span{
		{TraceID: model.TraceID{9}, SpanID: model.SpanID{1}, StartTime: now.Add(-200 * time.Millisecond)},
		{TraceID: model.TraceID{9}, SpanID: model.SpanID{2}, StartTime: now.Add(-200 * time.Millisecond)},
		{TraceID: model.TraceID{9}, SpanID: model.SpanID{3}, StartTime: now.Add(-200 * time.Millisecond)},
	}
	ts.Process(ctx, model.Batch{Spans: spans})
	ts.tickAt(ctx, now.Add(200*time.Millisecond))

	mu.Lock()
	n := len(exported)
	mu.Unlock()
	if n != 3 {
		t.Errorf("expected 3 spans (all kept at 100%%), got %d", n)
	}
}

func TestTailSampler_MaxTraces_Evicts(t *testing.T) {
	var mu sync.Mutex
	var exported []model.Span

	maxTraces := 5
	ts := NewTail(time.Minute, maxTraces, 1.0, nil, makeExport(&mu, &exported))
	now := time.Now()
	ts.now = func() time.Time { return now }
	ctx := context.Background()

	// Fill to maxTraces
	for i := 0; i < maxTraces; i++ {
		s := model.Span{
			TraceID:   model.TraceID{byte(20 + i)},
			SpanID:    model.SpanID{byte(i)},
			StartTime: now.Add(-time.Duration(i) * time.Millisecond),
		}
		ts.Process(ctx, model.Batch{Spans: []model.Span{s}})
	}

	// Add one more span. The sampler must evict one pending trace immediately
	// instead of allowing the pending map to grow beyond maxTraces.
	extra := model.Span{
		TraceID:   model.TraceID{byte(99)},
		SpanID:    model.SpanID{99},
		StartTime: now.Add(-200 * time.Millisecond),
	}
	ts.Process(ctx, model.Batch{Spans: []model.Span{extra}})

	ts.mu.Lock()
	pending := len(ts.pending)
	ts.mu.Unlock()
	if pending != maxTraces {
		t.Fatalf("pending traces = %d, want %d", pending, maxTraces)
	}

	// Advance clock so all traces are past decisionWait (1 minute); flush everything.
	ts.tickAt(ctx, now.Add(2*time.Minute))

	mu.Lock()
	n := len(exported)
	mu.Unlock()

	if n != maxTraces+1 {
		t.Errorf("expected %d exported spans, got %d", maxTraces+1, n)
	}
}

func TestTailSampler_MaxBytes_Evicts(t *testing.T) {
	ts := NewTail(time.Minute, 100, 1.0, nil, func(context.Context, model.Batch) error { return nil }, 100)
	ts.Process(context.Background(), model.Batch{Spans: []model.Span{{TraceID: model.TraceID{1}, Name: "large"}}})
	ts.Process(context.Background(), model.Batch{Spans: []model.Span{{TraceID: model.TraceID{2}, Name: "large"}}})
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if len(ts.pending) > 1 {
		t.Fatalf("pending traces = %d, want byte-bounded eviction", len(ts.pending))
	}
	if ts.currentBytes > ts.maxBytes {
		t.Fatalf("current bytes = %d, max = %d", ts.currentBytes, ts.maxBytes)
	}
}

func TestTailSampler_ServiceRule_Keeps(t *testing.T) {
	var mu sync.Mutex
	var exported []model.Span

	rule := ServiceRule{Services: map[string]struct{}{"my-svc": {}}}
	ts := NewTail(50*time.Millisecond, 1000, 0.0, []Rule{rule}, makeExport(&mu, &exported))
	now := time.Now()
	ts.now = func() time.Time { return now }
	ctx := context.Background()

	keep := model.Span{
		TraceID:   model.TraceID{30},
		SpanID:    model.SpanID{1},
		StartTime: now.Add(-200 * time.Millisecond),
		Resource:  model.Resource{Attrs: []model.Attribute{model.StringAttr("service.name", "my-svc")}},
	}
	drop := model.Span{
		TraceID:   model.TraceID{31},
		SpanID:    model.SpanID{1},
		StartTime: now.Add(-200 * time.Millisecond),
		Resource:  model.Resource{Attrs: []model.Attribute{model.StringAttr("service.name", "other-svc")}},
	}

	ts.Process(ctx, model.Batch{Spans: []model.Span{keep, drop}})
	ts.tickAt(ctx, now.Add(200*time.Millisecond))

	mu.Lock()
	n := len(exported)
	mu.Unlock()
	if n != 1 {
		t.Errorf("expected 1 kept span (service rule), got %d", n)
	}
	if exported[0].Resource.ServiceName() != "my-svc" {
		t.Errorf("expected my-svc span to be kept, got %q", exported[0].Resource.ServiceName())
	}
}

func TestTailSampler_DecidedCache_LateSpan_Drop(t *testing.T) {
	var mu sync.Mutex
	var exported []model.Span

	// defaultRate=0 and no rules drops all traces.
	ts := NewTail(50*time.Millisecond, 1000, 0.0, nil, makeExport(&mu, &exported))
	now := time.Now()
	ts.now = func() time.Time { return now }
	ctx := context.Background()

	// Send a span and force a drop decision.
	s1 := model.Span{
		TraceID:   model.TraceID{40},
		SpanID:    model.SpanID{1},
		StartTime: now.Add(-200 * time.Millisecond),
	}
	ts.Process(ctx, model.Batch{Spans: []model.Span{s1}})
	ts.tickAt(ctx, now.Add(200*time.Millisecond))

	mu.Lock()
	n := len(exported)
	mu.Unlock()
	if n != 0 {
		t.Fatalf("expected 0 exports after drop decision, got %d", n)
	}

	// Late span for the same (dropped) trace should be silently discarded.
	late := model.Span{
		TraceID: model.TraceID{40},
		SpanID:  model.SpanID{2},
	}
	ts.Process(ctx, model.Batch{Spans: []model.Span{late}})

	mu.Lock()
	n = len(exported)
	mu.Unlock()
	if n != 0 {
		t.Errorf("late span for dropped trace must not be exported, got %d", n)
	}
}

func TestTailSampler_ConcurrentSpans(t *testing.T) {
	var mu sync.Mutex
	exportCalls := 0
	export := func(_ context.Context, b model.Batch) error {
		mu.Lock()
		exportCalls++
		mu.Unlock()
		return nil
	}

	sharedTraceID := model.TraceID{50}
	ts := NewTail(50*time.Millisecond, 1000, 1.0, nil, export)
	now := time.Now()
	ts.now = func() time.Time { return now }
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := model.Span{
				TraceID:   sharedTraceID,
				SpanID:    model.SpanID{byte(i)},
				StartTime: now.Add(-200 * time.Millisecond),
			}
			ts.Process(ctx, model.Batch{Spans: []model.Span{s}})
		}()
	}
	wg.Wait()

	// Tick after all goroutines finish.
	ts.tickAt(ctx, now.Add(200*time.Millisecond))

	mu.Lock()
	n := exportCalls
	mu.Unlock()
	if n != 1 {
		t.Errorf("expected exactly 1 export call for the single trace, got %d", n)
	}
}
