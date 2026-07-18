package sampling

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/yaop-labs/coral/internal/model"
)

type decision uint8

const (
	decisionPending decision = iota
	decisionKeep
	decisionDrop
)

// Rule decides whether a pending trace should be kept.
type Rule interface {
	Match(t *PendingTrace) bool
}

// PendingTrace holds all spans for one trace until a decision is made.
type PendingTrace struct {
	ID        model.TraceID
	Tenant    string
	Spans     []model.Span
	FirstSeen time.Time
	LastSeen  time.Time
	HasError  bool
	HasDebug  bool
	HasRoot   bool
}

type traceKey struct {
	tenant string
	id     model.TraceID
}

func (pt *PendingTrace) add(s model.Span) {
	pt.Spans = append(pt.Spans, s)
	if s.HasError() {
		pt.HasError = true
	}
	if s.AttrValue("debug") == "true" {
		pt.HasDebug = true
	}
	if s.IsRoot() {
		pt.HasRoot = true
	}
	if s.StartTime.Before(pt.FirstSeen) || pt.FirstSeen.IsZero() {
		pt.FirstSeen = s.StartTime
	}
	now := s.EndTime
	if now.IsZero() {
		now = s.StartTime
	}
	if now.After(pt.LastSeen) {
		pt.LastSeen = now
	}
}

// TailSampler buffers spans by trace ID and makes a keep/drop decision after
// decisionWait has elapsed since the last span arrived for that trace.
// Decided batches are emitted via the export function provided at construction.
type TailSampler struct {
	decisionWait time.Duration
	maxTraces    int
	maxBytes     int64
	currentBytes int64
	defaultRate  float64
	rules        []Rule
	export       func(context.Context, model.Batch) error

	mu      sync.Mutex
	pending map[traceKey]*PendingTrace

	decided *lru.Cache[traceKey, decision]
	tenant  func(context.Context) string

	ticker    *time.Ticker
	done      chan struct{}
	now       func() time.Time
	closeOnce sync.Once
	startOnce sync.Once
}

// Stats returns the currently buffered trace count and byte usage.
func (ts *TailSampler) Stats() (pendingTraces int, pendingBytes int64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return len(ts.pending), ts.currentBytes
}

func NewTail(
	decisionWait time.Duration,
	maxTraces int,
	defaultRate float64,
	rules []Rule,
	export func(context.Context, model.Batch) error, maxBytes ...int64,
) *TailSampler {
	if decisionWait <= 0 {
		decisionWait = 30 * time.Second
	}
	if maxTraces <= 0 {
		maxTraces = 100_000
	}
	bytes := int64(256 << 20)
	if len(maxBytes) > 0 && maxBytes[0] > 0 {
		bytes = maxBytes[0]
	}
	cache, _ := lru.New[traceKey, decision](maxTraces * 2)
	return &TailSampler{
		decisionWait: decisionWait,
		maxTraces:    maxTraces,
		maxBytes:     bytes,
		defaultRate:  defaultRate,
		rules:        rules,
		export:       export,
		pending:      make(map[traceKey]*PendingTrace),
		decided:      cache,
		done:         make(chan struct{}),
		now:          time.Now,
		tenant:       func(context.Context) string { return "" },
	}
}

// SetTenantExtractor configures the optional tenant identity used in trace
// buffering and deduplication keys.
func (ts *TailSampler) SetTenantExtractor(fn func(context.Context) string) {
	if fn == nil {
		fn = func(context.Context) string { return "" }
	}
	ts.mu.Lock()
	ts.tenant = fn
	ts.mu.Unlock()
}

// Process buffers spans by trace ID and returns an empty batch.
// Decided traces are emitted asynchronously via the export function.
func (ts *TailSampler) Process(ctx context.Context, b model.Batch) (model.Batch, error) {
	var emit []model.Batch

	ts.mu.Lock()
	now := ts.now()
	tenant := ts.tenant(ctx)
	for _, s := range b.Spans {
		key := traceKey{tenant: tenant, id: s.TraceID}
		if d, ok := ts.decided.Get(key); ok {
			if d == decisionKeep {
				emit = append(emit, model.Batch{Spans: []model.Span{s}})
			}
			continue
		}
		pt, ok := ts.pending[key]
		if !ok {
			if len(ts.pending) >= ts.maxTraces {
				if evicted := ts.evictOldestLocked(); evicted != nil {
					ts.currentBytes -= pendingBytes(evicted)
					d := ts.decide(evicted)
					ts.decided.Add(traceKey{tenant: evicted.Tenant, id: evicted.ID}, d)
					if d == decisionKeep {
						emit = append(emit, model.Batch{Spans: evicted.Spans})
					}
				}
			}
			pt = &PendingTrace{ID: s.TraceID, Tenant: tenant, FirstSeen: now}
			ts.pending[key] = pt
		}
		pt.add(s)
		ts.currentBytes += int64(s.SizeBytes())
		for ts.currentBytes > ts.maxBytes {
			if evicted := ts.evictOldestLocked(); evicted != nil {
				ts.currentBytes -= pendingBytes(evicted)
			} else {
				break
			}
		}
	}
	ts.mu.Unlock()

	for _, batch := range emit {
		_ = ts.export(ctx, batch)
	}
	return model.Batch{}, nil
}

func (ts *TailSampler) evictOldestLocked() *PendingTrace {
	var oldestID traceKey
	var oldest *PendingTrace
	for id, pt := range ts.pending {
		if oldest == nil || pt.LastSeen.Before(oldest.LastSeen) {
			oldestID = id
			oldest = pt
		}
	}
	if oldest == nil {
		return nil
	}
	delete(ts.pending, oldestID)
	return oldest
}

func pendingBytes(pt *PendingTrace) int64 {
	var n int64
	for _, s := range pt.Spans {
		n += int64(s.SizeBytes())
	}
	return n
}

// Start launches the background tick loop that ages out pending traces.
func (ts *TailSampler) Start(ctx context.Context) {
	ts.startOnce.Do(func() {
		ts.ticker = time.NewTicker(ts.decisionWait / 2)
		go func() {
			for {
				select {
				case t := <-ts.ticker.C:
					ts.tickAt(ctx, t)
				case <-ts.done:
					return
				}
			}
		}()
	})
}

// tickAt ages out traces whose last span arrived more than decisionWait ago.
func (ts *TailSampler) tickAt(ctx context.Context, now time.Time) {
	ts.mu.Lock()
	var ready []*PendingTrace
	for id, pt := range ts.pending {
		age := now.Sub(pt.LastSeen)
		if age >= ts.decisionWait {
			ready = append(ready, pt)
			delete(ts.pending, id)
			ts.currentBytes -= pendingBytes(pt)
		}
	}
	ts.mu.Unlock()

	for _, pt := range ready {
		d := ts.decide(pt)
		ts.decided.Add(traceKey{tenant: pt.Tenant, id: pt.ID}, d)
		if d == decisionKeep {
			_ = ts.export(ctx, model.Batch{Spans: pt.Spans})
		}
	}
}

// decide returns keep or drop for a completed trace.
func (ts *TailSampler) decide(pt *PendingTrace) decision {
	for _, r := range ts.rules {
		if r.Match(pt) {
			return decisionKeep
		}
	}
	if rand.Float64() < ts.defaultRate {
		return decisionKeep
	}
	return decisionDrop
}

// Close stops the tick loop and flushes all buffered traces (force keep).
func (ts *TailSampler) Close() error {
	ts.closeOnce.Do(func() {
		if ts.ticker != nil {
			ts.ticker.Stop()
		}
		close(ts.done)
		ts.mu.Lock()
		remaining := make([]*PendingTrace, 0, len(ts.pending))
		for id, pt := range ts.pending {
			remaining = append(remaining, pt)
			delete(ts.pending, id)
		}
		ts.currentBytes = 0
		ts.mu.Unlock()
		for _, pt := range remaining {
			_ = ts.export(context.Background(), model.Batch{Spans: pt.Spans})
		}
	})
	return nil
}

// ErrorRule keeps traces that contain at least one error span.
type ErrorRule struct{}

func (ErrorRule) Match(t *PendingTrace) bool { return t.HasError }

// DebugTagRule keeps traces tagged with debug="true".
type DebugTagRule struct{}

func (DebugTagRule) Match(t *PendingTrace) bool { return t.HasDebug }

// DurationRule keeps traces whose duration exceeds Threshold.
type DurationRule struct{ Threshold time.Duration }

func (r DurationRule) Match(t *PendingTrace) bool {
	return t.LastSeen.Sub(t.FirstSeen) >= r.Threshold
}

// ServiceRule keeps traces that include any span from the given services.
type ServiceRule struct{ Services map[string]struct{} }

func (r ServiceRule) Match(t *PendingTrace) bool {
	for _, s := range t.Spans {
		if _, ok := r.Services[s.Resource.ServiceName()]; ok {
			return true
		}
	}
	return false
}
