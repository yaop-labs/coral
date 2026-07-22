package processor

import (
	"context"
	"sync"
	"time"

	"github.com/yaop-labs/coral/internal/model"
)

// BatchProcessor accumulates spans and flushes when either maxSize is
// reached or timeout elapses, whichever comes first.
// Flushed batches are sent to the emit function passed to NewBatch.
// Process returns an empty batch; spans are emitted asynchronously.
type BatchProcessor struct {
	maxSize  int
	maxBytes int64
	timeout  time.Duration
	emit     func(context.Context, model.Batch) error

	mu           sync.Mutex
	pending      []model.Span
	pendingBytes int64
	timer        *time.Timer
	done         chan struct{}
	now          func() time.Time
}

func NewBatch(maxSize int, timeout time.Duration, emit func(context.Context, model.Batch) error, maxBytes ...int64) *BatchProcessor {
	if maxSize <= 0 {
		maxSize = 512
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	bytes := int64(64 << 20)
	if len(maxBytes) > 0 && maxBytes[0] > 0 {
		bytes = maxBytes[0]
	}
	return &BatchProcessor{
		maxSize:  maxSize,
		maxBytes: bytes,
		timeout:  timeout,
		emit:     emit,
		done:     make(chan struct{}),
		now:      time.Now,
	}
}

func (p *BatchProcessor) Process(ctx context.Context, b model.Batch) (model.Batch, error) {
	p.mu.Lock()
	var toFlush []model.Span
	for _, span := range b.Spans {
		spanBytes := int64(span.SizeBytes())
		if len(p.pending) > 0 && p.pendingBytes+spanBytes > p.maxBytes {
			toFlush = append(toFlush, p.drainLocked()...)
		}
		if spanBytes > p.maxBytes {
			toFlush = append(toFlush, span)
			continue
		}
		p.pending = append(p.pending, span)
		p.pendingBytes += spanBytes
	}
	shouldFlush := len(p.pending) >= p.maxSize
	if p.timer == nil && !shouldFlush {
		p.timer = time.AfterFunc(p.timeout, func() { p.flush(ctx) })
	}
	if shouldFlush {
		toFlush = append(toFlush, p.drainLocked()...)
	}
	p.mu.Unlock()

	if len(toFlush) > 0 {
		_ = p.emit(ctx, model.Batch{Spans: toFlush})
	}

	return model.Batch{}, nil
}

func (p *BatchProcessor) Close() error {
	p.mu.Lock()
	remaining := p.drainLocked()
	p.mu.Unlock()

	if len(remaining) > 0 {
		_ = p.emit(context.Background(), model.Batch{Spans: remaining})
	}
	return nil
}

func (p *BatchProcessor) flush(ctx context.Context) {
	p.mu.Lock()
	spans := p.drainLocked()
	p.mu.Unlock()
	if len(spans) > 0 {
		_ = p.emit(ctx, model.Batch{Spans: spans})
	}
}

// drainLocked must be called with p.mu held.
func (p *BatchProcessor) drainLocked() []model.Span {
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}
	if len(p.pending) == 0 {
		return nil
	}
	out := p.pending
	p.pending = nil
	p.pendingBytes = 0
	return out
}

// Stats reports retained batch state for bounded observability and tests.
func (p *BatchProcessor) Stats() (items int, bytes, maxBytes int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pending), p.pendingBytes, p.maxBytes
}
