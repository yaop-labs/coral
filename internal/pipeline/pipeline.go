package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
)

// Config controls pipeline concurrency.
type Config struct {
	Workers   int
	QueueSize int
}

func (c *Config) setDefaults() {
	if c.Workers <= 0 {
		c.Workers = runtime.NumCPU()
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 10000
	}
}

// Pipeline moves batches of one signal from receivers through processors to
// exporters. A single worker-pool implementation serves every signal type
// (traces, metrics, logs); the element type T carries the signal-specific data.
//
// Delivery is at-most-once within coral: there is no spool, so batches are
// dropped on backpressure or shutdown rather than persisted. End-to-end
// durability rests on the wisp spool and amber WAL at the edges (contract §1).
type Pipeline[T Signal] struct {
	cfg        Config
	receivers  []Receiver[T]
	processors []Processor[T]
	exporters  []Exporter[T]
	logger     *slog.Logger

	in           chan T
	wg           sync.WaitGroup
	shutdownOnce sync.Once

	batchesIn      atomic.Uint64
	batchesDropped atomic.Uint64
	itemsOut       atomic.Uint64
}

// New creates a pipeline for signal type T. T must be supplied explicitly, as
// it cannot be inferred from the arguments: pipeline.New[model.Batch](cfg, log).
func New[T Signal](cfg Config, logger *slog.Logger) *Pipeline[T] {
	cfg.setDefaults()
	return &Pipeline[T]{
		cfg:    cfg,
		logger: logger,
		in:     make(chan T, cfg.QueueSize),
	}
}

func (p *Pipeline[T]) AddReceiver(r Receiver[T])    { p.receivers = append(p.receivers, r) }
func (p *Pipeline[T]) AddProcessor(pr Processor[T]) { p.processors = append(p.processors, pr) }
func (p *Pipeline[T]) AddExporter(e Exporter[T])    { p.exporters = append(p.exporters, e) }

// Enqueue pushes b onto the worker queue, blocking on backpressure until a
// worker is free or ctx is canceled. Empty batches are dropped silently. It is
// the single entry point for every source — Receiver emit closures and the
// shared OTLP ingress alike. Enqueue must not be called after Shutdown.
func (p *Pipeline[T]) Enqueue(ctx context.Context, b T) error {
	if b.Len() == 0 {
		return nil
	}
	p.batchesIn.Add(1)
	select {
	case p.in <- b:
		return nil
	case <-ctx.Done():
		p.batchesDropped.Add(1)
		return ctx.Err()
	}
}

// Start launches the worker pool and all receivers.
func (p *Pipeline[T]) Start(ctx context.Context) error {
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}

	for _, r := range p.receivers {
		go func() {
			if err := r.Start(ctx, p.Enqueue); err != nil && ctx.Err() == nil {
				p.logger.Error("receiver exited with error", "err", err)
			}
		}()
	}
	return nil
}

// Shutdown stops receivers, drains the queue, closes processors and exporters.
// Safe to call multiple times; subsequent calls are no-ops.
func (p *Pipeline[T]) Shutdown(ctx context.Context) error {
	var err error
	p.shutdownOnce.Do(func() {
		for _, r := range p.receivers {
			if stopErr := r.Stop(ctx); stopErr != nil {
				p.logger.Error("receiver stop error", "err", stopErr)
			}
		}

		close(p.in)
		p.wg.Wait()

		for i := len(p.processors) - 1; i >= 0; i-- {
			if closeErr := p.processors[i].Close(); closeErr != nil {
				p.logger.Error("processor close error", "err", closeErr)
			}
		}
		for _, e := range p.exporters {
			if closeErr := e.Close(); closeErr != nil {
				p.logger.Error("exporter close error", "err", closeErr)
				err = closeErr
			}
		}
	})
	return err
}

// Export sends b through the full processor chain and then to exporters.
func (p *Pipeline[T]) Export(ctx context.Context, b T) error {
	return p.processFrom(ctx, b, 0)
}

// ExportFrom sends b through processors starting at startIndex.
// It is used by stateful processors that flush batches downstream.
func (p *Pipeline[T]) ExportFrom(ctx context.Context, b T, startIndex int) error {
	return p.processFrom(ctx, b, startIndex)
}

func (p *Pipeline[T]) worker(ctx context.Context) {
	defer p.wg.Done()
	for b := range p.in {
		if err := p.processFrom(ctx, b, 0); err != nil && ctx.Err() == nil {
			p.logger.Error("pipeline processing error", "err", err)
		}
	}
}

func (p *Pipeline[T]) processFrom(ctx context.Context, b T, startIndex int) error {
	var err error
	if startIndex < 0 {
		startIndex = 0
	}
	if startIndex > len(p.processors) {
		startIndex = len(p.processors)
	}
	for _, pr := range p.processors[startIndex:] {
		b, err = pr.Process(ctx, b)
		if err != nil {
			return fmt.Errorf("processor: %w", err)
		}
		if b.Len() == 0 {
			return nil
		}
	}
	for _, e := range p.exporters {
		if err := e.Export(ctx, b); err != nil {
			p.logger.Error("exporter error", "err", err)
		}
	}
	p.itemsOut.Add(uint64(b.Len()))
	return nil
}

// Stats returns pipeline counters for observability: batches enqueued, batches
// dropped on backpressure, and items (spans/points/records) exported.
func (p *Pipeline[T]) Stats() (batchesIn, batchesDropped, itemsOut uint64) {
	return p.batchesIn.Load(), p.batchesDropped.Load(), p.itemsOut.Load()
}
