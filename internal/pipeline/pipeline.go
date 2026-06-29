package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/yaop-labs/coral/internal/model"
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

// Pipeline moves batches from receivers through processors to exporters.
type Pipeline struct {
	cfg        Config
	receivers  []Receiver
	processors []Processor
	exporters  []Exporter
	logger     *slog.Logger

	in           chan model.Batch
	wg           sync.WaitGroup
	shutdownOnce sync.Once

	batchesIn      atomic.Uint64
	batchesDropped atomic.Uint64
	spansOut       atomic.Uint64
}

func New(cfg Config, logger *slog.Logger) *Pipeline {
	cfg.setDefaults()
	return &Pipeline{
		cfg:    cfg,
		logger: logger,
		in:     make(chan model.Batch, cfg.QueueSize),
	}
}

func (p *Pipeline) AddReceiver(r Receiver)    { p.receivers = append(p.receivers, r) }
func (p *Pipeline) AddProcessor(pr Processor) { p.processors = append(p.processors, pr) }
func (p *Pipeline) AddExporter(e Exporter)    { p.exporters = append(p.exporters, e) }

// Start launches the worker pool and all receivers.
func (p *Pipeline) Start(ctx context.Context) error {
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}

	emit := func(ctx context.Context, b model.Batch) error {
		if len(b.Spans) == 0 {
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

	for _, r := range p.receivers {
		r := r
		go func() {
			if err := r.Start(ctx, emit); err != nil && ctx.Err() == nil {
				p.logger.Error("receiver exited with error", "err", err)
			}
		}()
	}
	return nil
}

// Shutdown stops receivers, drains the queue, closes processors and exporters.
// Safe to call multiple times; subsequent calls are no-ops.
func (p *Pipeline) Shutdown(ctx context.Context) error {
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
func (p *Pipeline) Export(ctx context.Context, b model.Batch) error {
	return p.processFrom(ctx, b, 0)
}

// ExportFrom sends b through processors starting at startIndex.
// It is used by stateful processors that flush batches downstream.
func (p *Pipeline) ExportFrom(ctx context.Context, b model.Batch, startIndex int) error {
	return p.processFrom(ctx, b, startIndex)
}

func (p *Pipeline) worker(ctx context.Context) {
	defer p.wg.Done()
	for b := range p.in {
		if err := p.process(ctx, b); err != nil && ctx.Err() == nil {
			p.logger.Error("pipeline processing error", "err", err)
		}
	}
}

func (p *Pipeline) process(ctx context.Context, b model.Batch) error {
	return p.processFrom(ctx, b, 0)
}

func (p *Pipeline) processFrom(ctx context.Context, b model.Batch, startIndex int) error {
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
		if len(b.Spans) == 0 {
			return nil
		}
	}
	for _, e := range p.exporters {
		if err := e.Export(ctx, b); err != nil {
			p.logger.Error("exporter error", "err", err)
		}
	}
	p.spansOut.Add(uint64(len(b.Spans)))
	return nil
}

// Stats returns pipeline counters for observability.
func (p *Pipeline) Stats() (batchesIn, batchesDropped, spansOut uint64) {
	return p.batchesIn.Load(), p.batchesDropped.Load(), p.spansOut.Load()
}
