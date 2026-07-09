// Package logs is coral's logs pipeline: it receives OTLP logs from agents and
// forwards them without lossy conversion.
package logs

import (
	"context"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

// Batch is a set of OTLP ResourceLogs flowing through the pipeline.
type Batch struct {
	ResourceLogs []*logspb.ResourceLogs
}

func (b Batch) Empty() bool { return len(b.ResourceLogs) == 0 }

// Records counts log records across the batch.
func (b Batch) Records() int {
	n := 0
	for _, rl := range b.ResourceLogs {
		for _, sl := range rl.GetScopeLogs() {
			n += len(sl.GetLogRecords())
		}
	}
	return n
}

// Receiver generates Batches and pushes them via emit.
type Receiver interface {
	Start(ctx context.Context, emit func(context.Context, Batch) error) error
	Stop(ctx context.Context) error
}

// Exporter ships a Batch onward.
type Exporter interface {
	Export(ctx context.Context, b Batch) error
	Close() error
}

// Pipeline moves log batches from receivers to exporters.
type Pipeline struct {
	workers   int
	receivers []Receiver
	exporters []Exporter
	logger    *slog.Logger

	in           chan Batch
	wg           sync.WaitGroup
	shutdownOnce sync.Once
	recordsOut   atomic.Uint64
}

func NewPipeline(workers, queueSize int, logger *slog.Logger) *Pipeline {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	if queueSize <= 0 {
		queueSize = 10000
	}
	return &Pipeline{workers: workers, logger: logger, in: make(chan Batch, queueSize)}
}

func (p *Pipeline) AddReceiver(r Receiver) { p.receivers = append(p.receivers, r) }
func (p *Pipeline) AddExporter(e Exporter) { p.exporters = append(p.exporters, e) }

func (p *Pipeline) Start(ctx context.Context) error {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}
	emit := func(ctx context.Context, b Batch) error {
		if b.Empty() {
			return nil
		}
		select {
		case p.in <- b:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, r := range p.receivers {
		r := r
		go func() {
			if err := r.Start(ctx, emit); err != nil && ctx.Err() == nil {
				p.logger.Error("log receiver exited with error", "err", err)
			}
		}()
	}
	return nil
}

func (p *Pipeline) Shutdown(ctx context.Context) error {
	var err error
	p.shutdownOnce.Do(func() {
		for _, r := range p.receivers {
			if e := r.Stop(ctx); e != nil {
				p.logger.Error("log receiver stop error", "err", e)
			}
		}
		close(p.in)
		p.wg.Wait()
		for _, e := range p.exporters {
			if ce := e.Close(); ce != nil {
				p.logger.Error("log exporter close error", "err", ce)
				err = ce
			}
		}
	})
	return err
}

func (p *Pipeline) worker(ctx context.Context) {
	defer p.wg.Done()
	for b := range p.in {
		for _, e := range p.exporters {
			if err := e.Export(ctx, b); err != nil && ctx.Err() == nil {
				p.logger.Error("log exporter error", "err", err)
			}
		}
		p.recordsOut.Add(uint64(b.Records()))
	}
}

// RecordsOut returns the count of log records exported.
func (p *Pipeline) RecordsOut() uint64 { return p.recordsOut.Load() }
