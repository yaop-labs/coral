// Package metric is coral's metrics pipeline: it receives OTLP metrics from
// agents (wisp), enriches them, and forwards them to amber. It runs parallel to
// the trace pipeline and operates directly on OTLP ResourceMetrics — a lossless
// passthrough that only adds resource attributes, never re-encodes samples.
package metric

import (
	"context"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// Batch is a set of OTLP ResourceMetrics flowing through the pipeline.
type Batch struct {
	ResourceMetrics []*metricspb.ResourceMetrics
}

func (b Batch) Empty() bool { return len(b.ResourceMetrics) == 0 }

// Points counts data points across the batch (for stats).
func (b Batch) Points() int {
	n := 0
	for _, rm := range b.ResourceMetrics {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				n += metricPoints(m)
			}
		}
	}
	return n
}

func metricPoints(m *metricspb.Metric) int {
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		return len(d.Gauge.GetDataPoints())
	case *metricspb.Metric_Sum:
		return len(d.Sum.GetDataPoints())
	case *metricspb.Metric_Histogram:
		return len(d.Histogram.GetDataPoints())
	case *metricspb.Metric_ExponentialHistogram:
		return len(d.ExponentialHistogram.GetDataPoints())
	case *metricspb.Metric_Summary:
		return len(d.Summary.GetDataPoints())
	default:
		return 0
	}
}

// Receiver generates Batches and pushes them via emit.
type Receiver interface {
	Start(ctx context.Context, emit func(context.Context, Batch) error) error
	Stop(ctx context.Context) error
}

// Processor enriches or filters a Batch.
type Processor interface {
	Process(ctx context.Context, b Batch) (Batch, error)
	Close() error
}

// Exporter ships a Batch onward (to amber).
type Exporter interface {
	Export(ctx context.Context, b Batch) error
	Close() error
}

// Pipeline moves metric batches from receivers through processors to exporters.
type Pipeline struct {
	workers    int
	receivers  []Receiver
	processors []Processor
	exporters  []Exporter
	logger     *slog.Logger

	in           chan Batch
	wg           sync.WaitGroup
	shutdownOnce sync.Once
	pointsOut    atomic.Uint64
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

func (p *Pipeline) AddReceiver(r Receiver)    { p.receivers = append(p.receivers, r) }
func (p *Pipeline) AddProcessor(pr Processor) { p.processors = append(p.processors, pr) }
func (p *Pipeline) AddExporter(e Exporter)    { p.exporters = append(p.exporters, e) }

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
				p.logger.Error("metric receiver exited with error", "err", err)
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
				p.logger.Error("metric receiver stop error", "err", e)
			}
		}
		close(p.in)
		p.wg.Wait()
		for i := len(p.processors) - 1; i >= 0; i-- {
			if e := p.processors[i].Close(); e != nil {
				p.logger.Error("metric processor close error", "err", e)
			}
		}
		for _, e := range p.exporters {
			if ce := e.Close(); ce != nil {
				p.logger.Error("metric exporter close error", "err", ce)
				err = ce
			}
		}
	})
	return err
}

func (p *Pipeline) worker(ctx context.Context) {
	defer p.wg.Done()
	for b := range p.in {
		if err := p.process(ctx, b); err != nil && ctx.Err() == nil {
			p.logger.Error("metric pipeline processing error", "err", err)
		}
	}
}

func (p *Pipeline) process(ctx context.Context, b Batch) error {
	var err error
	for _, pr := range p.processors {
		b, err = pr.Process(ctx, b)
		if err != nil {
			return err
		}
		if b.Empty() {
			return nil
		}
	}
	for _, e := range p.exporters {
		if err := e.Export(ctx, b); err != nil {
			p.logger.Error("metric exporter error", "err", err)
		}
	}
	p.pointsOut.Add(uint64(b.Points()))
	return nil
}

// PointsOut returns the count of data points exported.
func (p *Pipeline) PointsOut() uint64 { return p.pointsOut.Load() }
