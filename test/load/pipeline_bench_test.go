package load

import (
	"context"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/hnlbs/collector/internal/model"
	"github.com/hnlbs/collector/internal/pipeline"
	"github.com/hnlbs/collector/internal/processor/sampling"
)

// devnullExporter discards all spans.
type devnullExporter struct{}

func (devnullExporter) Export(_ context.Context, _ model.Batch) error { return nil }
func (devnullExporter) Close() error                                  { return nil }

// benchReceiver holds the pipeline's emit closure for direct injection.
type benchReceiver struct {
	mu    sync.Mutex
	emit  func(context.Context, model.Batch) error
	ready chan struct{}
}

func newBenchReceiver() *benchReceiver {
	return &benchReceiver{ready: make(chan struct{})}
}

func (r *benchReceiver) Start(ctx context.Context, emit func(context.Context, model.Batch) error) error {
	r.mu.Lock()
	r.emit = emit
	r.mu.Unlock()
	close(r.ready)
	<-ctx.Done()
	return nil
}

func (r *benchReceiver) Stop(_ context.Context) error { return nil }

func (r *benchReceiver) Send(ctx context.Context, b model.Batch) error {
	<-r.ready
	r.mu.Lock()
	fn := r.emit
	r.mu.Unlock()
	return fn(ctx, b)
}

// startBenchPipeline wires a pipeline with a benchReceiver and devnull exporter.
// The caller must cancel the context and call Shutdown.
func startBenchPipeline(b *testing.B, procs ...pipeline.Processor) (*pipeline.Pipeline, *benchReceiver, context.CancelFunc) {
	b.Helper()
	recv := newBenchReceiver()
	p := pipeline.New(pipeline.Config{
		Workers:   runtime.NumCPU(),
		QueueSize: 10_000,
	}, slog.Default())
	p.AddReceiver(recv)
	for _, pr := range procs {
		p.AddProcessor(pr)
	}
	p.AddExporter(devnullExporter{})
	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Start(ctx); err != nil {
		cancel()
		b.Fatalf("pipeline.Start: %v", err)
	}
	<-recv.ready
	return p, recv, cancel
}

// BenchmarkPipeline_Throughput measures single-span batch throughput from a
// fakeReceiver through a pipeline with no processors to a devnull exporter.
func BenchmarkPipeline_Throughput(b *testing.B) {
	p, recv, cancel := startBenchPipeline(b)
	defer func() {
		cancel()
		p.Shutdown(context.Background())
	}()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sp := model.Span{
			TraceID: model.TraceID{byte(i), byte(i >> 8)},
			SpanID:  model.SpanID{byte(i)},
			Name:    "bench",
		}
		recv.Send(ctx, model.Batch{Spans: []model.Span{sp}}) //nolint
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "spans/s")
}

// BenchmarkPipeline_BatchedThroughput measures throughput when spans are sent
// in batches of 100.
func BenchmarkPipeline_BatchedThroughput(b *testing.B) {
	const batchSize = 100
	p, recv, cancel := startBenchPipeline(b)
	defer func() {
		cancel()
		p.Shutdown(context.Background())
	}()

	batch := make([]model.Span, batchSize)
	for i := range batch {
		batch[i] = model.Span{
			TraceID: model.TraceID{byte(i)},
			SpanID:  model.SpanID{byte(i)},
			Name:    "bench",
		}
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		recv.Send(ctx, model.Batch{Spans: batch}) //nolint
	}
	b.StopTimer()
	total := b.N * batchSize
	b.ReportMetric(float64(total)/b.Elapsed().Seconds(), "spans/s")
}

// BenchmarkTailSampler_Process measures the tail sampler's ingestion
// throughput: b.N calls to Process, each with 10 spans on distinct trace IDs.
func BenchmarkTailSampler_Process(b *testing.B) {
	export := func(_ context.Context, _ model.Batch) error { return nil }
	ts := sampling.NewTail(30*time.Second, 100_000, 0.5, nil, export)
	ctx := context.Background()

	const spansPerCall = 10
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		spans := make([]model.Span, spansPerCall)
		for j := range spans {
			spans[j] = model.Span{
				TraceID:   model.TraceID{byte(i), byte(i >> 8), byte(i >> 16), byte(j)},
				SpanID:    model.SpanID{byte(j)},
				StartTime: now,
			}
		}
		ts.Process(ctx, model.Batch{Spans: spans}) //nolint
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*spansPerCall)/b.Elapsed().Seconds(), "spans/s")
}
