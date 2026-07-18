// Package logs is coral's logs path: it receives OTLP logs from agents,
// optionally redacts them, and forwards them without lossy conversion. It
// shares the generic worker-pool in internal/pipeline with the trace and metric
// paths.
package logs

import (
	"log/slog"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"

	"github.com/yaop-labs/coral/internal/pipeline"
)

// Batch is a set of OTLP ResourceLogs flowing through the pipeline.
type Batch struct {
	ResourceLogs []*logspb.ResourceLogs
}

func (b Batch) Empty() bool { return b.Len() == 0 }

// Len reports the number of log records across the batch, satisfying
// pipeline.Signal. It also drives the exported-records counter.
func (b Batch) Len() int {
	n := 0
	for _, rl := range b.ResourceLogs {
		for _, sl := range rl.GetScopeLogs() {
			n += len(sl.GetLogRecords())
		}
	}
	return n
}

func (b Batch) SizeBytes() int {
	n := 0
	for _, rl := range b.ResourceLogs {
		n += proto.Size(rl)
	}
	return n
}

// Pipeline, Receiver, Processor, and Exporter are the log-signal
// instantiations of the generic pipeline types.
type (
	Pipeline  = pipeline.Pipeline[Batch]
	Receiver  = pipeline.Receiver[Batch]
	Processor = pipeline.Processor[Batch]
	Exporter  = pipeline.Exporter[Batch]
)

// NewPipeline builds a log pipeline over the shared generic worker-pool.
func NewPipeline(workers, queueSize int, logger *slog.Logger, queueBytes ...int64) *Pipeline {
	var bytes int64
	if len(queueBytes) > 0 {
		bytes = queueBytes[0]
	}
	return pipeline.New[Batch](pipeline.Config{Workers: workers, QueueSize: queueSize, QueueBytes: bytes}, logger)
}
