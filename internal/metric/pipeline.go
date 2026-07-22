// Package metric is coral's metrics path: it receives OTLP metrics from agents
// (wisp), enriches them, and forwards them to amber. It shares the generic
// worker-pool in internal/pipeline with the trace and log paths and operates
// directly on OTLP ResourceMetrics — a lossless passthrough that only adds
// resource attributes, never re-encodes samples.
package metric

import (
	"log/slog"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"

	"github.com/yaop-labs/coral/internal/delivery"
	"github.com/yaop-labs/coral/internal/pipeline"
)

// Batch is a set of OTLP ResourceMetrics flowing through the pipeline.
type Batch struct {
	ResourceMetrics []*metricspb.ResourceMetrics
	RecordID        string
	DeliveryAttempt uint64
	Tenant          string
	JournalUnits    int
}

func (b Batch) DeliveryMetadata() delivery.Metadata {
	if b.RecordID == "" {
		return delivery.Metadata{Tenant: b.Tenant}
	}
	units := b.JournalUnits
	if units <= 0 {
		units = b.Len()
	}
	return delivery.Metadata{Tenant: b.Tenant, Records: []delivery.RecordContribution{{RecordID: b.RecordID, Attempt: b.DeliveryAttempt, Units: units}}}
}

func (b Batch) Empty() bool { return b.Len() == 0 }

// Len reports the number of data points across the batch, satisfying
// pipeline.Signal. It also drives the exported-points counter.
func (b Batch) Len() int {
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

func (b Batch) SizeBytes() int {
	n := len(b.RecordID) + len(b.Tenant) + 16
	for _, rm := range b.ResourceMetrics {
		n += proto.Size(rm)
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

// Pipeline, Receiver, Processor, and Exporter are the metric-signal
// instantiations of the generic pipeline types.
type (
	Pipeline  = pipeline.Pipeline[Batch]
	Receiver  = pipeline.Receiver[Batch]
	Processor = pipeline.Processor[Batch]
	Exporter  = pipeline.Exporter[Batch]
)

// NewPipeline builds a metric pipeline over the shared generic worker-pool.
func NewPipeline(workers, queueSize int, logger *slog.Logger, queueBytes ...int64) *Pipeline {
	var bytes int64
	if len(queueBytes) > 0 {
		bytes = queueBytes[0]
	}
	return pipeline.New[Batch](pipeline.Config{Workers: workers, QueueSize: queueSize, QueueBytes: bytes}, logger)
}
