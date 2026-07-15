package metric

import (
	"context"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/yaop-labs/coral/internal/otlpredact"
)

// RedactProcessor scrubs credential-bearing values from metric attributes —
// resource, scope, and each data point — the metric-signal counterpart of the
// trace validate processor's redaction (contract §8). It never re-encodes
// samples.
type RedactProcessor struct {
	r *otlpredact.Redactor
}

func NewRedactProcessor(patterns []string) (*RedactProcessor, error) {
	red, err := otlpredact.New(patterns)
	if err != nil {
		return nil, err
	}
	return &RedactProcessor{r: red}, nil
}

func (p *RedactProcessor) Process(_ context.Context, b Batch) (Batch, error) {
	if !p.r.Enabled() {
		return b, nil
	}
	for _, rm := range b.ResourceMetrics {
		if rm.GetResource() != nil {
			p.r.RedactKeyValues(rm.Resource.Attributes)
		}
		for _, sm := range rm.GetScopeMetrics() {
			if sm.GetScope() != nil {
				p.r.RedactKeyValues(sm.Scope.Attributes)
			}
			for _, m := range sm.GetMetrics() {
				p.redactDataPoints(m)
			}
		}
	}
	return b, nil
}

func (p *RedactProcessor) Close() error { return nil }

func (p *RedactProcessor) redactDataPoints(m *metricspb.Metric) {
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		for _, dp := range d.Gauge.GetDataPoints() {
			p.r.RedactKeyValues(dp.Attributes)
		}
	case *metricspb.Metric_Sum:
		for _, dp := range d.Sum.GetDataPoints() {
			p.r.RedactKeyValues(dp.Attributes)
		}
	case *metricspb.Metric_Histogram:
		for _, dp := range d.Histogram.GetDataPoints() {
			p.r.RedactKeyValues(dp.Attributes)
		}
	case *metricspb.Metric_ExponentialHistogram:
		for _, dp := range d.ExponentialHistogram.GetDataPoints() {
			p.r.RedactKeyValues(dp.Attributes)
		}
	case *metricspb.Metric_Summary:
		for _, dp := range d.Summary.GetDataPoints() {
			p.r.RedactKeyValues(dp.Attributes)
		}
	}
}
