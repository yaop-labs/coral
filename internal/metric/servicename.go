package metric

import (
	"context"

	"github.com/yaop-labs/coral/internal/otlpresource"
)

// ServiceNameProcessor guarantees every ResourceMetrics carries a service.name
// (contract §6). It runs first in the metric pipeline so enrichment and export
// downstream always see a labeled resource.
type ServiceNameProcessor struct{}

func NewServiceNameProcessor() *ServiceNameProcessor { return &ServiceNameProcessor{} }

func (p *ServiceNameProcessor) Process(_ context.Context, b Batch) (Batch, error) {
	for _, rm := range b.ResourceMetrics {
		rm.Resource = otlpresource.EnsureServiceName(rm.GetResource())
	}
	return b, nil
}

func (p *ServiceNameProcessor) Close() error { return nil }
