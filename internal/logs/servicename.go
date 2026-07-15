package logs

import (
	"context"

	"github.com/yaop-labs/coral/internal/otlpresource"
)

// ServiceNameProcessor guarantees every ResourceLogs carries a service.name
// (contract §6). It runs first in the log pipeline so redaction and export
// downstream always see a labeled resource.
type ServiceNameProcessor struct{}

func NewServiceNameProcessor() *ServiceNameProcessor { return &ServiceNameProcessor{} }

func (p *ServiceNameProcessor) Process(_ context.Context, b Batch) (Batch, error) {
	for _, rl := range b.ResourceLogs {
		rl.Resource = otlpresource.EnsureServiceName(rl.GetResource())
	}
	return b, nil
}

func (p *ServiceNameProcessor) Close() error { return nil }
