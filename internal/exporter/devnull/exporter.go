package devnull

import (
	"context"

	"github.com/hnlbs/collector/internal/model"
)

// Exporter discards all batches. Useful for testing and benchmarking.
type Exporter struct{}

func New() *Exporter { return &Exporter{} }

func (e *Exporter) Export(_ context.Context, _ model.Batch) error { return nil }
func (e *Exporter) Close() error                                   { return nil }
