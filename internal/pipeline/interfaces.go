package pipeline

import (
	"context"

	"github.com/hnlbs/collector/internal/model"
)

// Receiver generates Batches and pushes them into the pipeline via emit.
// Start blocks until ctx is canceled or a fatal error occurs.
// Stop stops the receiver.
type Receiver interface {
	Start(ctx context.Context, emit func(context.Context, model.Batch) error) error
	Stop(ctx context.Context) error
}

// Processor transforms or filters a Batch.
// Process returns an empty Batch to drop all spans.
// Close releases processor resources.
type Processor interface {
	Process(ctx context.Context, b model.Batch) (model.Batch, error)
	Close() error
}

// Exporter receives Batches that have passed the full processor chain.
// Export may be called concurrently.
// Close waits for in-flight exports.
type Exporter interface {
	Export(ctx context.Context, b model.Batch) error
	Close() error
}
