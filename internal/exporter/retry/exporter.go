package retry

import (
	"context"
	"time"

	"github.com/yaop-labs/coral/internal/exporter/backoff"
	"github.com/yaop-labs/coral/internal/model"
	"github.com/yaop-labs/coral/internal/pipeline"
)

type Config struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type Exporter struct {
	inner  pipeline.Exporter[model.Batch]
	policy backoff.Policy
}

// Wrap adds retries to inner. The inner exporter classifies its failures
// (backoff.Permanent / backoff.StatusError); Wrap only drives the backoff, so
// permanent errors such as 4xx are surfaced without wasted retries.
func Wrap(inner pipeline.Exporter[model.Batch], cfg Config) pipeline.Exporter[model.Batch] {
	// One explicitly means a single attempt. Zero is unset and is resolved by
	// backoff.Policy to the same three-attempt default used by metrics and logs.
	if cfg.MaxAttempts == 1 {
		return inner
	}
	return &Exporter{inner: inner, policy: backoff.Policy{
		MaxAttempts:    cfg.MaxAttempts,
		InitialBackoff: cfg.InitialBackoff,
		MaxBackoff:     cfg.MaxBackoff,
	}}
}

func (e *Exporter) Export(ctx context.Context, b model.Batch) error {
	return e.policy.Do(ctx, func(ctx context.Context) error {
		return e.inner.Export(ctx, b)
	})
}

func (e *Exporter) Close() error {
	return e.inner.Close()
}
