package retry

import (
	"context"
	"time"

	"github.com/hnlbs/collector/internal/model"
	"github.com/hnlbs/collector/internal/pipeline"
)

type Config struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type Exporter struct {
	inner pipeline.Exporter
	cfg   Config
}

func Wrap(inner pipeline.Exporter, cfg Config) pipeline.Exporter {
	if cfg.MaxAttempts <= 1 {
		return inner
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 100 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 5 * time.Second
	}
	if cfg.MaxBackoff < cfg.InitialBackoff {
		cfg.MaxBackoff = cfg.InitialBackoff
	}
	return &Exporter{inner: inner, cfg: cfg}
}

func (e *Exporter) Export(ctx context.Context, b model.Batch) error {
	var err error
	backoff := e.cfg.InitialBackoff
	for attempt := 1; attempt <= e.cfg.MaxAttempts; attempt++ {
		err = e.inner.Export(ctx, b)
		if err == nil {
			return nil
		}
		if attempt == e.cfg.MaxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > e.cfg.MaxBackoff {
			backoff = e.cfg.MaxBackoff
		}
	}
	return err
}

func (e *Exporter) Close() error {
	return e.inner.Close()
}
