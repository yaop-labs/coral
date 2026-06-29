package retry

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yaop-labs/coral/internal/model"
)

type flakyExporter struct {
	mu         sync.Mutex
	failures   int
	attempts   int
	closeCalls int
}

func (e *flakyExporter) Export(_ context.Context, _ model.Batch) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.attempts++
	if e.attempts <= e.failures {
		return errors.New("temporary failure")
	}
	return nil
}

func (e *flakyExporter) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closeCalls++
	return nil
}

func (e *flakyExporter) Attempts() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.attempts
}

func TestWrap_RetriesUntilSuccess(t *testing.T) {
	inner := &flakyExporter{failures: 2}
	exp := Wrap(inner, Config{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
	})

	err := exp.Export(context.Background(), model.Batch{Spans: []model.Span{{Name: "span"}}})
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if inner.Attempts() != 3 {
		t.Fatalf("attempts = %d, want 3", inner.Attempts())
	}
}

func TestWrap_DisabledWhenMaxAttemptsIsOne(t *testing.T) {
	inner := &flakyExporter{}
	exp := Wrap(inner, Config{MaxAttempts: 1})
	if exp != inner {
		t.Fatal("Wrap should return inner exporter when retry is disabled")
	}
}
