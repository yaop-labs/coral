package metric

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"

	"github.com/yaop-labs/coral/internal/exporter/backoff"
)

// RetryPolicy is the shared retry policy; classification and backoff live in
// the backoff package so every signal retries identically (contract §4).
type RetryPolicy = backoff.Policy

// AmberExporter posts OTLP metric requests to amber's /v1/metrics endpoint.
// amber ingests metrics over HTTP only, so this is HTTP/protobuf.
type AmberExporter struct {
	url    string
	client *http.Client
	retry  RetryPolicy
}

func NewAmberExporter(endpoint string, timeout time.Duration, retry RetryPolicy) (*AmberExporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("amber metric exporter: endpoint required")
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	url := strings.TrimRight(endpoint, "/")
	if !strings.HasSuffix(url, "/v1/metrics") {
		url += "/v1/metrics"
	}
	return &AmberExporter{url: url, client: &http.Client{Timeout: timeout}, retry: retry}, nil
}

func (e *AmberExporter) Export(ctx context.Context, b Batch) error {
	if b.Empty() {
		return nil
	}
	req := &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: b.ResourceMetrics}
	body, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("amber metrics: marshal: %w", err)
	}
	return e.retry.Do(ctx, func(ctx context.Context) error {
		return post(ctx, e.client, e.url, "amber metrics", body)
	})
}

func (e *AmberExporter) Close() error { return nil }

// CROSExporter posts OTLP metric requests to CROS's /v1/metrics endpoint.
type CROSExporter struct {
	url    string
	client *http.Client
	retry  RetryPolicy
}

func NewCROSExporter(endpoint string, timeout time.Duration, retry RetryPolicy) (*CROSExporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("cros metric exporter: endpoint required")
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	url := strings.TrimRight(endpoint, "/")
	if !strings.HasSuffix(url, "/v1/metrics") {
		url += "/v1/metrics"
	}
	return &CROSExporter{url: url, client: &http.Client{Timeout: timeout}, retry: retry}, nil
}

func (e *CROSExporter) Export(ctx context.Context, b Batch) error {
	if b.Empty() {
		return nil
	}
	req := &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: b.ResourceMetrics}
	body, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("cros metrics: marshal: %w", err)
	}
	return e.retry.Do(ctx, func(ctx context.Context) error {
		return post(ctx, e.client, e.url, "cros metrics", body)
	})
}

func (e *CROSExporter) Close() error { return nil }

// post sends one OTLP/protobuf request and classifies the outcome per §4.
func post(ctx context.Context, client *http.Client, url, who string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return backoff.Permanent(fmt.Errorf("%s: request: %w", who, err))
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: post: %w", who, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return backoff.StatusError(resp.StatusCode, resp.Header, who+": "+strings.TrimSpace(string(snippet)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}
