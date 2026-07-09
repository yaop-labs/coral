package logs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"

	"github.com/yaop-labs/coral/internal/exporter/backoff"
)

// RetryPolicy is the shared retry policy; classification and backoff live in
// the backoff package so every signal retries identically (contract §4).
type RetryPolicy = backoff.Policy

// CROSExporter posts OTLP log requests to CROS's /v1/logs endpoint.
type CROSExporter struct {
	url    string
	client *http.Client
	retry  RetryPolicy
}

func NewCROSExporter(endpoint string, timeout time.Duration, retry RetryPolicy) (*CROSExporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("cros log exporter: endpoint required")
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	url := strings.TrimRight(endpoint, "/")
	if !strings.HasSuffix(url, "/v1/logs") {
		url += "/v1/logs"
	}
	return &CROSExporter{url: url, client: &http.Client{Timeout: timeout}, retry: retry}, nil
}

func (e *CROSExporter) Export(ctx context.Context, b Batch) error {
	if b.Empty() {
		return nil
	}
	req := &collogspb.ExportLogsServiceRequest{ResourceLogs: b.ResourceLogs}
	body, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("cros logs: marshal: %w", err)
	}
	return e.retry.Do(ctx, func(ctx context.Context) error {
		return post(ctx, e.client, e.url, "cros logs", body)
	})
}

func (e *CROSExporter) Close() error { return nil }

// post sends one OTLP/protobuf log request and classifies the outcome per §4.
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
