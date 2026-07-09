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
)

// RetryPolicy bounds the CROS log exporter's backoff.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

func (r *RetryPolicy) applyDefaults() {
	if r.MaxAttempts <= 0 {
		r.MaxAttempts = 3
	}
	if r.InitialBackoff <= 0 {
		r.InitialBackoff = 200 * time.Millisecond
	}
	if r.MaxBackoff <= 0 {
		r.MaxBackoff = 5 * time.Second
	}
}

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
	retry.applyDefaults()
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
	backoff := e.retry.InitialBackoff
	var lastErr error
	for attempt := 0; attempt < e.retry.MaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			backoff *= 2
			if backoff > e.retry.MaxBackoff {
				backoff = e.retry.MaxBackoff
			}
		}
		if lastErr = e.post(ctx, body); lastErr == nil {
			return nil
		}
	}
	return lastErr
}

func (e *CROSExporter) post(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("cros logs: request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("cros logs: post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("cros logs: status %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (e *CROSExporter) Close() error { return nil }
