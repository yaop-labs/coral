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

// AmberExporter posts OTLP log requests to amber's /v1/logs endpoint. amber is
// the platform source of truth; logs must reach it (contract §1), so this runs
// alongside the CROS fan-out rather than instead of it.
type AmberExporter struct {
	url    string
	client *http.Client
	retry  RetryPolicy
}

func NewAmberExporter(endpoint string, timeout time.Duration, retry RetryPolicy) (*AmberExporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("amber log exporter: endpoint required")
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	retry.applyDefaults()
	url := strings.TrimRight(endpoint, "/")
	if !strings.HasSuffix(url, "/v1/logs") {
		url += "/v1/logs"
	}
	return &AmberExporter{url: url, client: &http.Client{Timeout: timeout}, retry: retry}, nil
}

func (e *AmberExporter) Export(ctx context.Context, b Batch) error {
	if b.Empty() {
		return nil
	}
	req := &collogspb.ExportLogsServiceRequest{ResourceLogs: b.ResourceLogs}
	body, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("amber logs: marshal: %w", err)
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

func (e *AmberExporter) post(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("amber logs: request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("amber logs: post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("amber logs: status %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (e *AmberExporter) Close() error { return nil }
