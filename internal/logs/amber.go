package logs

import (
	"context"
	"fmt"
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
	return e.retry.Do(ctx, func(ctx context.Context) error {
		return post(ctx, e.client, e.url, "amber logs", body)
	})
}

func (e *AmberExporter) Close() error { return nil }
