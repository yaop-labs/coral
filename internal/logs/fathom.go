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
	"github.com/yaop-labs/coral/internal/reefedge"
	"github.com/yaop-labs/reef/edge"
)

// RetryPolicy is the shared retry policy; classification and backoff live in
// the backoff package so every signal retries identically (contract §4).
type RetryPolicy = backoff.Policy

// FathomExporter posts OTLP log requests to fathom's /v1/logs endpoint.
type FathomExporter struct {
	url    string
	client *http.Client
	retry  RetryPolicy
	edge   io.Closer
}

func NewFathomExporter(endpoint string, timeout time.Duration, retry RetryPolicy, options ...edge.ClientConfig) (*FathomExporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("fathom log exporter: endpoint required")
	}
	client, managed, err := exporterHTTPClient(endpoint, timeout, options)
	if err != nil {
		return nil, fmt.Errorf("fathom log exporter transport: %w", err)
	}
	url := strings.TrimRight(endpoint, "/")
	if !strings.HasSuffix(url, "/v1/logs") {
		url += "/v1/logs"
	}
	return &FathomExporter{url: url, client: client, retry: retry, edge: managed}, nil
}

func (e *FathomExporter) Export(ctx context.Context, b Batch) error {
	if b.Empty() {
		return nil
	}
	req := &collogspb.ExportLogsServiceRequest{ResourceLogs: b.ResourceLogs}
	body, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("fathom logs: marshal: %w", err)
	}
	return e.retry.Do(ctx, func(ctx context.Context) error {
		return post(ctx, e.client, e.url, "fathom logs", body)
	})
}

func (e *FathomExporter) Close() error {
	if e.edge == nil {
		return nil
	}
	return e.edge.Close()
}

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

func exporterHTTPClient(endpoint string, timeout time.Duration, options []edge.ClientConfig) (*http.Client, io.Closer, error) {
	var cfg edge.ClientConfig
	if len(options) > 0 {
		cfg = options[0]
	}
	cfg.Target = endpoint
	return reefedge.NewHTTPClient(timeout, cfg, nil)
}
