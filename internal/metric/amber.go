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
	"github.com/yaop-labs/coral/internal/reefedge"
	"github.com/yaop-labs/reef/edge"
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
	edge   io.Closer
}

func NewAmberExporter(endpoint string, timeout time.Duration, retry RetryPolicy, options ...edge.ClientConfig) (*AmberExporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("amber metric exporter: endpoint required")
	}
	client, managed, err := exporterHTTPClient(endpoint, timeout, options)
	if err != nil {
		return nil, fmt.Errorf("amber metric exporter transport: %w", err)
	}
	url := strings.TrimRight(endpoint, "/")
	if !strings.HasSuffix(url, "/v1/metrics") {
		url += "/v1/metrics"
	}
	return &AmberExporter{url: url, client: client, retry: retry, edge: managed}, nil
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

func (e *AmberExporter) Close() error {
	if e.edge == nil {
		return nil
	}
	return e.edge.Close()
}

// FathomExporter posts OTLP metric requests to fathom's /v1/metrics endpoint.
type FathomExporter struct {
	url    string
	client *http.Client
	retry  RetryPolicy
	edge   io.Closer
}

func NewFathomExporter(endpoint string, timeout time.Duration, retry RetryPolicy, options ...edge.ClientConfig) (*FathomExporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("fathom metric exporter: endpoint required")
	}
	client, managed, err := exporterHTTPClient(endpoint, timeout, options)
	if err != nil {
		return nil, fmt.Errorf("fathom metric exporter transport: %w", err)
	}
	url := strings.TrimRight(endpoint, "/")
	if !strings.HasSuffix(url, "/v1/metrics") {
		url += "/v1/metrics"
	}
	return &FathomExporter{url: url, client: client, retry: retry, edge: managed}, nil
}

func (e *FathomExporter) Export(ctx context.Context, b Batch) error {
	if b.Empty() {
		return nil
	}
	req := &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: b.ResourceMetrics}
	body, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("fathom metrics: marshal: %w", err)
	}
	return e.retry.Do(ctx, func(ctx context.Context) error {
		return post(ctx, e.client, e.url, "fathom metrics", body)
	})
}

func (e *FathomExporter) Close() error {
	if e.edge == nil {
		return nil
	}
	return e.edge.Close()
}

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
	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if readErr != nil {
		return fmt.Errorf("%s: read response: %w", who, readErr)
	}
	if resp.StatusCode >= 300 {
		if len(responseBody) > 256 {
			responseBody = responseBody[:256]
		}
		return backoff.StatusError(resp.StatusCode, resp.Header, who+": "+strings.TrimSpace(string(responseBody)))
	}
	var response colmetricspb.ExportMetricsServiceResponse
	if err := proto.Unmarshal(responseBody, &response); err != nil {
		return backoff.Permanent(fmt.Errorf("%s: invalid OTLP response: %w", who, err))
	}
	if partial := response.GetPartialSuccess(); partial != nil && partial.GetRejectedDataPoints() > 0 {
		return backoff.Permanent(fmt.Errorf(
			"%s: partial success rejected_data_points=%d: %s",
			who, partial.GetRejectedDataPoints(), partial.GetErrorMessage(),
		))
	}
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
