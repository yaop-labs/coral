// Package amber sends trace batches to amber over OTLP/HTTP at POST /v1/traces.
//
// Earlier this posted a bespoke JSON payload to /api/v1/batch, a route amber
// does not serve; amber ingests traces only over OTLP. We now speak OTLP.
package amber

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/yaop-labs/coral/internal/exporter/backoff"
	"github.com/yaop-labs/coral/internal/model"
)

// Exporter posts OTLP trace requests to amber's /v1/traces endpoint.
type Exporter struct {
	url    string
	client *http.Client
}

func New(endpoint string, timeout time.Duration) (*Exporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("amber exporter: endpoint required")
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	url := strings.TrimRight(endpoint, "/")
	if !strings.HasSuffix(url, "/v1/traces") {
		url += "/v1/traces"
	}
	return &Exporter{url: url, client: &http.Client{Timeout: timeout}}, nil
}

func (e *Exporter) Export(ctx context.Context, b model.Batch) error {
	req := toTraceRequest(b)
	if len(req.ResourceSpans) == 0 {
		return nil
	}
	body, err := proto.Marshal(req)
	if err != nil {
		return backoff.Permanent(fmt.Errorf("amber: marshal: %w", err))
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return backoff.Permanent(fmt.Errorf("amber: request: %w", err))
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := e.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("amber: post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return backoff.StatusError(resp.StatusCode, resp.Header, "amber: "+strings.TrimSpace(string(snippet)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (e *Exporter) Close() error { return nil }
