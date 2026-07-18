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
	"github.com/yaop-labs/coral/internal/reefedge"
	"github.com/yaop-labs/reef/edge"
)

// Exporter posts OTLP trace requests to amber's /v1/traces endpoint.
type Exporter struct {
	url    string
	client *http.Client
	edge   io.Closer
}

func New(endpoint string, timeout time.Duration, options ...edge.ClientConfig) (*Exporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("amber exporter: endpoint required")
	}
	var clientCfg edge.ClientConfig
	if len(options) > 0 {
		clientCfg = options[0]
	}
	clientCfg.Target = endpoint
	client, managed, err := newHTTPClient(timeout, clientCfg)
	if err != nil {
		return nil, fmt.Errorf("amber exporter transport: %w", err)
	}
	url := strings.TrimRight(endpoint, "/")
	if !strings.HasSuffix(url, "/v1/traces") {
		url += "/v1/traces"
	}
	return &Exporter{url: url, client: client, edge: managed}, nil
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

func newHTTPClient(timeout time.Duration, cfg edge.ClientConfig) (*http.Client, io.Closer, error) {
	return reefedge.NewHTTPClient(timeout, cfg, nil)
}

func (e *Exporter) Close() error {
	if e.edge == nil {
		return nil
	}
	return e.edge.Close()
}
