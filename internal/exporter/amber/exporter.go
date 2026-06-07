package amber

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/hnlbs/collector/internal/model"
)

// Exporter sends batches to the Amber HTTP API at POST /api/v1/batch.
type Exporter struct {
	endpoint string
	client   *http.Client
}

func New(endpoint string, timeout time.Duration) (*Exporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("amber exporter: endpoint required")
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Exporter{
		endpoint: endpoint,
		client:   &http.Client{Timeout: timeout},
	}, nil
}

func (e *Exporter) Export(ctx context.Context, b model.Batch) error {
	payload := toPayload(b)
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("amber: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint+"/api/v1/batch", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("amber: request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("amber: post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("amber: unexpected status %d", resp.StatusCode)
	}
	return nil
}

func (e *Exporter) Close() error { return nil }

type payload struct {
	Spans []spanPayload `json:"spans,omitempty"`
}

type spanPayload struct {
	TraceID      string         `json:"trace_id"`
	SpanID       string         `json:"span_id"`
	ParentSpanID string         `json:"parent_span_id,omitempty"`
	Service      string         `json:"service"`
	Name         string         `json:"name"`
	Kind         string         `json:"kind"`
	StartUS      int64          `json:"start_us"`
	DurationUS   int64          `json:"duration_us"`
	Status       string         `json:"status"`
	StatusMsg    string         `json:"status_msg,omitempty"`
	Attrs        map[string]any `json:"attrs,omitempty"`
}

func toPayload(b model.Batch) payload {
	spans := make([]spanPayload, 0, len(b.Spans))
	for _, s := range b.Spans {
		sp := spanPayload{
			TraceID:    s.TraceID.String(),
			SpanID:     s.SpanID.String(),
			Service:    s.Resource.ServiceName(),
			Name:       s.Name,
			Kind:       s.Kind.String(),
			StartUS:    s.StartTime.UnixMicro(),
			DurationUS: s.Duration().Microseconds(),
			Status:     s.Status.String(),
			StatusMsg:  s.StatusMsg,
		}
		if !s.ParentSpanID.IsZero() {
			sp.ParentSpanID = s.ParentSpanID.String()
		}
		if len(s.Attrs) > 0 {
			sp.Attrs = make(map[string]any, len(s.Attrs))
			for _, a := range s.Attrs {
				sp.Attrs[a.Key] = a.Value.Interface()
			}
		}
		spans = append(spans, sp)
	}
	return payload{Spans: spans}
}
