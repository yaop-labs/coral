package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/yaop-labs/coral/internal/model"
)

// Config configures the S3 exporter.
type Config struct {
	Bucket string
	Region string
	Prefix string
	Format string
}

// Exporter writes span batches to S3 as gzipped JSON Lines.
// Each Export call writes one object.
type Exporter struct {
	cfg    Config
	client *s3.Client
}

// New validates cfg and returns an Exporter. Call Init(ctx) before Export.
func New(cfg Config) (*Exporter, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 exporter: bucket required")
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("s3 exporter: region required")
	}
	if cfg.Format == "" {
		cfg.Format = "jsonl"
	}
	return &Exporter{cfg: cfg}, nil
}

// Init loads AWS credentials and wires the S3 client. Must be called once
// before Export, with the application context so credential refresh respects
// shutdown.
func (e *Exporter) Init(ctx context.Context) error {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(e.cfg.Region))
	if err != nil {
		return fmt.Errorf("s3 exporter: aws config: %w", err)
	}
	e.client = s3.NewFromConfig(awsCfg)
	return nil
}

func (e *Exporter) Export(ctx context.Context, b model.Batch) error {
	if len(b.Spans) == 0 {
		return nil
	}

	body, err := e.encode(b)
	if err != nil {
		return fmt.Errorf("s3: encode: %w", err)
	}

	key := e.objectKey(b)
	_, err = e.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:          aws.String(e.cfg.Bucket),
		Key:             aws.String(key),
		Body:            bytes.NewReader(body),
		ContentEncoding: aws.String("gzip"),
		ContentType:     aws.String("application/x-ndjson"),
	})
	if err != nil {
		return fmt.Errorf("s3: put object: %w", err)
	}
	return nil
}

func (e *Exporter) Close() error { return nil }

func (e *Exporter) encode(b model.Batch) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	enc := json.NewEncoder(gz)
	for _, s := range b.Spans {
		if err := enc.Encode(toLine(s)); err != nil {
			return nil, err
		}
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (e *Exporter) objectKey(b model.Batch) string {
	now := time.Now().UTC()
	prefix := e.cfg.Prefix
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	firstID := ""
	if len(b.Spans) > 0 {
		firstID = b.Spans[0].TraceID.String()[:8]
	}
	return fmt.Sprintf("%s%s/%02d/%s-%d.jsonl.gz",
		prefix,
		now.Format("2006-01-02"),
		now.Hour(),
		firstID,
		now.UnixNano(),
	)
}

// spanLine is the JSON Lines wire format written to S3.
type spanLine struct {
	TraceID    string         `json:"trace_id"`
	SpanID     string         `json:"span_id"`
	ParentID   string         `json:"parent_span_id,omitempty"`
	Service    string         `json:"service"`
	Name       string         `json:"name"`
	Kind       string         `json:"kind"`
	StartUS    int64          `json:"start_us"`
	DurationUS int64          `json:"duration_us"`
	Status     string         `json:"status"`
	Attrs      map[string]any `json:"attrs,omitempty"`
}

func toLine(s model.Span) spanLine {
	l := spanLine{
		TraceID:    s.TraceID.String(),
		SpanID:     s.SpanID.String(),
		Service:    s.Resource.ServiceName(),
		Name:       s.Name,
		Kind:       s.Kind.String(),
		StartUS:    s.StartTime.UnixMicro(),
		DurationUS: s.Duration().Microseconds(),
		Status:     s.Status.String(),
	}
	if !s.ParentSpanID.IsZero() {
		l.ParentID = s.ParentSpanID.String()
	}
	if len(s.Attrs) > 0 {
		l.Attrs = make(map[string]any, len(s.Attrs))
		for _, a := range s.Attrs {
			l.Attrs[a.Key] = a.Value.Interface()
		}
	}
	return l
}
