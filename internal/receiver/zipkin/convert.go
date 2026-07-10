package zipkin

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/yaop-labs/coral/internal/model"
)

// zipkinSpan is the Zipkin v2 JSON wire format.
// https://zipkin.io/zipkin-api/#/default/post_spans
type zipkinSpan struct {
	TraceID        string            `json:"traceId"`
	ID             string            `json:"id"`
	ParentID       string            `json:"parentId,omitempty"`
	Name           string            `json:"name"`
	Kind           string            `json:"kind"`      // CLIENT, SERVER, PRODUCER, CONSUMER
	Timestamp      int64             `json:"timestamp"` // microseconds since epoch
	Duration       int64             `json:"duration"`  // microseconds
	LocalEndpoint  *zipkinEndpoint   `json:"localEndpoint,omitempty"`
	RemoteEndpoint *zipkinEndpoint   `json:"remoteEndpoint,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
	Debug          bool              `json:"debug,omitempty"`
	Shared         bool              `json:"shared,omitempty"`
}

type zipkinEndpoint struct {
	ServiceName string `json:"serviceName,omitempty"`
	IPv4        string `json:"ipv4,omitempty"`
	IPv6        string `json:"ipv6,omitempty"`
	Port        int    `json:"port,omitempty"`
}

func decodeSpans(body []byte) ([]model.Span, error) {
	var zspans []zipkinSpan
	if err := json.Unmarshal(body, &zspans); err != nil {
		return nil, fmt.Errorf("zipkin json: %w", err)
	}
	out := make([]model.Span, 0, len(zspans))
	for _, zs := range zspans {
		s, err := fromZipkin(zs)
		if err != nil {
			continue // skip malformed individual spans
		}
		out = append(out, s)
	}
	return out, nil
}

func fromZipkin(zs zipkinSpan) (model.Span, error) {
	traceID, err := parseHex16(zs.TraceID)
	if err != nil {
		return model.Span{}, fmt.Errorf("trace_id: %w", err)
	}
	spanID, err := parseHex8(zs.ID)
	if err != nil {
		return model.Span{}, fmt.Errorf("span_id: %w", err)
	}

	// timestamp is optional in the Zipkin model, but coral needs a real start
	// time: a zero value would silently become 1970. Reject such spans instead.
	if zs.Timestamp == 0 {
		return model.Span{}, fmt.Errorf("timestamp: required")
	}

	s := model.Span{
		TraceID:   traceID,
		SpanID:    spanID,
		Name:      zs.Name,
		Kind:      kindFromZipkin(zs.Kind),
		StartTime: time.UnixMicro(zs.Timestamp),
		EndTime:   time.UnixMicro(zs.Timestamp + zs.Duration),
	}

	if zs.ParentID != "" {
		pid, err := parseHex8(zs.ParentID)
		if err == nil {
			s.ParentSpanID = pid
		}
	}

	if zs.LocalEndpoint != nil && zs.LocalEndpoint.ServiceName != "" {
		s.Resource = model.Resource{
			Attrs: []model.Attribute{model.StringAttr("service.name", zs.LocalEndpoint.ServiceName)},
		}
	}

	for k, v := range zs.Tags {
		s.Attrs = append(s.Attrs, model.StringAttr(k, v))
	}

	if zs.Tags["error"] != "" {
		s.Status = model.StatusError
		s.StatusMsg = zs.Tags["error"]
	}

	if zs.Debug {
		s.Attrs = append(s.Attrs, model.StringAttr("debug", "true"))
	}

	return s, nil
}

func kindFromZipkin(kind string) model.SpanKind {
	switch kind {
	case "CLIENT":
		return model.KindClient
	case "SERVER":
		return model.KindServer
	case "PRODUCER":
		return model.KindProducer
	case "CONSUMER":
		return model.KindConsumer
	default:
		return model.KindUnspecified
	}
}

// parseHex16 parses a 32-hex-char trace ID into [16]byte.
func parseHex16(s string) (model.TraceID, error) {
	// Allow 16-char (64-bit) or 32-char (128-bit) trace IDs.
	var id model.TraceID
	if len(s) == 16 {
		// 64-bit: pad left with zeros (put in low 8 bytes).
		b, err := hexDecode(s, id[8:])
		if err != nil {
			return id, err
		}
		_ = b
		return id, nil
	}
	if len(s) != 32 {
		return id, fmt.Errorf("invalid trace ID length %d", len(s))
	}
	if _, err := hexDecode(s, id[:]); err != nil {
		return id, err
	}
	return id, nil
}

// parseHex8 parses a 16-hex-char span ID into [8]byte.
func parseHex8(s string) (model.SpanID, error) {
	var id model.SpanID
	if len(s) != 16 {
		return id, fmt.Errorf("invalid span ID length %d", len(s))
	}
	if _, err := hexDecode(s, id[:]); err != nil {
		return id, err
	}
	return id, nil
}

func hexDecode(s string, dst []byte) (int, error) {
	if len(s) != len(dst)*2 {
		return 0, fmt.Errorf("hex length mismatch: %d vs %d", len(s), len(dst)*2)
	}
	for i := range dst {
		hi := hexVal(s[i*2])
		lo := hexVal(s[i*2+1])
		if hi == 255 || lo == 255 {
			return 0, fmt.Errorf("invalid hex char in %q", s)
		}
		dst[i] = hi<<4 | lo
	}
	return len(dst), nil
}

func hexVal(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	default:
		return 255
	}
}
