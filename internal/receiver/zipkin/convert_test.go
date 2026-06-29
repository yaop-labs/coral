package zipkin

import (
	"testing"
	"time"

	"github.com/yaop-labs/coral/internal/model"
)

func TestDecodeSpans_Basic(t *testing.T) {
	body := []byte(`[{
		"traceId": "0102030405060708090a0b0c0d0e0f10",
		"id": "1122334455667788",
		"name": "http.get",
		"kind": "SERVER",
		"timestamp": 1000000,
		"duration": 50000,
		"localEndpoint": {"serviceName": "payments"},
		"tags": {"http.method": "GET"}
	}]`)

	spans, err := decodeSpans(body)
	if err != nil {
		t.Fatalf("decodeSpans: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	s := spans[0]
	if s.Name != "http.get" {
		t.Errorf("name = %q", s.Name)
	}
	if s.Kind != model.KindServer {
		t.Errorf("kind = %v", s.Kind)
	}
	if s.Resource.ServiceName() != "payments" {
		t.Errorf("service = %q", s.Resource.ServiceName())
	}
	if d := s.Duration(); d != 50*time.Millisecond {
		t.Errorf("duration = %v, want 50ms", d)
	}
	if s.AttrValue("http.method") != "GET" {
		t.Errorf("http.method = %q", s.AttrValue("http.method"))
	}
	if !s.IsRoot() {
		t.Error("expected root span (no parentId)")
	}
}

func TestDecodeSpans_WithParent(t *testing.T) {
	body := []byte(`[{
		"traceId": "00000000000000010000000000000001",
		"id": "0000000000000002",
		"parentId": "0000000000000001",
		"name": "child",
		"timestamp": 1000,
		"duration": 500
	}]`)

	spans, err := decodeSpans(body)
	if err != nil {
		t.Fatalf("decodeSpans: %v", err)
	}
	if spans[0].IsRoot() {
		t.Error("expected non-root span")
	}
}

func TestDecodeSpans_ErrorTag(t *testing.T) {
	body := []byte(`[{
		"traceId": "00000000000000000000000000000001",
		"id": "0000000000000001",
		"name": "op",
		"timestamp": 1000,
		"duration": 100,
		"tags": {"error": "connection refused"}
	}]`)

	spans, err := decodeSpans(body)
	if err != nil {
		t.Fatalf("decodeSpans: %v", err)
	}
	if !spans[0].HasError() {
		t.Error("expected error span")
	}
	if spans[0].StatusMsg != "connection refused" {
		t.Errorf("status_msg = %q", spans[0].StatusMsg)
	}
}

func TestDecodeSpans_Debug(t *testing.T) {
	body := []byte(`[{
		"traceId": "00000000000000000000000000000001",
		"id": "0000000000000001",
		"name": "op",
		"timestamp": 1000,
		"duration": 100,
		"debug": true
	}]`)

	spans, err := decodeSpans(body)
	if err != nil {
		t.Fatalf("decodeSpans: %v", err)
	}
	if spans[0].AttrValue("debug") != "true" {
		t.Error("expected debug attribute")
	}
}

func TestDecodeSpans_64BitTraceID(t *testing.T) {
	// parseHex16 accepts 64-bit trace IDs.
	body := []byte(`[{
		"traceId": "0102030405060708",
		"id": "0102030405060709",
		"name": "op",
		"timestamp": 1000,
		"duration": 100
	}]`)

	spans, err := decodeSpans(body)
	if err != nil {
		t.Fatalf("decodeSpans: %v", err)
	}
	// 64-bit trace ID goes into the low 8 bytes.
	want := model.TraceID{0, 0, 0, 0, 0, 0, 0, 0, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	if spans[0].TraceID != want {
		t.Errorf("TraceID = %v, want %v", spans[0].TraceID, want)
	}
}

func TestDecodeSpans_MultipleSpans(t *testing.T) {
	body := []byte(`[
		{"traceId": "00000000000000000000000000000001", "id": "0000000000000001", "name": "a", "timestamp": 1, "duration": 1},
		{"traceId": "00000000000000000000000000000001", "id": "0000000000000002", "name": "b", "timestamp": 2, "duration": 1},
		{"traceId": "00000000000000000000000000000001", "id": "0000000000000003", "name": "c", "timestamp": 3, "duration": 1}
	]`)

	spans, err := decodeSpans(body)
	if err != nil {
		t.Fatalf("decodeSpans: %v", err)
	}
	if len(spans) != 3 {
		t.Fatalf("expected 3 spans, got %d", len(spans))
	}
}

func TestDecodeSpans_InvalidJSON(t *testing.T) {
	_, err := decodeSpans([]byte(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestKindFromZipkin(t *testing.T) {
	cases := []struct {
		in   string
		want model.SpanKind
	}{
		{"CLIENT", model.KindClient},
		{"SERVER", model.KindServer},
		{"PRODUCER", model.KindProducer},
		{"CONSUMER", model.KindConsumer},
		{"", model.KindUnspecified},
		{"UNKNOWN", model.KindUnspecified},
	}
	for _, tc := range cases {
		if got := kindFromZipkin(tc.in); got != tc.want {
			t.Errorf("kindFromZipkin(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestStartTime(t *testing.T) {
	body := []byte(`[{
		"traceId": "00000000000000000000000000000001",
		"id": "0000000000000001",
		"name": "op",
		"timestamp": 1000000,
		"duration": 2000000
	}]`)
	spans, _ := decodeSpans(body)
	s := spans[0]
	wantStart := time.UnixMicro(1000000)
	wantEnd := time.UnixMicro(3000000)
	if !s.StartTime.Equal(wantStart) {
		t.Errorf("start = %v, want %v", s.StartTime, wantStart)
	}
	if !s.EndTime.Equal(wantEnd) {
		t.Errorf("end = %v, want %v", s.EndTime, wantEnd)
	}
}
