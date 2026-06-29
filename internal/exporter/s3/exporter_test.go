package s3

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/yaop-labs/coral/internal/model"
)

func TestEncode_JSONLines(t *testing.T) {
	e := &Exporter{cfg: Config{Format: "jsonl"}}

	spans := []model.Span{
		{
			TraceID:   model.TraceID{1},
			SpanID:    model.SpanID{1},
			Name:      "op1",
			Kind:      model.KindServer,
			Status:    model.StatusOK,
			StartTime: time.Unix(0, 1000000),
			EndTime:   time.Unix(0, 2000000),
			Resource:  model.Resource{Attrs: []model.Attribute{model.StringAttr("service.name", "svc")}},
			Attrs:     []model.Attribute{model.StringAttr("db", "pg")},
		},
		{
			TraceID: model.TraceID{2},
			SpanID:  model.SpanID{2},
			Name:    "op2",
			Status:  model.StatusError,
		},
	}

	body, err := e.encode(model.Batch{Spans: spans})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	gr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gzip: %v", err)
	}
	dec := json.NewDecoder(gr)

	var lines []spanLine
	for dec.More() {
		var l spanLine
		if err := dec.Decode(&l); err != nil {
			t.Fatalf("decode: %v", err)
		}
		lines = append(lines, l)
	}

	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	if lines[0].Service != "svc" {
		t.Errorf("service = %q", lines[0].Service)
	}
	if lines[0].Attrs["db"] != "pg" {
		t.Errorf("attrs[db] = %q", lines[0].Attrs["db"])
	}
	if lines[1].Status != "error" {
		t.Errorf("status = %q", lines[1].Status)
	}
	if lines[0].DurationUS != 1000 {
		t.Errorf("duration_us = %d, want 1000", lines[0].DurationUS)
	}
}

func TestToLine_NoParent(t *testing.T) {
	s := model.Span{TraceID: model.TraceID{1}, SpanID: model.SpanID{1}}
	l := toLine(s)
	if l.ParentID != "" {
		t.Errorf("root span should have empty parent_span_id")
	}
}

func TestObjectKey_Format(t *testing.T) {
	e := &Exporter{cfg: Config{Prefix: "traces/", Format: "jsonl"}}
	b := model.Batch{Spans: []model.Span{{TraceID: model.TraceID{0xab, 0xcd}}}}
	key := e.objectKey(b)
	if key == "" {
		t.Error("object key should not be empty")
	}
	if len(key) < 7 || key[:7] != "traces/" {
		t.Errorf("key should start with prefix, got %q", key)
	}
}

func TestObjectKey_EmptyBatch(t *testing.T) {
	e := &Exporter{cfg: Config{Prefix: "p", Format: "jsonl"}}
	key := e.objectKey(model.Batch{})
	if key == "" {
		t.Error("key should not be empty even for empty batch")
	}
}

func TestNew_MissingBucket(t *testing.T) {
	_, err := New(Config{Region: "us-east-1"})
	if err == nil {
		t.Fatal("expected error for missing bucket")
	}
	if !contains(err.Error(), "bucket") {
		t.Errorf("error %q should contain 'bucket'", err.Error())
	}
}

func TestNew_MissingRegion(t *testing.T) {
	_, err := New(Config{Bucket: "b"})
	if err == nil {
		t.Fatal("expected error for missing region")
	}
	if !contains(err.Error(), "region") {
		t.Errorf("error %q should contain 'region'", err.Error())
	}
}

func TestNew_DefaultFormat(t *testing.T) {
	exp, err := New(Config{Bucket: "b", Region: "r"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exp.cfg.Format != "jsonl" {
		t.Errorf("default format = %q, want jsonl", exp.cfg.Format)
	}
}

func TestNew_ValidConfig(t *testing.T) {
	_, err := New(Config{Bucket: "b", Region: "r", Format: "jsonl"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEncode_GzipValid(t *testing.T) {
	e := &Exporter{cfg: Config{Bucket: "b", Region: "r", Format: "jsonl"}}

	spans := []model.Span{
		{TraceID: model.TraceID{1}, SpanID: model.SpanID{1}, Name: "a"},
		{TraceID: model.TraceID{2}, SpanID: model.SpanID{2}, Name: "b"},
		{TraceID: model.TraceID{3}, SpanID: model.SpanID{3}, Name: "c"},
	}

	body, err := e.encode(model.Batch{Spans: spans})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	gr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	dec := json.NewDecoder(gr)

	count := 0
	for dec.More() {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			t.Fatalf("decode line %d: %v", count, err)
		}
		if !json.Valid(raw) {
			t.Errorf("line %d is not valid JSON", count)
		}
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 lines, got %d", count)
	}
}

func TestEncode_EmptyBatch(t *testing.T) {
	e := &Exporter{cfg: Config{Bucket: "b", Region: "r", Format: "jsonl"}}

	body, err := e.encode(model.Batch{})
	if err != nil {
		t.Fatalf("encode empty batch: %v", err)
	}

	gr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	dec := json.NewDecoder(gr)
	if dec.More() {
		t.Error("expected no lines for empty batch")
	}
}

func TestToLine_AllFields(t *testing.T) {
	start := time.Unix(1000, 0)
	end := time.Unix(1001, 500000)

	s := model.Span{
		TraceID:      model.TraceID{0x01, 0x02},
		SpanID:       model.SpanID{0x03, 0x04},
		ParentSpanID: model.SpanID{0x05, 0x06},
		Resource:     model.Resource{Attrs: []model.Attribute{model.StringAttr("service.name", "my-service")}},
		Name:         "my-op",
		Kind:         model.KindClient,
		StartTime:    start,
		EndTime:      end,
		Status:       model.StatusOK,
		Attrs: []model.Attribute{
			model.StringAttr("http.method", "GET"),
			{Key: "http.status_code", Value: model.IntValue(200)},
		},
	}

	l := toLine(s)

	if l.TraceID != s.TraceID.String() {
		t.Errorf("TraceID = %q, want %q", l.TraceID, s.TraceID.String())
	}
	if l.SpanID != s.SpanID.String() {
		t.Errorf("SpanID = %q, want %q", l.SpanID, s.SpanID.String())
	}
	if l.ParentID != s.ParentSpanID.String() {
		t.Errorf("ParentID = %q, want %q", l.ParentID, s.ParentSpanID.String())
	}
	if l.Service != "my-service" {
		t.Errorf("Service = %q, want my-service", l.Service)
	}
	if l.Name != "my-op" {
		t.Errorf("Name = %q, want my-op", l.Name)
	}
	if l.Kind != "client" {
		t.Errorf("Kind = %q, want client", l.Kind)
	}
	if l.StartUS != start.UnixMicro() {
		t.Errorf("StartUS = %d, want %d", l.StartUS, start.UnixMicro())
	}
	wantDuration := s.Duration().Microseconds()
	if l.DurationUS != wantDuration {
		t.Errorf("DurationUS = %d, want %d", l.DurationUS, wantDuration)
	}
	if l.Status != "ok" {
		t.Errorf("Status = %q, want ok", l.Status)
	}
	if l.Attrs["http.method"] != "GET" {
		t.Errorf("Attrs[http.method] = %q, want GET", l.Attrs["http.method"])
	}
	if l.Attrs["http.status_code"] != int64(200) {
		t.Errorf("Attrs[http.status_code] = %#v, want 200", l.Attrs["http.status_code"])
	}
}

func TestToLine_StatusStrings(t *testing.T) {
	cases := []struct {
		status model.SpanStatus
		want   string
	}{
		{model.StatusUnset, "unset"},
		{model.StatusOK, "ok"},
		{model.StatusError, "error"},
	}
	for _, tc := range cases {
		s := model.Span{Status: tc.status}
		l := toLine(s)
		if l.Status != tc.want {
			t.Errorf("status %v: got %q, want %q", tc.status, l.Status, tc.want)
		}
	}
}

func TestToLine_KindStrings(t *testing.T) {
	cases := []struct {
		kind model.SpanKind
		want string
	}{
		{model.KindUnspecified, "unspecified"},
		{model.KindInternal, "internal"},
		{model.KindServer, "server"},
		{model.KindClient, "client"},
		{model.KindProducer, "producer"},
		{model.KindConsumer, "consumer"},
	}
	for _, tc := range cases {
		s := model.Span{Kind: tc.kind}
		l := toLine(s)
		if l.Kind != tc.want {
			t.Errorf("kind %v: got %q, want %q", tc.kind, l.Kind, tc.want)
		}
	}
}

func TestObjectKey_Prefix(t *testing.T) {
	e := &Exporter{cfg: Config{Bucket: "b", Region: "r", Prefix: "traces", Format: "jsonl"}}
	key := e.objectKey(model.Batch{Spans: []model.Span{{TraceID: model.TraceID{1}}}})
	if !contains(key, "traces/") {
		t.Errorf("key %q should start with traces/", key)
	}
}

func TestObjectKey_PrefixTrailingSlash(t *testing.T) {
	e := &Exporter{cfg: Config{Bucket: "b", Region: "r", Prefix: "traces/", Format: "jsonl"}}
	key := e.objectKey(model.Batch{Spans: []model.Span{{TraceID: model.TraceID{1}}}})
	if !contains(key, "traces/") {
		t.Errorf("key %q should contain traces/", key)
	}
	if contains(key, "traces//") {
		t.Errorf("key %q should not contain double slash", key)
	}
}

func TestObjectKey_NoPrefix(t *testing.T) {
	e := &Exporter{cfg: Config{Bucket: "b", Region: "r", Prefix: "", Format: "jsonl"}}
	key := e.objectKey(model.Batch{})
	if len(key) > 0 && key[0] == '/' {
		t.Errorf("key %q should not start with / when prefix is empty", key)
	}
}

func TestObjectKey_ContainsDate(t *testing.T) {
	e := &Exporter{cfg: Config{Bucket: "b", Region: "r", Format: "jsonl"}}
	key := e.objectKey(model.Batch{})
	today := time.Now().UTC().Format("2006-01-02")
	if !contains(key, today) {
		t.Errorf("key %q should contain today's date %q", key, today)
	}
}

func TestObjectKey_ContainsHour(t *testing.T) {
	e := &Exporter{cfg: Config{Bucket: "b", Region: "r", Format: "jsonl"}}
	key := e.objectKey(model.Batch{})
	hour := fmt.Sprintf("%02d", time.Now().UTC().Hour())
	if !contains(key, hour) {
		t.Errorf("key %q should contain hour %q", key, hour)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
			return false
		}())
}
