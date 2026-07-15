package otlphttp

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func sampleReq() *coltracepb.ExportTraceServiceRequest {
	return &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{Name: "checkout"}},
			}},
		}},
	}
}

func spanName(r *coltracepb.ExportTraceServiceRequest) string {
	return r.GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()[0].GetName()
}

func TestReadBody_GzipProtobuf(t *testing.T) {
	raw, _ := proto.Marshal(sampleReq())
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write(raw)
	_ = gz.Close()

	req := httptest.NewRequest(http.MethodPost, "/v1/traces", &buf)
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()

	body, enc, ok := ReadBody(w, req, 16<<20)
	if !ok {
		t.Fatalf("ReadBody not ok, code %d", w.Code)
	}
	if enc != EncodingProtobuf {
		t.Fatalf("enc = %v, want protobuf", enc)
	}
	var got coltracepb.ExportTraceServiceRequest
	if err := Unmarshal(enc, body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if spanName(&got) != "checkout" {
		t.Fatal("gzip body not decompressed/decoded correctly")
	}
}

func TestReadBody_JSON(t *testing.T) {
	raw, _ := protojson.Marshal(sampleReq())
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	body, enc, ok := ReadBody(w, req, 16<<20)
	if !ok {
		t.Fatalf("ReadBody not ok, code %d", w.Code)
	}
	if enc != EncodingJSON {
		t.Fatalf("enc = %v, want json", enc)
	}
	var got coltracepb.ExportTraceServiceRequest
	if err := Unmarshal(enc, body, &got); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	if spanName(&got) != "checkout" {
		t.Fatal("json body not decoded")
	}
}

func TestReadBody_UnsupportedContentType(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader("x"))
	req.Header.Set("Content-Type", "text/csv")
	w := httptest.NewRecorder()
	if _, _, ok := ReadBody(w, req, 16<<20); ok {
		t.Fatal("expected rejection of unknown content-type")
	}
	if w.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("code = %d, want 415", w.Code)
	}
}

func TestReadBody_TooLarge(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader(strings.Repeat("a", 2048)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	w := httptest.NewRecorder()
	if _, _, ok := ReadBody(w, req, 1024); ok {
		t.Fatal("expected rejection of oversize body")
	}
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code = %d, want 413", w.Code)
	}
}

func TestReadBody_DecompressedBodyTooLarge(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write([]byte(strings.Repeat("a", 2048)))
	_ = gz.Close()
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", &buf)
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	if _, _, ok := ReadBody(w, req, 1024); ok {
		t.Fatal("expected rejection of oversized decompressed body")
	}
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code = %d, want 413", w.Code)
	}
}

func TestReadBody_MethodNotAllowed(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/traces", nil)
	w := httptest.NewRecorder()
	if _, _, ok := ReadBody(w, req, 1024); ok {
		t.Fatal("expected rejection of non-POST")
	}
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("code = %d, want 405", w.Code)
	}
}
