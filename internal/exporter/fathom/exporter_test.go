package fathom

import (
	"context"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yaop-labs/coral/internal/model"
)

func TestExporterExport(t *testing.T) {
	var gotContentType string
	var gotBytes int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" {
			t.Fatalf("path = %q, want /v1/traces", r.URL.Path)
		}
		gotContentType = r.Header.Get("Content-Type")
		gotBytes = int(r.ContentLength)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	exporter, err := New(server.URL, time.Second)
	if err != nil {
		t.Fatalf("new exporter: %v", err)
	}
	err = exporter.Export(context.Background(), model.Batch{Spans: []model.Span{testSpan()}})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if gotContentType != "application/x-protobuf" {
		t.Fatalf("content type = %q", gotContentType)
	}
	if gotBytes == 0 {
		t.Fatal("expected protobuf body")
	}
}

func testSpan() model.Span {
	var traceID model.TraceID
	var spanID model.SpanID
	copy(traceID[:], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	copy(spanID[:], []byte{1, 2, 3, 4, 5, 6, 7, 8})
	return model.Span{
		TraceID:   traceID,
		SpanID:    spanID,
		Name:      "POST /checkout",
		StartTime: time.Unix(0, 1),
		EndTime:   time.Unix(0, 2),
		Status:    model.StatusError,
		Resource: model.Resource{Attrs: []model.Attribute{
			model.StringAttr("service.name", "checkout"),
		}},
	}
}

func TestToTraceRequestPreservesRawEventsAndLinks(t *testing.T) {
	sp := &tracepb.Span{Name: "raw", Events: []*tracepb.Span_Event{{Name: "exception"}}, Links: []*tracepb.Span_Link{{TraceId: []byte{1}}}}
	raw, err := proto.Marshal(sp)
	if err != nil {
		t.Fatal(err)
	}
	req := toTraceRequest(model.Batch{Spans: []model.Span{{Name: "raw", OTLP: raw}}})
	out := req.GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()[0]
	if len(out.GetEvents()) != 1 || len(out.GetLinks()) != 1 {
		t.Fatalf("events/links lost: %d/%d", len(out.GetEvents()), len(out.GetLinks()))
	}
}
