package logs

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func TestAmberLogExporterExport(t *testing.T) {
	var gotPath string
	var got *collogspb.ExportLogsServiceRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		req := &collogspb.ExportLogsServiceRequest{}
		_ = proto.Unmarshal(body, req)
		gotPath = r.URL.Path
		got = req
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exp, err := NewAmberExporter(server.URL, time.Second, RetryPolicy{})
	if err != nil {
		t.Fatalf("new amber exporter: %v", err)
	}
	err = exp.Export(context.Background(), Batch{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringKV("service.name", "checkout")}},
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
			TimeUnixNano: 1,
			Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "payment timeout"}},
		}}}},
	}}})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if gotPath != "/v1/logs" {
		t.Fatalf("path = %q, want /v1/logs", gotPath)
	}
	if got == nil || len(got.ResourceLogs) != 1 {
		t.Fatalf("unexpected log request: %+v", got)
	}
}

func TestLogExporterPartialSuccessIsNotDelivery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		body, _ := proto.Marshal(&collogspb.ExportLogsServiceResponse{
			PartialSuccess: &collogspb.ExportLogsPartialSuccess{RejectedLogRecords: 1, ErrorMessage: "invalid record"},
		})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer server.Close()
	exporter, err := NewAmberExporter(server.URL, time.Second, RetryPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	batch := Batch{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{}}}},
	}}}
	if err := exporter.Export(context.Background(), batch); err == nil {
		t.Fatal("partial success was treated as complete delivery")
	}
}
