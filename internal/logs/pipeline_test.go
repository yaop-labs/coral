package logs

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func TestLogPipelineEndToEnd(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var mu sync.Mutex
	var got *collogspb.ExportLogsServiceRequest
	var gotPath string
	fathom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		req := &collogspb.ExportLogsServiceRequest{}
		_ = proto.Unmarshal(body, req)
		mu.Lock()
		got, gotPath = req, r.URL.Path
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer fathom.Close()

	recv := NewOTLPReceiver("127.0.0.1:0", "", logger)
	exp, err := NewFathomExporter(fathom.URL, 2*time.Second, RetryPolicy{})
	if err != nil {
		t.Fatal(err)
	}

	p := NewPipeline(2, 100, logger)
	p.AddReceiver(recv)
	p.AddExporter(exp)

	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		_ = p.Shutdown(context.Background())
	}()

	conn, err := grpc.NewClient(recv.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := collogspb.NewLogsServiceClient(conn)

	req := &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringKV("service.name", "checkout")}},
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
			TimeUnixNano: 1,
			Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "payment timeout"}},
		}}}},
	}}}
	if _, err := client.Export(context.Background(), req); err != nil {
		t.Fatalf("grpc export: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := got != nil
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if got == nil {
		t.Fatal("fake fathom received nothing")
	}
	if gotPath != "/v1/logs" {
		t.Errorf("path = %q, want /v1/logs", gotPath)
	}
	if len(got.ResourceLogs) != 1 {
		t.Fatalf("ResourceLogs = %d, want 1", len(got.ResourceLogs))
	}
	body := got.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Body.GetStringValue()
	if body != "payment timeout" {
		t.Fatalf("log body = %q", body)
	}
}

func TestFathomLogExporterExport(t *testing.T) {
	var gotPath string
	var got *collogspb.ExportLogsServiceRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		req := &collogspb.ExportLogsServiceRequest{}
		_ = proto.Unmarshal(body, req)
		gotPath = r.URL.Path
		got = req
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	exp, err := NewFathomExporter(server.URL, time.Second, RetryPolicy{})
	if err != nil {
		t.Fatalf("new fathom exporter: %v", err)
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

func stringKV(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}
