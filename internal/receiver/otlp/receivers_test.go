package otlp

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/hnlbs/collector/internal/model"
)

type collectEmit struct {
	mu    sync.Mutex
	spans []model.Span
}

func (c *collectEmit) emit(_ context.Context, b model.Batch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.spans = append(c.spans, b.Spans...)
	return nil
}

func (c *collectEmit) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.spans)
}

func waitForAddr(t *testing.T, addrFn func() string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a := addrFn(); a != "" {
			return a
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("receiver did not bind within timeout")
	return ""
}

func TestHTTPReceiver_InvalidEndpoint(t *testing.T) {
	_, err := NewHTTP("")
	if err == nil {
		t.Error("expected error for empty endpoint")
	}
}

func TestHTTPReceiver_StartStop(t *testing.T) {
	r, err := NewHTTP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = r.Start(ctx, func(_ context.Context, _ model.Batch) error { return nil }) }()
	defer cancel()

	addr := waitForAddr(t, r.Addr)
	if addr == "" {
		t.Fatal("no addr")
	}

	stopCtx, sc := context.WithTimeout(context.Background(), 2*time.Second)
	defer sc()
	if err := r.Stop(stopCtx); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

func TestHTTPReceiver_Healthz(t *testing.T) {
	r, err := NewHTTP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Start(ctx, func(_ context.Context, _ model.Batch) error { return nil }) }()

	addr := waitForAddr(t, r.Addr)
	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHTTPReceiver_MethodNotAllowed(t *testing.T) {
	r, err := NewHTTP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Start(ctx, func(_ context.Context, _ model.Batch) error { return nil }) }()

	addr := waitForAddr(t, r.Addr)
	resp, err := http.Get("http://" + addr + "/v1/traces")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

func TestHTTPReceiver_BadProtobuf(t *testing.T) {
	r, err := NewHTTP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Start(ctx, func(_ context.Context, _ model.Batch) error { return nil }) }()

	addr := waitForAddr(t, r.Addr)
	resp, err := http.Post("http://"+addr+"/v1/traces",
		"application/x-protobuf", bytes.NewReader([]byte{0xFF, 0xFE}))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHTTPReceiver_EmptyBatch(t *testing.T) {
	called := false
	r, err := NewHTTP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = r.Start(ctx, func(_ context.Context, _ model.Batch) error {
			called = true
			return nil
		})
	}()

	addr := waitForAddr(t, r.Addr)
	body, _ := proto.Marshal(&coltracepb.ExportTraceServiceRequest{})
	resp, err := http.Post("http://"+addr+"/v1/traces",
		"application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	time.Sleep(20 * time.Millisecond)
	if called {
		t.Error("emit should not be called for empty batch")
	}
}

func TestHTTPReceiver_ValidBatch(t *testing.T) {
	col := &collectEmit{}
	r, err := NewHTTP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Start(ctx, col.emit) }()

	addr := waitForAddr(t, r.Addr)

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{
					Value: &commonpb.AnyValue_StringValue{StringValue: "svc"}}},
			}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{
					{TraceId: make([]byte, 16), SpanId: make([]byte, 8), Name: "op1"},
					{TraceId: make([]byte, 16), SpanId: make([]byte, 8), Name: "op2"},
				},
			}},
		}},
	}
	body, _ := proto.Marshal(req)
	resp, err := http.Post("http://"+addr+"/v1/traces",
		"application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if col.count() >= 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("expected 2 spans, got %d", col.count())
}

func TestGRPCReceiver_InvalidEndpoint(t *testing.T) {
	_, err := NewGRPC("", 0)
	if err == nil {
		t.Error("expected error for empty endpoint")
	}
}

func TestGRPCReceiver_StartStop(t *testing.T) {
	r, err := NewGRPC("127.0.0.1:0", 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = r.Start(ctx, func(_ context.Context, _ model.Batch) error { return nil }) }()
	defer cancel()

	addr := waitForAddr(t, r.Addr)
	if addr == "" {
		t.Fatal("no addr")
	}

	stopCtx, sc := context.WithTimeout(context.Background(), 2*time.Second)
	defer sc()
	if err := r.Stop(stopCtx); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

func TestGRPCReceiver_Export(t *testing.T) {
	col := &collectEmit{}
	r, err := NewGRPC("127.0.0.1:0", 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Start(ctx, col.emit) }()

	addr := waitForAddr(t, r.Addr)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := coltracepb.NewTraceServiceClient(conn)
	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{
					Value: &commonpb.AnyValue_StringValue{StringValue: "grpc-svc"}}},
			}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{
					{TraceId: make([]byte, 16), SpanId: make([]byte, 8), Name: "grpc-op"},
				},
			}},
		}},
	}
	if _, err := client.Export(context.Background(), req); err != nil {
		t.Fatalf("Export: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if col.count() >= 1 {
			reqs, errs, accepted := r.Stats()
			if reqs != 1 {
				t.Errorf("requests = %d, want 1", reqs)
			}
			if errs != 0 {
				t.Errorf("errs = %d, want 0", errs)
			}
			if accepted != 1 {
				t.Errorf("spansAccepted = %d, want 1", accepted)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("span not received via gRPC, got %d", col.count())
}

func TestGRPCReceiver_EmptyRequest(t *testing.T) {
	col := &collectEmit{}
	r, err := NewGRPC("127.0.0.1:0", 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Start(ctx, col.emit) }()

	addr := waitForAddr(t, r.Addr)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := coltracepb.NewTraceServiceClient(conn)
	if _, err := client.Export(context.Background(), &coltracepb.ExportTraceServiceRequest{}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	reqs, _, accepted := r.Stats()
	if reqs != 1 {
		t.Errorf("requests = %d, want 1", reqs)
	}
	if accepted != 0 {
		t.Errorf("spansAccepted = %d, want 0", accepted)
	}
	if col.count() != 0 {
		t.Errorf("emit should not be called for empty request")
	}
}
