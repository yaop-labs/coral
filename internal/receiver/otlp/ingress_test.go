package otlp

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/yaop-labs/coral/internal/logs"
	"github.com/yaop-labs/coral/internal/metric"
	"github.com/yaop-labs/coral/internal/model"
)

// capture records what each signal sink received.
type capture struct {
	mu     sync.Mutex
	traces int
	points int
	logs   int
}

func (c *capture) counts() (int, int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.traces, c.points, c.logs
}

func (c *capture) sink() Sink {
	return Sink{
		Traces: func(_ context.Context, b model.Batch) error {
			c.mu.Lock()
			c.traces += b.Len()
			c.mu.Unlock()
			return nil
		},
		Metrics: func(_ context.Context, b metric.Batch) error {
			c.mu.Lock()
			c.points += b.Len()
			c.mu.Unlock()
			return nil
		},
		Logs: func(_ context.Context, b logs.Batch) error {
			c.mu.Lock()
			c.logs += b.Len()
			c.mu.Unlock()
			return nil
		},
	}
}

func startIngress(t *testing.T, sink Sink) *Server {
	t.Helper()
	s := NewServer("127.0.0.1:0", "127.0.0.1:0", 0, sink)
	if err := s.Start(); err != nil {
		t.Fatalf("ingress Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Stop(ctx)
	})
	return s
}

func waitCounts(t *testing.T, c *capture, wantT, wantP, wantL int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		gt, gp, gl := c.counts()
		if gt >= wantT && gp >= wantP && gl >= wantL {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	gt, gp, gl := c.counts()
	t.Fatalf("counts = (traces %d, points %d, logs %d), want >= (%d, %d, %d)", gt, gp, gl, wantT, wantP, wantL)
}

func traceReq() *coltracepb.ExportTraceServiceRequest {
	return &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{kv("service.name", "svc")}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{
			{TraceId: make([]byte, 16), SpanId: make([]byte, 8), Name: "op"},
		}}},
	}}}
}

func metricReq() *colmetricspb.ExportMetricsServiceRequest {
	return &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{kv("service.name", "svc")}},
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{
			Name: "cpu_seconds_total",
			Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{
				TimeUnixNano: 1, Value: &metricspb.NumberDataPoint_AsInt{AsInt: 7},
			}}}},
		}}}},
	}}}
}

func logReq() *collogspb.ExportLogsServiceRequest {
	return &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{kv("service.name", "svc")}},
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
			TimeUnixNano: 1,
			Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hi"}},
		}}}},
	}}}
}

func kv(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func dialGRPC(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func postProto(t *testing.T, addr, path string, m proto.Message) *http.Response {
	t.Helper()
	body, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post("http://"+addr+path, "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// TestIngress_GRPC_AllSignals proves one gRPC server serves traces, metrics, and
// logs — the core of P0-1 (a stock OTel SDK hits a single endpoint per signal).
func TestIngress_GRPC_AllSignals(t *testing.T) {
	c := &capture{}
	s := startIngress(t, c.sink())
	conn := dialGRPC(t, s.GRPCAddr())

	if _, err := coltracepb.NewTraceServiceClient(conn).Export(context.Background(), traceReq()); err != nil {
		t.Fatalf("trace export: %v", err)
	}
	if _, err := colmetricspb.NewMetricsServiceClient(conn).Export(context.Background(), metricReq()); err != nil {
		t.Fatalf("metric export: %v", err)
	}
	if _, err := collogspb.NewLogsServiceClient(conn).Export(context.Background(), logReq()); err != nil {
		t.Fatalf("log export: %v", err)
	}

	waitCounts(t, c, 1, 1, 1)
	if _, _, traces, points, logRecs := s.Stats(); traces != 1 || points != 1 || logRecs != 1 {
		t.Errorf("Stats accepted = (t %d, p %d, l %d), want (1,1,1)", traces, points, logRecs)
	}
}

// TestIngress_HTTP_AllSignals proves one HTTP mux routes /v1/traces, /v1/metrics
// and /v1/logs.
func TestIngress_HTTP_AllSignals(t *testing.T) {
	c := &capture{}
	s := startIngress(t, c.sink())
	addr := s.HTTPAddr()

	for _, tc := range []struct {
		path string
		msg  proto.Message
	}{
		{"/v1/traces", traceReq()},
		{"/v1/metrics", metricReq()},
		{"/v1/logs", logReq()},
	} {
		resp := postProto(t, addr, tc.path, tc.msg)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("POST %s: status %d, want 200", tc.path, resp.StatusCode)
		}
	}
	waitCounts(t, c, 1, 1, 1)
}

func TestIngress_HTTP_MethodNotAllowed(t *testing.T) {
	c := &capture{}
	s := startIngress(t, c.sink())
	resp, err := http.Get("http://" + s.HTTPAddr() + "/v1/traces")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

func TestIngress_HTTP_UnsupportedContentType(t *testing.T) {
	c := &capture{}
	s := startIngress(t, c.sink())
	resp, err := http.Post("http://"+s.HTTPAddr()+"/v1/metrics", "text/plain", strings.NewReader("nope"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("status = %d, want 415", resp.StatusCode)
	}
}

func TestIngress_HTTP_BadProtobuf(t *testing.T) {
	c := &capture{}
	s := startIngress(t, c.sink())
	resp, err := http.Post("http://"+s.HTTPAddr()+"/v1/logs", "application/x-protobuf", bytes.NewReader([]byte{0xFF, 0xFE}))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestIngress_HTTP_Healthz(t *testing.T) {
	c := &capture{}
	s := startIngress(t, c.sink())
	resp, err := http.Get("http://" + s.HTTPAddr() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

// TestIngress_UnservedSignal proves that a signal with no pipeline is not served:
// gRPC returns Unimplemented and HTTP returns 404, rather than accept-and-drop.
func TestIngress_UnservedSignal(t *testing.T) {
	// Only traces are wired.
	c := &capture{}
	sink := c.sink()
	sink.Metrics = nil
	sink.Logs = nil
	s := startIngress(t, sink)

	conn := dialGRPC(t, s.GRPCAddr())
	_, err := colmetricspb.NewMetricsServiceClient(conn).Export(context.Background(), metricReq())
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("metric gRPC on traces-only ingress: code = %v, want Unimplemented", status.Code(err))
	}

	resp := postProto(t, s.HTTPAddr(), "/v1/metrics", metricReq())
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("POST /v1/metrics on traces-only ingress: status %d, want 404", resp.StatusCode)
	}
}
