package otlp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/yaop-labs/coral/internal/logs"
	"github.com/yaop-labs/coral/internal/metric"
	"github.com/yaop-labs/coral/internal/model"
	"github.com/yaop-labs/reef/bearer"
	"github.com/yaop-labs/reef/credential"
	"github.com/yaop-labs/reef/edge"
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

// rejectNamed builds a trace admit hook that rejects spans named "reject",
// standing in for any accept-time validation (e.g. oversized spans).
func rejectNamed(b model.Batch) (model.Batch, int, string) {
	kept := b.Spans[:0]
	rejected := 0
	for _, s := range b.Spans {
		if s.Name == "reject" {
			rejected++
			continue
		}
		kept = append(kept, s)
	}
	return model.Batch{Spans: kept}, rejected, "rejected test spans"
}

func twoSpanReq() *coltracepb.ExportTraceServiceRequest {
	return &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{
			{TraceId: make([]byte, 16), SpanId: make([]byte, 8), Name: "reject"},
			{TraceId: make([]byte, 16), SpanId: make([]byte, 8), Name: "keep"},
		}}},
	}}}
}

// TestIngress_HTTP_PartialSuccess proves a partially-invalid batch is answered
// 200 with partial_success (contract §4) — the sender must not retry — while the
// valid records are still admitted.
func TestIngress_HTTP_PartialSuccess(t *testing.T) {
	c := &capture{}
	sink := c.sink()
	sink.TraceAdmit = rejectNamed
	s := startIngress(t, sink)

	resp := postProto(t, s.HTTPAddr(), "/v1/traces", twoSpanReq())
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var out coltracepb.ExportTraceServiceResponse
	if err := proto.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.GetPartialSuccess().GetRejectedSpans() != 1 {
		t.Errorf("rejected_spans = %d, want 1", out.GetPartialSuccess().GetRejectedSpans())
	}
	if out.GetPartialSuccess().GetErrorMessage() == "" {
		t.Error("partial_success error_message should be set")
	}
	waitCounts(t, c, 1, 0, 0) // the one kept span was admitted
}

func TestIngress_HTTP_JSONResponseMatchesRequestEncoding(t *testing.T) {
	c := &capture{}
	sink := c.sink()
	sink.TraceAdmit = rejectNamed
	s := startIngress(t, sink)
	body, err := protojson.Marshal(twoSpanReq())
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post("http://"+s.HTTPAddr()+"/v1/traces", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("response content-type = %q, want application/json", got)
	}
	responseBody, _ := io.ReadAll(resp.Body)
	var out coltracepb.ExportTraceServiceResponse
	if err := protojson.Unmarshal(responseBody, &out); err != nil {
		t.Fatalf("response is not valid OTLP JSON: %v", err)
	}
	if got := out.GetPartialSuccess().GetRejectedSpans(); got != 1 {
		t.Fatalf("rejected spans = %d, want 1", got)
	}
}

func TestIngress_HTTP_BearerAuth(t *testing.T) {
	c := &capture{}
	s, err := NewSecureServer("", "127.0.0.1:0", 0, c.sink(), SecurityConfig{
		HTTP: edge.ServerConfig{
			Auth:                           &bearer.ServerConfig{Bearer: []bearer.Key{{Name: "test", Token: "test-token"}}},
			DangerAllowBearerOverPlaintext: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Stop(ctx)
	})
	body, _ := proto.Marshal(traceReq())
	url := "http://" + s.HTTPAddr() + "/v1/traces"
	resp, err := http.Post(url, "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", resp.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Authorization", "Bearer test-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("authenticated status = %d, want 200", resp.StatusCode)
	}
}

func TestIngress_GRPC_BearerAuth(t *testing.T) {
	c := &capture{}
	s, err := NewSecureServer("127.0.0.1:0", "", 0, c.sink(), SecurityConfig{
		GRPC: edge.ServerConfig{
			Auth:                           &bearer.ServerConfig{Bearer: []bearer.Key{{Name: "test", Token: "test-token"}}},
			DangerAllowBearerOverPlaintext: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Stop(ctx)
	})
	conn := dialGRPC(t, s.GRPCAddr())
	client := coltracepb.NewTraceServiceClient(conn)
	if _, err := client.Export(context.Background(), traceReq()); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated code = %v, want Unauthenticated", status.Code(err))
	}
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer test-token")
	if _, err := client.Export(ctx, traceReq()); err != nil {
		t.Fatalf("authenticated export: %v", err)
	}
}

func TestIngress_RejectsUnsafeExternalPlaintext(t *testing.T) {
	_, err := NewSecureServer("", "0.0.0.0:4318", 0, (&capture{}).sink(), SecurityConfig{
		HTTP: edge.ServerConfig{},
	})
	if err == nil || !strings.Contains(err.Error(), "insecure: true") {
		t.Fatalf("NewSecureServer error = %v, want explicit insecure opt-in", err)
	}

	_, err = NewSecureServer("", "127.0.0.1:0", 0, (&capture{}).sink(), SecurityConfig{
		HTTP: edge.ServerConfig{
			Auth: &bearer.ServerConfig{Bearer: []bearer.Key{{Name: "wisp", Token: "secret"}}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "danger_allow_bearer_over_plaintext") {
		t.Fatalf("bearer plaintext error = %v, want explicit danger opt-in", err)
	}
}

func TestIngress_HTTP_PrincipalPropagationAndTokenRotation(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("old-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	principals := make(chan string, 2)
	events := make(chan credential.Event, 4)
	sink := Sink{Traces: func(ctx context.Context, _ model.Batch) error {
		principal, _ := bearer.PrincipalFromContext(ctx)
		principals <- principal
		return nil
	}}
	s, err := NewSecureServer("", "127.0.0.1:0", 0, sink, SecurityConfig{
		HTTP: edge.ServerConfig{
			Auth: &bearer.ServerConfig{Bearer: []bearer.Key{{
				Name:      "wisp-project-a",
				TokenFile: tokenFile,
			}}},
			DangerAllowBearerOverPlaintext: true,
			ReloadInterval:                 time.Hour,
			Observer: credential.ObserverFunc(func(event credential.Event) {
				events <- event
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Stop(ctx)
	})

	if code := postTraceWithToken(t, s.HTTPAddr(), "old-token"); code != http.StatusOK {
		t.Fatalf("old token status = %d, want 200", code)
	}
	if principal := <-principals; principal != "wisp-project-a" {
		t.Fatalf("principal = %q", principal)
	}
	if err := os.WriteFile(tokenFile, []byte("new-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.ReloadCredentials(); err != nil {
		t.Fatalf("ReloadCredentials: %v", err)
	}
	if code := postTraceWithToken(t, s.HTTPAddr(), "old-token"); code != http.StatusUnauthorized {
		t.Fatalf("rotated old token status = %d, want 401", code)
	}
	if code := postTraceWithToken(t, s.HTTPAddr(), "new-token"); code != http.StatusOK {
		t.Fatalf("new token status = %d, want 200", code)
	}
	if principal := <-principals; principal != "wisp-project-a" {
		t.Fatalf("rotated principal = %q", principal)
	}

	statuses := s.CredentialStatus()
	if len(statuses) != 1 || statuses[0].Generation != 2 || statuses[0].LastError != "" {
		t.Fatalf("credential statuses = %+v", statuses)
	}
	initial, changed := <-events, <-events
	if !initial.Success || !changed.Success || !changed.Changed || changed.Status.Generation != 2 {
		t.Fatalf("credential events = initial:%+v changed:%+v", initial, changed)
	}
}

func TestTenantQuotaExceeded(t *testing.T) {
	ctx := context.WithValue(context.Background(), tenantContextKey{}, "tenant-a")
	if !quotaExceeded(ctx, map[string]TenantLimit{"tenant-a": {MaxItems: 1}}, 2, 0) {
		t.Fatal("item quota not enforced")
	}
	if !quotaExceeded(ctx, map[string]TenantLimit{"tenant-a": {MaxBytes: 10}}, 1, 11) {
		t.Fatal("byte quota not enforced")
	}
	if quotaExceeded(ctx, map[string]TenantLimit{"tenant-b": {MaxItems: 1}}, 2, 0) {
		t.Fatal("quota crossed tenant boundary")
	}
}

func TestLogRecordLimitsRejectBeforeSink(t *testing.T) {
	called := false
	s := &Server{
		tenantMap:    map[string]string{"principal": "tenant-a"},
		tenantLimits: map[string]TenantLimit{"tenant-a": {MaxLogRecordBytes: 1, MaxLogAttributes: 1, MaxLogAttributeKeys: 1}},
		sink:         Sink{Logs: func(context.Context, logs.Batch) error { called = true; return nil }},
	}
	ctx := bearer.ContextWithPrincipal(context.Background(), "principal")
	rl := []*logspb.ResourceLogs{{ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "too large"}}}}}}}}
	if _, _, err := s.admitLogs(ctx, rl); !errors.Is(err, errLogRecordTooLarge) {
		t.Fatalf("admission error = %v, want log record limit", err)
	}
	if called {
		t.Fatal("log sink called for rejected record")
	}
}

func TestTenantConcurrentAdmission(t *testing.T) {
	s := &Server{tenantLimits: map[string]TenantLimit{"tenant-a": {MaxConcurrent: 1}}, tenantStats: map[string]TenantCounters{"tenant-a": {}}}
	ctx := context.WithValue(context.Background(), tenantContextKey{}, "tenant-a")
	release, err := s.acquireTenant(ctx)
	if err != nil {
		t.Fatal("first acquire denied")
	}
	if _, err = s.acquireTenant(ctx); err == nil {
		t.Fatal("second acquire allowed")
	}
	release()
	if release2, err := s.acquireTenant(ctx); err != nil {
		t.Fatal("acquire after release denied")
	} else {
		release2()
	}
}

func TestTenantRequestRateLimit(t *testing.T) {
	s := &Server{tenantLimits: map[string]TenantLimit{"tenant-a": {MaxRequestsPerSecond: 1}}, tenantStats: map[string]TenantCounters{"tenant-a": {}}}
	ctx := context.WithValue(context.Background(), tenantContextKey{}, "tenant-a")
	release, err := s.acquireTenant(ctx)
	if err != nil {
		t.Fatal("first acquire denied")
	}
	release()
	if _, err = s.acquireTenant(ctx); !errors.Is(err, errTenantRate) {
		t.Fatalf("second acquire error = %v, want rate quota", err)
	}
	if got := s.TenantStats()["tenant-a"].QuotaRejected; got != 1 {
		t.Fatalf("quota rejects = %d, want 1", got)
	}
}

func postTraceWithToken(t *testing.T, address, token string) int {
	t.Helper()
	body, err := proto.Marshal(traceReq())
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+address+"/v1/traces", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// TestIngress_GRPC_PartialSuccess is the gRPC counterpart.
func TestIngress_GRPC_PartialSuccess(t *testing.T) {
	c := &capture{}
	sink := c.sink()
	sink.TraceAdmit = rejectNamed
	s := startIngress(t, sink)
	conn := dialGRPC(t, s.GRPCAddr())

	resp, err := coltracepb.NewTraceServiceClient(conn).Export(context.Background(), twoSpanReq())
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if resp.GetPartialSuccess().GetRejectedSpans() != 1 {
		t.Errorf("rejected_spans = %d, want 1", resp.GetPartialSuccess().GetRejectedSpans())
	}
	waitCounts(t, c, 1, 0, 0)
	if tr, _, _ := s.Rejected(); tr != 1 {
		t.Errorf("Rejected traces = %d, want 1", tr)
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
