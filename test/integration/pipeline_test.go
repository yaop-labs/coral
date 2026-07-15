package integration

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	"github.com/yaop-labs/coral/internal/app"
	"github.com/yaop-labs/coral/internal/config"
	"github.com/yaop-labs/coral/internal/model"
	"github.com/yaop-labs/coral/internal/pipeline"
	"github.com/yaop-labs/coral/internal/processor/sampling"
	otlprecv "github.com/yaop-labs/coral/internal/receiver/otlp"
)

// capturingExporter collects all exported spans for assertions.
type capturingExporter struct {
	mu    sync.Mutex
	spans []model.Span
}

func (e *capturingExporter) Export(_ context.Context, b model.Batch) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, b.Spans...)
	return nil
}

func (e *capturingExporter) Close() error { return nil }

func (e *capturingExporter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.spans)
}

// startApp wires up an App with the given config and a capturingExporter
// injected as the sole exporter. Returns the App and the exporter.
func startApp(t *testing.T, cfg config.Config) (*app.App, *capturingExporter) {
	t.Helper()
	cap := &capturingExporter{}
	a, err := app.NewWithExporter(cfg, nil, cap)
	if err != nil {
		t.Fatalf("app.NewWithExporter: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := a.Start(ctx); err != nil {
		cancel()
		t.Fatalf("app.Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		stopCtx, sc := context.WithTimeout(context.Background(), 5*time.Second)
		defer sc()
		_ = a.Shutdown(stopCtx)
	})
	return a, cap
}

func baseConfig() config.Config {
	return config.Config{
		Pipeline: config.PipelineConfig{Workers: 2, QueueSize: 256},
		Receivers: config.ReceiversConfig{
			OTLPHTTP: &config.OTLPEndpointConfig{Endpoint: "127.0.0.1:0"},
			OTLPGRPC: &config.OTLPEndpointConfig{Endpoint: "127.0.0.1:0"},
		},
		Exporters: []config.ExporterConfig{{Type: "devnull"}},
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func sendTracesHTTP(t *testing.T, addr string, req *coltracepb.ExportTraceServiceRequest) {
	t.Helper()
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post("http://"+addr+"/v1/traces", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/traces: status %d", resp.StatusCode)
	}
}

// startTraceIngress binds a unified OTLP ingress (HTTP only) that feeds p's
// trace queue, returning the bound HTTP address. It replaces the former
// per-signal HTTP trace receiver in these lower-level pipeline tests.
func startTraceIngress(t *testing.T, p *pipeline.Pipeline[model.Batch]) string {
	t.Helper()
	ing := otlprecv.NewServer("", "127.0.0.1:0", 0, otlprecv.Sink{Traces: p.Enqueue})
	if err := ing.Start(); err != nil {
		t.Fatalf("ingress Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ing.Stop(ctx)
	})
	return ing.HTTPAddr()
}

// ymlExporter builds a trace ExporterConfig from a YAML mapping (so its Raw node
// is populated the way the real loader populates it, letting buildExporter
// decode the amber endpoint).
func ymlExporter(t *testing.T, doc string) config.ExporterConfig {
	t.Helper()
	var ec config.ExporterConfig
	if err := yaml.Unmarshal([]byte(doc), &ec); err != nil {
		t.Fatalf("ymlExporter: %v", err)
	}
	return ec
}

// postProtoPath posts a protobuf OTLP message to addr+path and requires 200.
func postProtoPath(t *testing.T, addr, path string, m proto.Message) {
	t.Helper()
	body, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post("http://"+addr+path, "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s: status %d", path, resp.StatusCode)
	}
}

func sendTracesGRPC(t *testing.T, addr string, req *coltracepb.ExportTraceServiceRequest) {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := coltracepb.NewTraceServiceClient(conn)
	if _, err := client.Export(context.Background(), req); err != nil {
		t.Fatalf("gRPC Export: %v", err)
	}
}

func makeSpanRequest(traceID [16]byte, spanID [8]byte, name string, hasError bool) *coltracepb.ExportTraceServiceRequest {
	now := uint64(time.Now().UnixNano())
	status := &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK}
	if hasError {
		status = &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR}
	}
	return &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{StringValue: "test-svc"},
					}},
				},
			},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					TraceId:           traceID[:],
					SpanId:            spanID[:],
					Name:              name,
					StartTimeUnixNano: now,
					EndTimeUnixNano:   now + uint64(time.Millisecond),
					Status:            status,
				}},
			}},
		}},
	}
}

func TestIntegration_HTTPSpanReachesExporter(t *testing.T) {
	cfg := baseConfig()
	a, cap := startApp(t, cfg)

	req := makeSpanRequest(
		[16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		[8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		"GET /api",
		false,
	)
	sendTracesHTTP(t, a.OTLPHTTPAddr(), req)

	waitFor(t, 2*time.Second, func() bool { return cap.count() >= 1 })
}

func TestIntegration_GRPCSpanReachesExporter(t *testing.T) {
	cfg := baseConfig()
	a, cap := startApp(t, cfg)

	req := makeSpanRequest(
		[16]byte{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
		[8]byte{2, 2, 2, 2, 2, 2, 2, 2},
		"POST /submit",
		false,
	)
	sendTracesGRPC(t, a.OTLPGRPCAddr(), req)

	waitFor(t, 2*time.Second, func() bool { return cap.count() >= 1 })
}

func TestIntegration_MultipleSpansAllReachExporter(t *testing.T) {
	cfg := baseConfig()
	a, cap := startApp(t, cfg)

	for i := range 5 {
		var traceID [16]byte
		var spanID [8]byte
		traceID[0] = byte(i + 10)
		spanID[0] = byte(i + 10)
		req := makeSpanRequest(traceID, spanID, "op", false)
		sendTracesHTTP(t, a.OTLPHTTPAddr(), req)
	}

	waitFor(t, 2*time.Second, func() bool { return cap.count() >= 5 })
}

func TestIntegration_ShutdownIsClean(t *testing.T) {
	cfg := baseConfig()
	a, _ := startApp(t, cfg)

	req := makeSpanRequest(
		[16]byte{3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3},
		[8]byte{3, 3, 3, 3, 3, 3, 3, 3},
		"shutdown-test",
		false,
	)
	sendTracesHTTP(t, a.OTLPHTTPAddr(), req)
	time.Sleep(50 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Shutdown(stopCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// fakeReceiver satisfies pipeline.Receiver and lets tests inject batches directly.
type fakeReceiver struct {
	mu    sync.Mutex
	emit  func(context.Context, model.Batch) error
	ready chan struct{}
}

func newFakeReceiver() *fakeReceiver {
	return &fakeReceiver{ready: make(chan struct{})}
}

func (r *fakeReceiver) Start(ctx context.Context, emit func(context.Context, model.Batch) error) error {
	r.mu.Lock()
	r.emit = emit
	r.mu.Unlock()
	close(r.ready)
	<-ctx.Done()
	return nil
}

func (r *fakeReceiver) Stop(_ context.Context) error { return nil }

func (r *fakeReceiver) Send(ctx context.Context, b model.Batch) error {
	<-r.ready
	r.mu.Lock()
	emit := r.emit
	r.mu.Unlock()
	if emit == nil {
		return nil
	}
	return emit(ctx, b)
}

var _ pipeline.Receiver[model.Batch] = (*fakeReceiver)(nil)

func TestIntegration_FakeReceiver_SpanFlowsThroughPipeline(t *testing.T) {
	cap := &capturingExporter{}
	recv := newFakeReceiver()

	p := pipeline.New[model.Batch](pipeline.Config{Workers: 1, QueueSize: 16}, slog.Default())
	p.AddReceiver(recv)
	p.AddExporter(cap)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("pipeline.Start: %v", err)
	}

	b := model.Batch{Spans: []model.Span{
		{Name: "direct-span"},
	}}
	if err := recv.Send(context.Background(), b); err != nil {
		t.Fatalf("Send: %v", err)
	}

	waitFor(t, time.Second, func() bool { return cap.count() >= 1 })

	stopCtx, sc := context.WithTimeout(context.Background(), 2*time.Second)
	defer sc()
	if err := p.Shutdown(stopCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// processorCfg builds a ProcessorConfig from a YAML mapping document.
// The document must contain a type key.
func processorCfg(t *testing.T, yamlDoc string) config.ProcessorConfig {
	t.Helper()
	var raw yaml.Node
	if err := yaml.Unmarshal([]byte(yamlDoc), &raw); err != nil {
		t.Fatalf("processorCfg: parse: %v", err)
	}
	if raw.Kind != yaml.DocumentNode || len(raw.Content) == 0 {
		t.Fatalf("processorCfg: expected document node, got kind=%v", raw.Kind)
	}
	mappingNode := raw.Content[0]

	typVal := ""
	for i := 0; i+1 < len(mappingNode.Content); i += 2 {
		if mappingNode.Content[i].Value == "type" {
			typVal = mappingNode.Content[i+1].Value
			break
		}
	}
	if typVal == "" {
		t.Fatal("processorCfg: no 'type' key in yaml doc")
	}

	rawMapping := &yaml.Node{Kind: yaml.MappingNode}
	for i := 0; i+1 < len(mappingNode.Content); i += 2 {
		if mappingNode.Content[i].Value == "type" {
			continue
		}
		rawMapping.Content = append(rawMapping.Content,
			mappingNode.Content[i], mappingNode.Content[i+1])
	}

	return config.ProcessorConfig{
		Type: typVal,
		Raw:  *rawMapping,
	}
}

func TestE2E_ValidateProcessor_DropsOversizedSpan(t *testing.T) {
	cfg := baseConfig()
	cfg.Processors = []config.ProcessorConfig{
		processorCfg(t, "type: validate\nmax_span_bytes: 80"),
	}
	a, cap := startApp(t, cfg)

	// Build a span with a very long name (200 chars) which will exceed 80 bytes.
	longName := string(make([]byte, 200))
	req := makeSpanRequest(
		[16]byte{60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		[8]byte{60, 0, 0, 0, 0, 0, 0, 1},
		longName,
		false,
	)

	sendTracesHTTP(t, a.OTLPHTTPAddr(), req)
	time.Sleep(100 * time.Millisecond)

	if cap.count() != 0 {
		t.Errorf("expected 0 spans (oversized dropped), got %d", cap.count())
	}
}

func TestE2E_AttributesProcessor_DeletesKey(t *testing.T) {
	cfg := baseConfig()
	cfg.Processors = []config.ProcessorConfig{
		processorCfg(t, "type: attributes\nactions:\n  - action: delete\n    key: sensitive"),
	}
	cap := &capturingExporter{}
	a, err := app.NewWithExporter(cfg, nil, cap)
	if err != nil {
		t.Fatalf("NewWithExporter: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := a.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		sc, scCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scCancel()
		a.Shutdown(sc)
	})

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{StringValue: "attr-svc"},
					}},
				},
			},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					TraceId:           []byte{70, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
					SpanId:            []byte{70, 0, 0, 0, 0, 0, 0, 1},
					Name:              "attr-op",
					StartTimeUnixNano: uint64(time.Now().UnixNano()),
					EndTimeUnixNano:   uint64(time.Now().Add(time.Millisecond).UnixNano()),
					Attributes: []*commonpb.KeyValue{
						{Key: "sensitive", Value: &commonpb.AnyValue{
							Value: &commonpb.AnyValue_StringValue{StringValue: "secret"},
						}},
						{Key: "safe", Value: &commonpb.AnyValue{
							Value: &commonpb.AnyValue_StringValue{StringValue: "ok"},
						}},
					},
				}},
			}},
		}},
	}

	sendTracesHTTP(t, a.OTLPHTTPAddr(), req)
	waitFor(t, 2*time.Second, func() bool { return cap.count() >= 1 })

	spans := cap.spans
	if len(spans) == 0 {
		t.Fatal("no spans exported")
	}
	for _, s := range spans {
		for _, attr := range s.Attrs {
			if attr.Key == "sensitive" {
				t.Errorf("sensitive attribute must be deleted, found value=%q", attr.Value.String())
			}
		}
	}
}

// TestE2E_TailSampling_ErrorTraceKept verifies that the error rule keeps error
// traces and resumes the pipeline after the sampler.
func TestE2E_TailSampling_ErrorTraceKept(t *testing.T) {
	cap := &capturingExporter{}
	p := pipeline.New[model.Batch](pipeline.Config{Workers: 2, QueueSize: 256}, slog.Default())

	ts := sampling.NewTail(
		30*time.Millisecond,
		1000,
		0.0,
		[]sampling.Rule{sampling.ErrorRule{}},
		func(ctx context.Context, b model.Batch) error {
			return p.ExportFrom(ctx, b, 1)
		},
	)

	p.AddProcessor(ts)
	p.AddExporter(cap)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		sc, scCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scCancel()
		p.Shutdown(sc)
	})

	if err := p.Start(ctx); err != nil {
		t.Fatalf("pipeline.Start: %v", err)
	}
	ts.Start(ctx)
	addr := startTraceIngress(t, p)

	req := makeSpanRequest(
		[16]byte{80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		[8]byte{80, 0, 0, 0, 0, 0, 0, 1},
		"error-op",
		true,
	)
	sendTracesHTTP(t, addr, req)

	waitFor(t, 500*time.Millisecond, func() bool { return cap.count() >= 1 })
	if cap.count() == 0 {
		t.Error("error trace must be kept by tail sampler")
	}
}

// TestE2E_TailSampling_CleanTraceDropped verifies that a clean trace is dropped
// when the sampler has only an error rule and a zero default keep rate.
func TestE2E_TailSampling_CleanTraceDropped(t *testing.T) {
	cap := &capturingExporter{}
	p := pipeline.New[model.Batch](pipeline.Config{Workers: 2, QueueSize: 256}, slog.Default())

	ts := sampling.NewTail(
		30*time.Millisecond,
		1000,
		0.0,
		[]sampling.Rule{sampling.ErrorRule{}},
		func(ctx context.Context, b model.Batch) error {
			return p.ExportFrom(ctx, b, 1)
		},
	)

	p.AddProcessor(ts)
	p.AddExporter(cap)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		sc, scCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scCancel()
		p.Shutdown(sc)
	})

	if err := p.Start(ctx); err != nil {
		t.Fatalf("pipeline.Start: %v", err)
	}
	ts.Start(ctx)
	addr := startTraceIngress(t, p)

	req := makeSpanRequest(
		[16]byte{81, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		[8]byte{81, 0, 0, 0, 0, 0, 0, 1},
		"clean-op",
		false,
	)
	sendTracesHTTP(t, addr, req)

	// Wait well beyond decisionWait (30ms) to let the tail sampler decide.
	time.Sleep(200 * time.Millisecond)

	if cap.count() != 0 {
		t.Errorf("clean trace must be dropped, got %d spans", cap.count())
	}
}

// TestE2E_UnifiedEndpoint_AllSignals is the headline P0-1 check: traces,
// metrics, and logs sent to the SAME OTLP ports (gRPC and HTTP) each reach their
// pipeline and are forwarded to amber. Before the unified ingress, metrics on
// the trace port returned Unimplemented and /v1/metrics returned 404.
func TestE2E_UnifiedEndpoint_AllSignals(t *testing.T) {
	var mu sync.Mutex
	hits := map[string]int{}
	amber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits[r.URL.Path]++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer amber.Close()

	cfg := config.Config{
		Pipeline: config.PipelineConfig{Workers: 2, QueueSize: 256},
		Receivers: config.ReceiversConfig{
			OTLPHTTP: &config.OTLPEndpointConfig{Endpoint: "127.0.0.1:0"},
			OTLPGRPC: &config.OTLPEndpointConfig{Endpoint: "127.0.0.1:0"},
		},
		Exporters: []config.ExporterConfig{ymlExporter(t, "type: amber\nendpoint: "+amber.URL+"/v1/traces")},
		MetricPipeline: &config.MetricPipelineConfig{
			Exporters: []config.MetricExporterConfig{{Type: "amber", Endpoint: amber.URL}},
		},
		LogPipeline: &config.LogPipelineConfig{
			Exporters: []config.LogExporterConfig{{Type: "amber", Endpoint: amber.URL}},
		},
	}

	a, err := app.New(cfg, nil)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := a.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		sc, scCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer scCancel()
		_ = a.Shutdown(sc)
	})

	// Traces over gRPC, metrics and logs over HTTP — all to the one ingress.
	sendTracesGRPC(t, a.OTLPGRPCAddr(), makeSpanRequest(
		[16]byte{1}, [8]byte{1}, "unified-trace", false))
	httpAddr := a.OTLPHTTPAddr()
	postProtoPath(t, httpAddr, "/v1/metrics", &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{
				Name: "unified_metric",
				Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{
					TimeUnixNano: 1, Value: &metricspb.NumberDataPoint_AsInt{AsInt: 1},
				}}}},
			}}}},
		}},
	})
	postProtoPath(t, httpAddr, "/v1/logs", &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
				TimeUnixNano: 1,
				Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "unified-log"}},
			}}}},
		}},
	})

	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return hits["/v1/traces"] >= 1 && hits["/v1/metrics"] >= 1 && hits["/v1/logs"] >= 1
	})
}

// TestE2E_LogRedaction drives a secret-bearing log through the app's log
// pipeline (ingress → redact → amber) and asserts amber receives the scrubbed
// record — the P1-7 redaction the review deferred to the P0-1 session.
func TestE2E_LogRedaction(t *testing.T) {
	var mu sync.Mutex
	var got *collogspb.ExportLogsServiceRequest
	amber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		req := &collogspb.ExportLogsServiceRequest{}
		_ = proto.Unmarshal(body, req)
		mu.Lock()
		got = req
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer amber.Close()

	cfg := config.Config{
		Pipeline:  config.PipelineConfig{Workers: 1, QueueSize: 64},
		Receivers: config.ReceiversConfig{OTLPHTTP: &config.OTLPEndpointConfig{Endpoint: "127.0.0.1:0"}},
		LogPipeline: &config.LogPipelineConfig{
			Processors: []config.ProcessorConfig{processorCfg(t, "type: redact\ncreds_patterns:\n  - '(?i)authorization|password'")},
			Exporters:  []config.LogExporterConfig{{Type: "amber", Endpoint: amber.URL}},
		},
	}
	a, err := app.New(cfg, nil)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := a.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		sc, scCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer scCancel()
		_ = a.Shutdown(sc)
	})

	postProtoPath(t, a.OTLPHTTPAddr(), "/v1/logs", &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
				TimeUnixNano: 1,
				Attributes: []*commonpb.KeyValue{{Key: "authorization", Value: &commonpb.AnyValue{
					Value: &commonpb.AnyValue_StringValue{StringValue: "Bearer xyz"}}}},
				Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "password=hunter2"}},
			}}}},
		}},
	})

	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return got != nil
	})

	mu.Lock()
	defer mu.Unlock()
	rec := got.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	if got := rec.Body.GetStringValue(); got != "[REDACTED]" {
		t.Errorf("log body not redacted at amber: %q", got)
	}
	if got := rec.Attributes[0].Value.GetStringValue(); got != "[REDACTED]" {
		t.Errorf("authorization attr not redacted at amber: %q", got)
	}
}

// TestE2E_PartialSuccess_OversizedSpanReported drives a batch of one oversized
// and one valid span through the app: the oversized span is rejected at accept
// time (validate.max_span_bytes) and reported via partial_success, while the
// valid span still reaches the exporter (contract §4, B-3).
func TestE2E_PartialSuccess_OversizedSpanReported(t *testing.T) {
	cfg := baseConfig()
	cfg.Processors = []config.ProcessorConfig{processorCfg(t, "type: validate\nmax_span_bytes: 150")}
	a, cap := startApp(t, cfg)

	big := makeSpanRequest([16]byte{0xB1}, [8]byte{0xB1}, string(make([]byte, 300)), false)
	small := makeSpanRequest([16]byte{0x51}, [8]byte{0x51}, "ok", false)
	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: append(big.ResourceSpans, small.ResourceSpans...),
	}

	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post("http://"+a.OTLPHTTPAddr()+"/v1/traces", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rb, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out coltracepb.ExportTraceServiceResponse
	if err := proto.Unmarshal(rb, &out); err != nil {
		t.Fatal(err)
	}
	if out.GetPartialSuccess().GetRejectedSpans() != 1 {
		t.Errorf("rejected_spans = %d, want 1", out.GetPartialSuccess().GetRejectedSpans())
	}

	waitFor(t, 2*time.Second, func() bool { return cap.count() == 1 })
	if cap.count() != 1 {
		t.Errorf("exported = %d, want 1 (valid span kept)", cap.count())
	}
}

// TestE2E_ServiceNameEnforced sends a metric and a log with no service.name and
// asserts coral stamps service.name=unknown_service before forwarding to amber
// (contract §6), for both the metric and log pipelines.
func TestE2E_ServiceNameEnforced(t *testing.T) {
	var mu sync.Mutex
	var gotM *colmetricspb.ExportMetricsServiceRequest
	var gotL *collogspb.ExportLogsServiceRequest
	amber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		switch r.URL.Path {
		case "/v1/metrics":
			m := &colmetricspb.ExportMetricsServiceRequest{}
			_ = proto.Unmarshal(body, m)
			gotM = m
		case "/v1/logs":
			l := &collogspb.ExportLogsServiceRequest{}
			_ = proto.Unmarshal(body, l)
			gotL = l
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer amber.Close()

	cfg := config.Config{
		Pipeline:       config.PipelineConfig{Workers: 1, QueueSize: 64},
		Receivers:      config.ReceiversConfig{OTLPHTTP: &config.OTLPEndpointConfig{Endpoint: "127.0.0.1:0"}},
		MetricPipeline: &config.MetricPipelineConfig{Exporters: []config.MetricExporterConfig{{Type: "amber", Endpoint: amber.URL}}},
		LogPipeline:    &config.LogPipelineConfig{Exporters: []config.LogExporterConfig{{Type: "amber", Endpoint: amber.URL}}},
	}
	a, err := app.New(cfg, nil)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := a.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		sc, scCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer scCancel()
		_ = a.Shutdown(sc)
	})

	// No Resource / service.name on either payload.
	postProtoPath(t, a.OTLPHTTPAddr(), "/v1/metrics", &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{
				Name: "m",
				Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{
					TimeUnixNano: 1, Value: &metricspb.NumberDataPoint_AsInt{AsInt: 1},
				}}}},
			}}}},
		}},
	})
	postProtoPath(t, a.OTLPHTTPAddr(), "/v1/logs", &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{TimeUnixNano: 1}}}},
		}},
	})

	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return gotM != nil && gotL != nil
	})

	mu.Lock()
	defer mu.Unlock()
	if v := resourceServiceName(gotM.ResourceMetrics[0].GetResource()); v != "unknown_service" {
		t.Errorf("metric service.name = %q, want unknown_service", v)
	}
	if v := resourceServiceName(gotL.ResourceLogs[0].GetResource()); v != "unknown_service" {
		t.Errorf("log service.name = %q, want unknown_service", v)
	}
}

func resourceServiceName(res *resourcepb.Resource) string {
	for _, kv := range res.GetAttributes() {
		if kv.GetKey() == "service.name" {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

func TestE2E_MultiReceiver_BothDeliver(t *testing.T) {
	cfg := baseConfig()
	cap := &capturingExporter{}
	a, err := app.NewWithExporter(cfg, nil, cap)
	if err != nil {
		t.Fatalf("NewWithExporter: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := a.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		sc, scCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scCancel()
		a.Shutdown(sc)
	})

	httpReq := makeSpanRequest(
		[16]byte{90, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		[8]byte{90, 0, 0, 0, 0, 0, 0, 1},
		"http-span",
		false,
	)
	grpcReq := makeSpanRequest(
		[16]byte{91, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		[8]byte{91, 0, 0, 0, 0, 0, 0, 1},
		"grpc-span",
		false,
	)

	sendTracesHTTP(t, a.OTLPHTTPAddr(), httpReq)
	sendTracesGRPC(t, a.OTLPGRPCAddr(), grpcReq)

	waitFor(t, 2*time.Second, func() bool { return cap.count() >= 2 })
	if cap.count() < 2 {
		t.Errorf("expected 2 spans (1 HTTP + 1 gRPC), got %d", cap.count())
	}
}
