package integration

import (
	"bytes"
	"context"
	"log/slog"
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
	"gopkg.in/yaml.v3"

	"github.com/hnlbs/collector/internal/app"
	"github.com/hnlbs/collector/internal/config"
	"github.com/hnlbs/collector/internal/model"
	"github.com/hnlbs/collector/internal/pipeline"
	"github.com/hnlbs/collector/internal/processor/sampling"
	otlprecv "github.com/hnlbs/collector/internal/receiver/otlp"
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
			OTLPHTTP: &config.EndpointConfig{Endpoint: "127.0.0.1:0"},
			OTLPGRPC: &config.EndpointConfig{Endpoint: "127.0.0.1:0"},
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

var _ pipeline.Receiver = (*fakeReceiver)(nil)

func TestIntegration_FakeReceiver_SpanFlowsThroughPipeline(t *testing.T) {
	cap := &capturingExporter{}
	recv := newFakeReceiver()

	p := pipeline.New(pipeline.Config{Workers: 1, QueueSize: 16}, slog.Default())
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
	p := pipeline.New(pipeline.Config{Workers: 2, QueueSize: 256}, slog.Default())

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

	httpRecv, err := otlprecv.NewHTTP("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}
	p.AddReceiver(httpRecv)

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
	time.Sleep(5 * time.Millisecond) // wait for receiver to bind

	req := makeSpanRequest(
		[16]byte{80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		[8]byte{80, 0, 0, 0, 0, 0, 0, 1},
		"error-op",
		true,
	)
	sendTracesHTTP(t, httpRecv.Addr(), req)

	waitFor(t, 500*time.Millisecond, func() bool { return cap.count() >= 1 })
	if cap.count() == 0 {
		t.Error("error trace must be kept by tail sampler")
	}
}

// TestE2E_TailSampling_CleanTraceDropped verifies that a clean trace is dropped
// when the sampler has only an error rule and a zero default keep rate.
func TestE2E_TailSampling_CleanTraceDropped(t *testing.T) {
	cap := &capturingExporter{}
	p := pipeline.New(pipeline.Config{Workers: 2, QueueSize: 256}, slog.Default())

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

	httpRecv, err := otlprecv.NewHTTP("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}
	p.AddReceiver(httpRecv)

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
	time.Sleep(5 * time.Millisecond) // wait for receiver to bind

	req := makeSpanRequest(
		[16]byte{81, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		[8]byte{81, 0, 0, 0, 0, 0, 0, 1},
		"clean-op",
		false,
	)
	sendTracesHTTP(t, httpRecv.Addr(), req)

	// Wait well beyond decisionWait (30ms) to let the tail sampler decide.
	time.Sleep(200 * time.Millisecond)

	if cap.count() != 0 {
		t.Errorf("clean trace must be dropped, got %d spans", cap.count())
	}
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
