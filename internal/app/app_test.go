package app

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yaop-labs/coral/internal/config"
	"github.com/yaop-labs/gyre"
	"github.com/yaop-labs/reef/bearer"
)

func testConfig() config.Config {
	return config.Config{
		Pipeline: config.PipelineConfig{Workers: 1, QueueSize: 64},
		Receivers: config.ReceiversConfig{
			OTLPGRPC: &config.OTLPEndpointConfig{Endpoint: "127.0.0.1:0"},
			OTLPHTTP: &config.OTLPEndpointConfig{Endpoint: "127.0.0.1:0"},
		},
		Exporters: []config.ExporterConfig{{Type: "devnull"}},
	}
}

func TestApp_New_ValidConfig(t *testing.T) {
	_, err := New(testConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
}

func TestApp_StartShutdownIsClean(t *testing.T) {
	a, err := New(testConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := t.Context()

	if err := a.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	if err := a.Shutdown(stopCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestApp_SelfObsMux(t *testing.T) {
	a, err := New(testConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := a.selfObsMux(a.pipeline)

	if code := selfObsGet(t, h, "/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz before ready = %d, want 503", code)
	}
	a.transition(gyre.StateReady, "test", "ready for test")
	if code := selfObsGet(t, h, "/readyz"); code != http.StatusOK {
		t.Errorf("/readyz after ready = %d, want 200", code)
	}
	if code := selfObsGet(t, h, "/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", code)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/status = %d, want 200", rec.Code)
	}
	var status gyre.Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode /status: %v", err)
	}
	if status.Name != "coral" || status.State != gyre.StateReady {
		t.Errorf("/status = %+v", status)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	if !strings.Contains(body, "coral_batches_in") {
		t.Errorf("/metrics missing coral_* metric:\n%s", body)
	}
	if !strings.Contains(body, "coral_otlp_rejected_spans") {
		t.Errorf("/metrics missing ingress counters:\n%s", body)
	}
	if !strings.Contains(body, "coral_otlp_log_limit_rejected") {
		t.Errorf("/metrics missing log limit counter:\n%s", body)
	}
	if !strings.Contains(body, "coral_otlp_metric_limit_rejected") {
		t.Errorf("/metrics missing metric limit counter:\n%s", body)
	}
	if !strings.Contains(body, "coral_otlp_tenant_accepted") {
		t.Errorf("/metrics missing tenant admission counter:\n%s", body)
	}
	for _, metric := range []string{"coral_wisp_dedup_hits", "coral_wisp_dedup_conflicts", "coral_wisp_dedup_misses", "coral_wisp_dedup_evictions"} {
		if !strings.Contains(body, metric) {
			t.Errorf("/metrics missing %s", metric)
		}
	}
	for _, metric := range []string{
		"coral_build_info{",
		"coral_ready 1",
		`coral_readiness_state{state="ready"} 1`,
		`coral_pipeline_queue_depth{signal="traces"}`,
		`coral_pipeline_queue_capacity{signal="traces"} 64`,
		`coral_pipeline_items_processed_total{signal="traces"}`,
		`coral_pipeline_items_delivered_total{signal="traces"}`,
		`coral_pipeline_drain_outcome{signal="traces",outcome="not_started"} 1`,
		`coral_credential_events_total{kind="server_leaf",outcome="success"} 0`,
	} {
		if !strings.Contains(body, metric) {
			t.Errorf("/metrics missing %q:\n%s", metric, body)
		}
	}
	if strings.Contains(body, "collector_") {
		t.Errorf("/metrics still uses the legacy collector_ prefix:\n%s", body)
	}
}

func TestApp_ReefEdgePolicyRequiresExternalPlaintextOptIn(t *testing.T) {
	cfg := testConfig()
	cfg.Receivers.OTLPGRPC.Endpoint = "0.0.0.0:4317"
	_, err := New(cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "insecure: true") {
		t.Fatalf("external OTLP plaintext error = %v", err)
	}
	cfg.Receivers.OTLPGRPC.Insecure = true
	if _, err := New(cfg, nil); err != nil {
		t.Fatalf("explicitly insecure OTLP config: %v", err)
	}

	cfg = testConfig()
	cfg.Metrics.Endpoint = "0.0.0.0:4888"
	_, err = New(cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "insecure: true") {
		t.Fatalf("external metrics plaintext error = %v", err)
	}
	cfg.Metrics.Insecure = true
	if _, err := New(cfg, nil); err != nil {
		t.Fatalf("explicitly insecure metrics config: %v", err)
	}
}

func TestApp_SelfObservabilityReefAuthBoundary(t *testing.T) {
	cfg := testConfig()
	cfg.Metrics = config.MetricsConfig{
		Endpoint: "127.0.0.1:0",
		Auth: &bearer.ServerConfig{Bearer: []bearer.Key{{
			Name:  "operator",
			Token: "metrics-secret",
		}}},
		EdgePolicyConfig: config.EdgePolicyConfig{
			DangerAllowBearerOverPlaintext: true,
		},
	}
	a, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = a.Close(ctx)
	}()

	base := "http://" + a.SelfObservabilityAddr()
	if code := appHTTPStatus(t, base+"/healthz", ""); code != http.StatusOK {
		t.Fatalf("unauthenticated /healthz = %d, want 200", code)
	}
	if code := appHTTPStatus(t, base+"/readyz", ""); code != http.StatusOK {
		t.Fatalf("unauthenticated /readyz = %d, want 200", code)
	}
	if code := appHTTPStatus(t, base+"/metrics", ""); code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated /metrics = %d, want 401", code)
	}
	if code := appHTTPStatus(t, base+"/status", ""); code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated /status = %d, want 401", code)
	}
	if code := appHTTPStatus(t, base+"/metrics", "metrics-secret"); code != http.StatusOK {
		t.Fatalf("authenticated /metrics = %d, want 200", code)
	}
}

func appHTTPStatus(t *testing.T, url, token string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func TestApp_GyreConformanceCloseBeforeStart(t *testing.T) {
	a, err := New(testConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := gyre.ConformanceCheck(t.Context(), a); err != nil {
		t.Fatalf("Gyre conformance: %v", err)
	}
	if got := a.Status().State; got != gyre.StateStopped {
		t.Fatalf("state after close-before-start = %q, want %q", got, gyre.StateStopped)
	}
	err = a.Start(t.Context())
	var gyreErr *gyre.Error
	if !errors.As(err, &gyreErr) || gyreErr.Code != gyre.CodeShuttingDown {
		t.Fatalf("Start after Close error = %v, want Gyre shutting_down", err)
	}
}

func TestApp_CloseIsIdempotent(t *testing.T) {
	a, err := New(testConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.Close(stopCtx); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := a.Close(stopCtx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := a.Status().State; got != gyre.StateStopped {
		t.Fatalf("state = %q, want %q", got, gyre.StateStopped)
	}
}

func TestApp_CloseHonorsContextWhileCleanupContinues(t *testing.T) {
	a, err := New(testConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	release := make(chan struct{})
	a.hooks = append(a.hooks, lifecycleHook{
		start: func(context.Context) error { return nil },
		stop: func(context.Context) error {
			<-release
			return nil
		},
	})
	if err := a.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = a.Close(stopCtx)
	var gyreErr *gyre.Error
	if !errors.As(err, &gyreErr) || gyreErr.Code != gyre.CodeShuttingDown {
		t.Fatalf("Close error = %v, want Gyre shutting_down", err)
	}

	close(release)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := a.Close(waitCtx); err != nil {
		t.Fatalf("Close after cleanup release: %v", err)
	}
	if got := a.Status().State; got != gyre.StateStopped {
		t.Fatalf("state = %q, want %q", got, gyre.StateStopped)
	}
}

func TestApp_StartFailureRollsBackStartedResources(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy ingress address: %v", err)
	}
	defer occupied.Close()

	metricsProbe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve metrics address: %v", err)
	}
	metricsAddr := metricsProbe.Addr().String()
	if err := metricsProbe.Close(); err != nil {
		t.Fatalf("release metrics address: %v", err)
	}

	cfg := testConfig()
	cfg.Metrics.Endpoint = metricsAddr
	cfg.Receivers.OTLPGRPC.Endpoint = occupied.Addr().String()
	cfg.Receivers.OTLPHTTP = nil
	a, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = a.Start(t.Context())
	var gyreErr *gyre.Error
	if !errors.As(err, &gyreErr) || gyreErr.Code != gyre.CodeUnavailable {
		t.Fatalf("Start error = %v, want Gyre unavailable", err)
	}
	if got := a.Status().State; got != gyre.StateFailed {
		t.Fatalf("state = %q, want %q", got, gyre.StateFailed)
	}

	rebound, err := net.Listen("tcp", metricsAddr)
	if err != nil {
		t.Fatalf("metrics listener leaked after failed Start: %v", err)
	}
	_ = rebound.Close()
	if err := a.Close(t.Context()); err != nil {
		t.Fatalf("Close after failed Start: %v", err)
	}
}

func TestPrometheusLabelValue(t *testing.T) {
	got := prometheusLabelValue("a\\b\"\n")
	if got != `a\\b\"\n` {
		t.Fatalf("prometheusLabelValue() = %q", got)
	}
}

func selfObsGet(t *testing.T, h http.Handler, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

func TestApp_NoReceivers(t *testing.T) {
	cfg := config.Config{
		Pipeline:  config.PipelineConfig{Workers: 1, QueueSize: 16},
		Exporters: []config.ExporterConfig{{Type: "devnull"}},
	}
	_, err := New(cfg, nil)
	if err == nil {
		t.Fatal("expected error without receivers")
	}
}

func TestApp_UnknownExporterType(t *testing.T) {
	cfg := testConfig()
	cfg.Exporters = []config.ExporterConfig{{Type: "kafka"}}
	_, err := New(cfg, nil)
	if err == nil {
		t.Fatal("expected error for unknown exporter type")
	}
}

func TestApp_UnknownProcessorType(t *testing.T) {
	cfg := testConfig()
	cfg.Processors = []config.ProcessorConfig{{Type: "magic"}}
	_, err := New(cfg, nil)
	if err == nil {
		t.Fatal("expected error for unknown processor type")
	}
}

func TestApp_ValidateProcessor(t *testing.T) {
	cfg := testConfig()
	cfg.Processors = []config.ProcessorConfig{{Type: "validate"}}
	_, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New with validate processor: %v", err)
	}
}

func TestApp_TailSamplingProcessor(t *testing.T) {
	cfg := testConfig()
	cfg.Processors = []config.ProcessorConfig{{Type: "tail_sampling"}}
	a, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New with tail_sampling processor: %v", err)
	}
	ctx := t.Context()
	if err := a.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	if err := a.Shutdown(stopCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestApp_TailSamplerMetrics(t *testing.T) {
	cfg := testConfig()
	cfg.Processors = []config.ProcessorConfig{{Type: "tail_sampling"}}
	a, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New with tail_sampling processor: %v", err)
	}
	a.transition(gyre.StateReady, "test", "ready")
	rec := httptest.NewRecorder()
	a.selfObsMux(a.pipeline).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, name := range []string{"coral_trace_sampler_pending_traces", "coral_trace_sampler_pending_bytes", "coral_trace_sampler_evictions_total", "coral_trace_sampler_late_spans_total"} {
		if !strings.Contains(rec.Body.String(), name) {
			t.Errorf("/metrics missing %s", name)
		}
	}
}

func TestApp_HeadSamplingProcessorRemoved(t *testing.T) {
	cfg := testConfig()
	cfg.Processors = []config.ProcessorConfig{{Type: "head_sampling"}}
	_, err := New(cfg, nil)
	if err == nil {
		t.Fatal("expected error for removed head_sampling processor")
	}
}

func TestApp_AllReceivers(t *testing.T) {
	cfg := config.Config{
		Pipeline: config.PipelineConfig{Workers: 1, QueueSize: 16},
		Receivers: config.ReceiversConfig{
			OTLPGRPC:        &config.OTLPEndpointConfig{Endpoint: "127.0.0.1:0"},
			OTLPHTTP:        &config.OTLPEndpointConfig{Endpoint: "127.0.0.1:0"},
			JaegerThriftUDP: &config.UDPConfig{Endpoint: "127.0.0.1:0"},
			JaegerThriftTCP: &config.EndpointConfig{Endpoint: "127.0.0.1:0"},
			ZipkinHTTP:      &config.EndpointConfig{Endpoint: "127.0.0.1:0"},
		},
		Exporters: []config.ExporterConfig{{Type: "devnull"}},
	}
	a, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New with all receivers: %v", err)
	}
	ctx := t.Context()
	if err := a.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	if err := a.Shutdown(stopCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
