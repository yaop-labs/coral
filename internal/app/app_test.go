package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yaop-labs/coral/internal/config"
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
	a.ready.Store(true)
	if code := selfObsGet(t, h, "/readyz"); code != http.StatusOK {
		t.Errorf("/readyz after ready = %d, want 200", code)
	}
	if code := selfObsGet(t, h, "/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", code)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	if !strings.Contains(body, "coral_batches_in") {
		t.Errorf("/metrics missing coral_* metric:\n%s", body)
	}
	if !strings.Contains(body, "coral_otlp_rejected_spans") {
		t.Errorf("/metrics missing ingress counters:\n%s", body)
	}
	if strings.Contains(body, "collector_") {
		t.Errorf("/metrics still uses the legacy collector_ prefix:\n%s", body)
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
