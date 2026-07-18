package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestPipelineBounds(t *testing.T) {
	for _, tc := range []struct {
		name string
		yaml string
	}{
		{"negative workers", "pipeline:\n  workers: -1\n"},
		{"excess workers", "pipeline:\n  workers: 1025\n"},
		{"negative queue", "pipeline:\n  queue_size: -1\n"},
		{"excess queue", "pipeline:\n  queue_size: 1000001\n"},
		{"negative queue bytes", "pipeline:\n  queue_bytes: -1\n"},
		{"excess queue bytes", "pipeline:\n  queue_bytes: 1099511627777\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Parse([]byte(tc.yaml)); err == nil {
				t.Fatal("Parse succeeded for invalid pipeline bounds")
			}
		})
	}
}

func TestJournalBounds(t *testing.T) {
	if _, err := Parse([]byte("journal_max_bytes: -1\n")); err == nil {
		t.Fatal("accepted negative journal budget")
	}
	if _, err := Parse([]byte("journal_max_bytes: 1099511627777\n")); err == nil {
		t.Fatal("accepted excessive journal budget")
	}
}

func TestTenantLimitBounds(t *testing.T) {
	if _, err := Parse([]byte("tenant_limits:\n  a:\n    max_items: -1\n")); err == nil {
		t.Fatal("accepted negative tenant items")
	}
	if _, err := Parse([]byte("tenant_limits:\n  a:\n    max_bytes: 1099511627777\n")); err == nil {
		t.Fatal("accepted excessive tenant bytes")
	}
	if _, err := Parse([]byte("tenant_limits:\n  a:\n    max_requests_per_second: 1000001\n")); err == nil {
		t.Fatal("accepted excessive tenant request rate")
	}
	if _, err := Parse([]byte("tenant_limits:\n  a:\n    max_log_record_bytes: 67108865\n")); err == nil {
		t.Fatal("accepted excessive log record limit")
	}
	if _, err := Parse([]byte("tenant_limits:\n  a:\n    max_log_attributes: 100001\n")); err == nil {
		t.Fatal("accepted excessive log attribute limit")
	}
	if _, err := Parse([]byte("tenant_limits:\n  a:\n    max_log_attribute_keys: 1000001\n")); err == nil {
		t.Fatal("accepted excessive log attribute key limit")
	}
	if _, err := Parse([]byte("tenant_limits:\n  a:\n    max_metric_attributes: 1000001\n")); err == nil {
		t.Fatal("accepted excessive metric attribute limit")
	}
}

func validConfig() Config {
	return Config{
		Receivers: ReceiversConfig{
			OTLPGRPC: &OTLPEndpointConfig{Endpoint: "127.0.0.1:4317"},
		},
		Exporters: []ExporterConfig{{Type: "devnull"}},
	}
}

func TestDuration_UnmarshalYAML(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{"30s", 30 * time.Second, false},
		{"5m", 5 * time.Minute, false},
		{"1h30m", 90 * time.Minute, false},
		{"500ms", 500 * time.Millisecond, false},
		{"garbage", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			type wrapper struct {
				D Duration `yaml:"d"`
			}
			var w wrapper
			err := yaml.Unmarshal([]byte("d: "+tt.input), &w)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error parsing %q", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse %q: %v", tt.input, err)
			}
			if got := w.D.Std(); got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEdgePolicy_ReloadIntervalBounds(t *testing.T) {
	for _, interval := range []time.Duration{-time.Second, time.Millisecond, 25 * time.Hour} {
		cfg := validConfig()
		cfg.Receivers.OTLPGRPC.CredentialReloadInterval = Duration(interval)
		if err := cfg.Validate(); err == nil {
			t.Fatalf("Validate interval %s: expected error", interval)
		}
	}
	cfg := validConfig()
	cfg.Receivers.OTLPGRPC.CredentialReloadInterval = Duration(time.Second)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate 1s: %v", err)
	}
}

func TestParse_Receivers(t *testing.T) {
	doc := []byte(`
receivers:
  otlp_grpc:
    endpoint: "0.0.0.0:4317"
    insecure: true
    credential_reload_interval: 2s
  jaeger_thrift_http:
    endpoint: "0.0.0.0:14250"
  zipkin_http:
    endpoint: "0.0.0.0:9411"
exporters:
  - type: devnull
`)
	cfg, err := Parse(doc)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Receivers.OTLPGRPC == nil || cfg.Receivers.OTLPGRPC.Endpoint != "0.0.0.0:4317" {
		t.Errorf("otlp_grpc not parsed: %+v", cfg.Receivers.OTLPGRPC)
	}
	if !cfg.Receivers.OTLPGRPC.Insecure ||
		cfg.Receivers.OTLPGRPC.CredentialReloadInterval.Std() != 2*time.Second {
		t.Errorf("otlp_grpc edge policy not parsed: %+v", cfg.Receivers.OTLPGRPC.EdgePolicyConfig)
	}
	if cfg.Receivers.JaegerThriftHTTP == nil || cfg.Receivers.JaegerThriftHTTP.Endpoint != "0.0.0.0:14250" {
		t.Errorf("jaeger_thrift_http not parsed: %+v", cfg.Receivers.JaegerThriftHTTP)
	}
	if cfg.Receivers.ZipkinHTTP == nil {
		t.Errorf("zipkin_http not parsed")
	}
}

func TestParse_Processors(t *testing.T) {
	doc := []byte(`
receivers:
  otlp_grpc:
    endpoint: "127.0.0.1:4317"
processors:
  - type: validate
    max_span_bytes: 65536
  - type: tail_sampling
    decision_wait: 30s
    max_traces: 100000
    default_keep_rate: 0.1
exporters:
  - type: devnull
`)
	cfg, err := Parse(doc)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(cfg.Processors) != 2 {
		t.Fatalf("expected 2 processors, got %d", len(cfg.Processors))
	}
	if cfg.Processors[0].Type != "validate" {
		t.Errorf("processors[0].type = %q", cfg.Processors[0].Type)
	}
	if cfg.Processors[1].Type != "tail_sampling" {
		t.Errorf("processors[1].type = %q", cfg.Processors[1].Type)
	}

	// Regression guard: typed fields must decode from Raw. go-yaml's inline
	// yaml.Node silently dropped these, so a ValidateConfig came back all-zero.
	var vc ValidateConfig
	if err := cfg.Processors[0].Raw.Decode(&vc); err != nil {
		t.Fatal(err)
	}
	if vc.MaxSpanBytes != 65536 {
		t.Errorf("validate.max_span_bytes = %d, want 65536 (Raw not captured?)", vc.MaxSpanBytes)
	}
	var ts TailSamplingConfig
	if err := cfg.Processors[1].Raw.Decode(&ts); err != nil {
		t.Fatal(err)
	}
	if ts.MaxTraces != 100000 {
		t.Errorf("tail_sampling.max_traces = %d, want 100000", ts.MaxTraces)
	}
}

func TestParse_Exporters(t *testing.T) {
	doc := []byte(`
receivers:
  otlp_grpc:
    endpoint: "127.0.0.1:4317"
exporters:
  - type: amber
    endpoint: "http://amber:8080"
    timeout: 10s
  - type: fathom
    endpoint: "http://fathom:8099"
    timeout: 5s
  - type: s3
    bucket: "my-traces"
    region: "us-east-1"
`)
	cfg, err := Parse(doc)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(cfg.Exporters) != 3 {
		t.Fatalf("expected 3 exporters, got %d", len(cfg.Exporters))
	}
	if cfg.Exporters[0].Type != "amber" {
		t.Errorf("exporters[0].type = %q", cfg.Exporters[0].Type)
	}
}

func TestParse_MetricExporters(t *testing.T) {
	doc := []byte(`
receivers:
  otlp_http:
    endpoint: "127.0.0.1:4318"
metric_pipeline:
  exporters:
    - type: amber
      endpoint: "http://amber:8080"
    - type: fathom
      endpoint: "http://fathom:8099"
`)
	cfg, err := Parse(doc)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.MetricPipeline == nil {
		t.Fatal("expected metric pipeline")
	}
	if len(cfg.MetricPipeline.Exporters) != 2 {
		t.Fatalf("expected 2 metric exporters, got %d", len(cfg.MetricPipeline.Exporters))
	}
	if cfg.MetricPipeline.Exporters[1].Type != "fathom" {
		t.Fatalf("expected fathom metric exporter, got %+v", cfg.MetricPipeline.Exporters[1])
	}
}

func TestParse_LogExporters(t *testing.T) {
	doc := []byte(`
receivers:
  otlp_http:
    endpoint: "127.0.0.1:4318"
log_pipeline:
  exporters:
    - type: fathom
      endpoint: "http://fathom:8099"
`)
	cfg, err := Parse(doc)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.LogPipeline == nil {
		t.Fatal("expected log pipeline")
	}
	if len(cfg.LogPipeline.Exporters) != 1 {
		t.Fatalf("expected 1 log exporter, got %d", len(cfg.LogPipeline.Exporters))
	}
	if cfg.LogPipeline.Exporters[0].Type != "fathom" {
		t.Fatalf("expected fathom log exporter, got %+v", cfg.LogPipeline.Exporters[0])
	}
}

func TestParse_LogExporter_Amber(t *testing.T) {
	// Logs must be allowed to reach amber (the source of truth); an untyped
	// exporter defaults to amber. Both used to be rejected by validation.
	doc := []byte(`
receivers:
  otlp_http:
    endpoint: "127.0.0.1:4318"
log_pipeline:
  exporters:
    - type: amber
      endpoint: "http://amber:8080"
    - endpoint: "http://amber:8080"
`)
	cfg, err := Parse(doc)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.LogPipeline == nil {
		t.Fatal("expected log pipeline")
	}
	if got := cfg.LogPipeline.Exporters[1].logType(); got != "amber" {
		t.Fatalf("untyped log exporter should default to amber, got %q", got)
	}
}

func TestParse_RedactProcessors(t *testing.T) {
	doc := []byte(`
receivers:
  otlp_http:
    endpoint: "127.0.0.1:4318"
metric_pipeline:
  processors:
    - type: redact
      creds_patterns:
        - '(?i)password'
  exporters:
    - type: amber
      endpoint: "http://amber:5318"
log_pipeline:
  processors:
    - type: redact
      creds_patterns:
        - '(?i)authorization'
  exporters:
    - type: amber
      endpoint: "http://amber:5318"
`)
	cfg, err := Parse(doc)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(cfg.MetricPipeline.Processors) != 1 || cfg.MetricPipeline.Processors[0].Type != "redact" {
		t.Fatalf("metric redact processor not parsed: %+v", cfg.MetricPipeline.Processors)
	}
	if len(cfg.LogPipeline.Processors) != 1 || cfg.LogPipeline.Processors[0].Type != "redact" {
		t.Fatalf("log redact processor not parsed: %+v", cfg.LogPipeline.Processors)
	}
	var rc RedactConfig
	if err := cfg.LogPipeline.Processors[0].Raw.Decode(&rc); err != nil {
		t.Fatal(err)
	}
	if len(rc.CredsPatterns) != 1 || rc.CredsPatterns[0] != "(?i)authorization" {
		t.Errorf("creds_patterns not decoded: %+v", rc.CredsPatterns)
	}
}

func TestValidate_LogPipeline_UnknownProcessor(t *testing.T) {
	doc := []byte(`
receivers:
  otlp_http:
    endpoint: "127.0.0.1:4318"
log_pipeline:
  processors:
    - type: attributes
  exporters:
    - type: amber
      endpoint: "http://amber:5318"
`)
	if _, err := Parse(doc); err == nil {
		t.Fatal("expected error for unknown log processor type")
	}
}

func TestParse_InvalidYAMLReturnsError(t *testing.T) {
	_, err := Parse([]byte("this is: not: valid: yaml"))
	if err == nil {
		t.Fatalf("expected error on invalid yaml")
	}
}

func TestLoad_File(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "collector.yaml")
	doc := `
pipeline:
  workers: 4
  queue_size: 5000
receivers:
  otlp_grpc:
    endpoint: "127.0.0.1:4317"
exporters:
  - type: devnull
`
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Pipeline.Workers != 4 {
		t.Fatalf("workers = %d, want 4", cfg.Pipeline.Workers)
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load("/no/such/path/collector.yaml")
	if err == nil {
		t.Fatalf("expected error for missing file")
	}
	if !strings.Contains(err.Error(), "read") {
		t.Errorf("expected 'read' in error, got: %v", err)
	}
}

func TestValidate_EmptyProcessorType(t *testing.T) {
	cfg := validConfig()
	cfg.Processors = []ProcessorConfig{{Type: ""}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for empty processor type")
	}
	if !strings.Contains(err.Error(), "type is required") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_EmptyExporterType(t *testing.T) {
	cfg := validConfig()
	cfg.Exporters = []ExporterConfig{{Type: ""}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for empty exporter type")
	}
	if !strings.Contains(err.Error(), "type is required") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestUDPConfig_Parsed(t *testing.T) {
	doc := []byte(`
receivers:
  jaeger_thrift_udp:
    endpoint: "0.0.0.0:6831"
    max_packet_size: 65000
exporters:
  - type: devnull
`)
	cfg, err := Parse(doc)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	udp := cfg.Receivers.JaegerThriftUDP
	if udp == nil {
		t.Fatal("jaeger_thrift_udp not parsed")
	}
	if udp.MaxPacketSize != 65000 {
		t.Errorf("max_packet_size = %d, want 65000", udp.MaxPacketSize)
	}
}

func TestValidate_NoReceivers(t *testing.T) {
	cfg := validConfig()
	cfg.Receivers = ReceiversConfig{}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error without receivers")
	}
	if !strings.Contains(err.Error(), "at least one receiver") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_NoExporters(t *testing.T) {
	cfg := validConfig()
	cfg.Exporters = nil
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error without exporters")
	}
	if !strings.Contains(err.Error(), "at least one exporter") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_HeadSamplingRemoved(t *testing.T) {
	cfg := validConfig()
	cfg.Processors = []ProcessorConfig{{Type: "head_sampling"}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for removed head_sampling")
	}
	if !strings.Contains(err.Error(), "head_sampling was removed") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParse_RejectsRemovedPipelineLocalReceivers(t *testing.T) {
	doc := []byte(`
receivers:
  otlp_http:
    endpoint: "127.0.0.1:4318"
metric_pipeline:
  receivers:
    otlp_http:
      endpoint: "127.0.0.1:4320"
  exporters:
    - type: amber
      endpoint: "http://amber:5318"
`)
	_, err := Parse(doc)
	if err == nil {
		t.Fatal("expected old metric_pipeline.receivers to fail loudly")
	}
	if !strings.Contains(err.Error(), "field receivers not found") {
		t.Fatalf("unexpected migration error: %v", err)
	}
}

func TestParse_ReefSecuritySchema(t *testing.T) {
	doc := []byte(`
receivers:
  otlp_grpc:
    endpoint: "127.0.0.1:4317"
    tls:
      enabled: true
      cert_file: server.crt
      key_file: server.key
      min_version: "1.3"
    auth:
      bearer:
        - name: wisp
          token_file: ingress.token
exporters:
  - type: devnull
metric_pipeline:
  exporters:
    - type: amber
      endpoint: https://amber:5318
      tls:
        enabled: true
        ca_file: ca.crt
        server_name: amber
      auth:
        token_file: amber.token
`)
	cfg, err := Parse(doc)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	grpc := cfg.Receivers.OTLPGRPC
	if grpc == nil || grpc.TLS == nil || !grpc.TLS.Enabled {
		t.Fatal("receiver Reef TLS config was not decoded")
	}
	if grpc.Auth == nil || len(grpc.Auth.Bearer) != 1 || grpc.Auth.Bearer[0].Name != "wisp" {
		t.Fatalf("receiver Reef auth config = %#v", grpc.Auth)
	}
	exp := cfg.MetricPipeline.Exporters[0]
	if exp.TLS == nil || exp.TLS.ServerName != "amber" {
		t.Fatalf("exporter Reef TLS config = %#v", exp.TLS)
	}
	if exp.Auth == nil || exp.Auth.TokenFile != "amber.token" {
		t.Fatalf("exporter Reef auth config = %#v", exp.Auth)
	}
}
