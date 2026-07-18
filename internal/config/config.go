package config

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/yaop-labs/reef/bearer"
	"github.com/yaop-labs/reef/tlsconf"
	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration for YAML unmarshaling.
type Duration time.Duration

func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("duration: expected scalar at line %d", node.Line)
	}
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("duration %q: %w", node.Value, err)
	}
	*d = Duration(parsed)
	return nil
}

// Config is the collector configuration.
type Config struct {
	// TenantMap binds authenticated Reef principals to stable tenant IDs.
	TenantMap    map[string]string      `yaml:"tenant_map"`
	TenantLimits map[string]TenantLimit `yaml:"tenant_limits"`
	Pipeline     PipelineConfig         `yaml:"pipeline"`
	Receivers    ReceiversConfig        `yaml:"receivers"`
	Processors   []ProcessorConfig      `yaml:"processors"`
	Exporters    []ExporterConfig       `yaml:"exporters"`
	Metrics      MetricsConfig          `yaml:"metrics"`

	// MetricPipeline is the optional metrics path (wisp → coral → amber),
	// independent of the trace pipeline above.
	MetricPipeline *MetricPipelineConfig `yaml:"metric_pipeline"`

	// LogPipeline is the optional logs path, independent of traces and metrics.
	LogPipeline *LogPipelineConfig `yaml:"log_pipeline"`
}

type TenantLimit struct {
	MaxItems int   `yaml:"max_items"`
	MaxBytes int64 `yaml:"max_bytes"`
}

// PipelineConfig configures pipeline concurrency.
type PipelineConfig struct {
	Workers    int   `yaml:"workers"`
	QueueSize  int   `yaml:"queue_size"`
	QueueBytes int64 `yaml:"queue_bytes"`
}

const (
	maxPipelineWorkers    = 1024
	maxPipelineQueueSize  = 1_000_000
	maxPipelineQueueBytes = 1 << 40
)

func (c PipelineConfig) validate() error {
	if c.Workers < 0 || c.Workers > maxPipelineWorkers {
		return fmt.Errorf("pipeline.workers must be between 0 and %d", maxPipelineWorkers)
	}
	if c.QueueSize < 0 || c.QueueSize > maxPipelineQueueSize {
		return fmt.Errorf("pipeline.queue_size must be between 0 and %d", maxPipelineQueueSize)
	}
	if c.QueueBytes < 0 || c.QueueBytes > maxPipelineQueueBytes {
		return fmt.Errorf("pipeline.queue_bytes must be between 0 and %d", maxPipelineQueueBytes)
	}
	return nil
}

// ReceiversConfig configures trace receivers.
type ReceiversConfig struct {
	OTLPGRPC         *OTLPEndpointConfig `yaml:"otlp_grpc"`
	OTLPHTTP         *OTLPEndpointConfig `yaml:"otlp_http"`
	JaegerThriftHTTP *EndpointConfig     `yaml:"jaeger_thrift_http"`
	JaegerThriftUDP  *UDPConfig          `yaml:"jaeger_thrift_udp"`
	JaegerThriftTCP  *EndpointConfig     `yaml:"jaeger_thrift_tcp"`
	ZipkinHTTP       *EndpointConfig     `yaml:"zipkin_http"`
}

// EndpointConfig configures a TCP or HTTP listener.
type EndpointConfig struct {
	Endpoint string `yaml:"endpoint"`
}

// OTLPEndpointConfig adds transport security to an OTLP listener.
type OTLPEndpointConfig struct {
	Endpoint         string                `yaml:"endpoint"`
	TLS              *tlsconf.ServerConfig `yaml:"tls"`
	Auth             *bearer.ServerConfig  `yaml:"auth"`
	EdgePolicyConfig `yaml:",inline"`
}

// EdgePolicyConfig contains Reef's explicit plaintext and credential lifecycle
// controls. External plaintext is rejected unless Insecure is set.
type EdgePolicyConfig struct {
	Insecure                       bool     `yaml:"insecure"`
	DangerAllowBearerOverPlaintext bool     `yaml:"danger_allow_bearer_over_plaintext"`
	CredentialReloadInterval       Duration `yaml:"credential_reload_interval"`
}

func (c EdgePolicyConfig) validate(path string) error {
	interval := c.CredentialReloadInterval.Std()
	if interval < 0 {
		return fmt.Errorf("%s.credential_reload_interval must not be negative", path)
	}
	if interval > 0 && interval < time.Second {
		return fmt.Errorf("%s.credential_reload_interval must be at least 1s", path)
	}
	if interval > 24*time.Hour {
		return fmt.Errorf("%s.credential_reload_interval must not exceed 24h", path)
	}
	return nil
}

// UDPConfig configures a UDP listener.
type UDPConfig struct {
	Endpoint      string `yaml:"endpoint"`
	MaxPacketSize int    `yaml:"max_packet_size"`
}

// ProcessorConfig stores a processor type plus the full YAML node so each
// processor decodes its own typed fields from Raw. A custom unmarshaler is
// required: go-yaml v3 does NOT capture siblings into an inline yaml.Node, so
// `Raw yaml.Node \`yaml:",inline"\“ silently decoded to nothing.
type ProcessorConfig struct {
	Type string
	Raw  yaml.Node
}

func (pc *ProcessorConfig) UnmarshalYAML(node *yaml.Node) error {
	pc.Raw = *node
	var head struct {
		Type string `yaml:"type"`
	}
	if err := node.Decode(&head); err != nil {
		return err
	}
	pc.Type = head.Type
	return nil
}

// ExporterConfig stores an exporter type plus the full YAML node (see
// ProcessorConfig for why a custom unmarshaler is required).
type ExporterConfig struct {
	Type string
	Raw  yaml.Node
}

func (ec *ExporterConfig) UnmarshalYAML(node *yaml.Node) error {
	ec.Raw = *node
	var head struct {
		Type string `yaml:"type"`
	}
	if err := node.Decode(&head); err != nil {
		return err
	}
	ec.Type = head.Type
	return nil
}

// ValidateConfig configures the validate processor.
type ValidateConfig struct {
	MaxSpanBytes  int      `yaml:"max_span_bytes"`
	CredsPatterns []string `yaml:"creds_patterns"`
}

// RedactConfig configures the redact processor of the metric and log pipelines.
type RedactConfig struct {
	CredsPatterns []string `yaml:"creds_patterns"`
}

// AttributeAction configures one attributes processor action.
type AttributeAction struct {
	Action string `yaml:"action"`
	Scope  string `yaml:"scope"` // "span" (default) or "resource"
	Key    string `yaml:"key"`
	Value  string `yaml:"value"`
	NewKey string `yaml:"new_key"`
}

// AttributesConfig configures the attributes processor.
type AttributesConfig struct {
	Actions []AttributeAction `yaml:"actions"`
}

// BatchConfig configures the batch processor.
type BatchConfig struct {
	MaxSize int      `yaml:"max_size"`
	Timeout Duration `yaml:"timeout"`
}

// SamplingRule configures one tail-sampling rule.
type SamplingRule struct {
	Type      string   `yaml:"type"`
	Threshold Duration `yaml:"threshold"`
	Services  []string `yaml:"services"`
}

// TailSamplingConfig configures tail sampling.
type TailSamplingConfig struct {
	DecisionWait    Duration       `yaml:"decision_wait"`
	MaxTraces       int            `yaml:"max_traces"`
	MaxBytes        int64          `yaml:"max_bytes"`
	DefaultKeepRate float64        `yaml:"default_keep_rate"`
	Rules           []SamplingRule `yaml:"rules"`
}

// AmberConfig configures the Amber exporter.
type AmberConfig struct {
	Endpoint         string                `yaml:"endpoint"`
	Timeout          Duration              `yaml:"timeout"`
	Retry            RetryConfig           `yaml:"retry"`
	TLS              *tlsconf.ClientConfig `yaml:"tls"`
	Auth             *bearer.ClientConfig  `yaml:"auth"`
	EdgePolicyConfig `yaml:",inline"`
}

// S3Config configures the S3 exporter.
type S3Config struct {
	Bucket string      `yaml:"bucket"`
	Region string      `yaml:"region"`
	Prefix string      `yaml:"prefix"`
	Format string      `yaml:"format"`
	Retry  RetryConfig `yaml:"retry"`
}

// RetryConfig configures exporter retries.
type RetryConfig struct {
	MaxAttempts    int      `yaml:"max_attempts"`
	InitialBackoff Duration `yaml:"initial_backoff"`
	MaxBackoff     Duration `yaml:"max_backoff"`
}

// MetricsConfig configures the collector's own /metrics (self-observability) endpoint.
type MetricsConfig struct {
	Endpoint         string                `yaml:"endpoint"`
	TLS              *tlsconf.ServerConfig `yaml:"tls"`
	Auth             *bearer.ServerConfig  `yaml:"auth"`
	EdgePolicyConfig `yaml:",inline"`
}

// MetricPipelineConfig configures the metrics pipeline: enrich processors and
// exporters. Metrics arrive over the shared OTLP ingress (top-level
// `receivers.otlp_grpc`/`otlp_http`), not a pipeline-local listener.
type MetricPipelineConfig struct {
	Processors []ProcessorConfig      `yaml:"processors"`
	Exporter   MetricExporterConfig   `yaml:"exporter"`
	Exporters  []MetricExporterConfig `yaml:"exporters"`
}

// MetricExporterConfig configures the amber metrics exporter.
type MetricExporterConfig struct {
	Type             string                `yaml:"type"`
	Endpoint         string                `yaml:"endpoint"`
	Timeout          Duration              `yaml:"timeout"`
	Retry            RetryConfig           `yaml:"retry"`
	TLS              *tlsconf.ClientConfig `yaml:"tls"`
	Auth             *bearer.ClientConfig  `yaml:"auth"`
	EdgePolicyConfig `yaml:",inline"`
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: read %s: %w", path, err)
	}
	return Parse(data)
}

func Parse(data []byte) (Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("config: parse yaml: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) Validate() error {
	if err := c.Pipeline.validate(); err != nil {
		return err
	}
	otlpIngress := c.Receivers.OTLPGRPC != nil || c.Receivers.OTLPHTTP != nil
	anyTraceReceiver := c.Receivers.AnyEnabled()
	metricActive := c.MetricPipeline != nil
	logActive := c.LogPipeline != nil
	if c.Receivers.OTLPGRPC != nil {
		if err := c.Receivers.OTLPGRPC.validate("receivers.otlp_grpc"); err != nil {
			return err
		}
	}
	if c.Receivers.OTLPHTTP != nil {
		if err := c.Receivers.OTLPHTTP.validate("receivers.otlp_http"); err != nil {
			return err
		}
	}
	if c.Metrics.Endpoint != "" {
		if err := c.Metrics.validate("metrics"); err != nil {
			return err
		}
	}

	if !anyTraceReceiver && !metricActive && !logActive {
		return fmt.Errorf("at least one receiver is required: enable an OTLP or legacy trace receiver, metric_pipeline, or log_pipeline")
	}

	// Metrics and logs are OTLP-only; they ride the shared ingress and cannot be
	// fed by the legacy (Jaeger/Zipkin) trace receivers.
	if (metricActive || logActive) && !otlpIngress {
		return fmt.Errorf("metric_pipeline/log_pipeline require the shared OTLP ingress: set receivers.otlp_grpc or receivers.otlp_http")
	}

	// The top-level receivers/processors/exporters describe the trace pipeline.
	// It is engaged when trace processors or exporters are declared; a bare trace
	// receiver with no metric/log pipeline also implies trace intent and must
	// carry exporters.
	traceEngaged := len(c.Exporters) > 0 || len(c.Processors) > 0 ||
		(anyTraceReceiver && !metricActive && !logActive)
	if traceEngaged {
		for i, pc := range c.Processors {
			if pc.Type == "" {
				return fmt.Errorf("processors[%d]: type is required", i)
			}
			switch pc.Type {
			case "validate", "attributes", "batch", "tail_sampling":
				if pc.Type == "tail_sampling" {
					var tc TailSamplingConfig
					if err := pc.Raw.Decode(&tc); err != nil {
						return fmt.Errorf("processors[%d]: %w", i, err)
					}
					if tc.MaxBytes < 0 || tc.MaxBytes > (1<<40) {
						return fmt.Errorf("processors[%d].max_bytes must be between 0 and %d", i, 1<<40)
					}
				}
			case "head_sampling":
				return fmt.Errorf("processors[%d]: head_sampling was removed; use tail_sampling", i)
			default:
				return fmt.Errorf("processors[%d]: unknown type %q", i, pc.Type)
			}
		}
		if len(c.Exporters) == 0 {
			return fmt.Errorf("exporters: at least one exporter is required")
		}
		for i, ec := range c.Exporters {
			if ec.Type == "" {
				return fmt.Errorf("exporters[%d]: type is required", i)
			}
			switch ec.Type {
			case "devnull", "amber", "fathom", "s3":
			default:
				return fmt.Errorf("exporters[%d]: unknown type %q", i, ec.Type)
			}
			if ec.Type == "amber" || ec.Type == "fathom" {
				var edgeConfig AmberConfig
				if err := ec.Raw.Decode(&edgeConfig); err != nil {
					return fmt.Errorf("exporters[%d]: %w", i, err)
				}
				if err := edgeConfig.validate(fmt.Sprintf("exporters[%d]", i)); err != nil {
					return err
				}
			}
		}
	}

	if metricActive {
		if err := c.MetricPipeline.validate(); err != nil {
			return err
		}
	}
	if logActive {
		if err := c.LogPipeline.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (m *MetricPipelineConfig) validate() error {
	exporters := m.effectiveExporters()
	if len(exporters) == 0 {
		return fmt.Errorf("metric_pipeline.exporter.endpoint is required")
	}
	for i, exporter := range exporters {
		if exporter.Endpoint == "" {
			return fmt.Errorf("metric_pipeline.exporters[%d].endpoint is required", i)
		}
		switch exporter.metricType() {
		case "amber", "fathom":
		default:
			return fmt.Errorf("metric_pipeline.exporters[%d]: unknown type %q", i, exporter.Type)
		}
		if err := exporter.validate(fmt.Sprintf("metric_pipeline.exporters[%d]", i)); err != nil {
			return err
		}
	}
	for i, pc := range m.Processors {
		if pc.Type == "" {
			return fmt.Errorf("metric_pipeline.processors[%d]: type is required", i)
		}
		switch pc.Type {
		case "attributes", "redact":
		default:
			return fmt.Errorf("metric_pipeline.processors[%d]: unknown type %q", i, pc.Type)
		}
	}
	return nil
}

func (m MetricPipelineConfig) effectiveExporters() []MetricExporterConfig {
	if len(m.Exporters) > 0 {
		return m.Exporters
	}
	if m.Exporter.Endpoint != "" {
		return []MetricExporterConfig{m.Exporter}
	}
	return nil
}

func (m MetricExporterConfig) metricType() string {
	if m.Type == "" {
		return "amber"
	}
	return m.Type
}

// LogPipelineConfig configures the logs pipeline: optional processors (redact)
// and exporters. Logs arrive over the shared OTLP ingress (top-level
// `receivers.otlp_grpc`/`otlp_http`), not a pipeline-local listener.
type LogPipelineConfig struct {
	Processors []ProcessorConfig   `yaml:"processors"`
	Exporter   LogExporterConfig   `yaml:"exporter"`
	Exporters  []LogExporterConfig `yaml:"exporters"`
}

// LogExporterConfig configures a log exporter.
type LogExporterConfig struct {
	Type             string                `yaml:"type"`
	Endpoint         string                `yaml:"endpoint"`
	Timeout          Duration              `yaml:"timeout"`
	Retry            RetryConfig           `yaml:"retry"`
	TLS              *tlsconf.ClientConfig `yaml:"tls"`
	Auth             *bearer.ClientConfig  `yaml:"auth"`
	EdgePolicyConfig `yaml:",inline"`
}

func (l *LogPipelineConfig) validate() error {
	exporters := l.effectiveExporters()
	if len(exporters) == 0 {
		return fmt.Errorf("log_pipeline.exporter.endpoint is required")
	}
	for i, exporter := range exporters {
		if exporter.Endpoint == "" {
			return fmt.Errorf("log_pipeline.exporters[%d].endpoint is required", i)
		}
		switch exporter.logType() {
		case "amber", "fathom":
		default:
			return fmt.Errorf("log_pipeline.exporters[%d]: unknown type %q", i, exporter.Type)
		}
		if err := exporter.validate(fmt.Sprintf("log_pipeline.exporters[%d]", i)); err != nil {
			return err
		}
	}
	for i, pc := range l.Processors {
		if pc.Type == "" {
			return fmt.Errorf("log_pipeline.processors[%d]: type is required", i)
		}
		if pc.Type != "redact" {
			return fmt.Errorf("log_pipeline.processors[%d]: unknown type %q", i, pc.Type)
		}
	}
	return nil
}

func (l LogPipelineConfig) effectiveExporters() []LogExporterConfig {
	if len(l.Exporters) > 0 {
		return l.Exporters
	}
	if l.Exporter.Endpoint != "" {
		return []LogExporterConfig{l.Exporter}
	}
	return nil
}

func (l LogExporterConfig) logType() string {
	if l.Type == "" {
		return "amber"
	}
	return l.Type
}

func (c ReceiversConfig) AnyEnabled() bool {
	return c.OTLPGRPC != nil ||
		c.OTLPHTTP != nil ||
		c.JaegerThriftHTTP != nil ||
		c.JaegerThriftUDP != nil ||
		c.JaegerThriftTCP != nil ||
		c.ZipkinHTTP != nil
}
