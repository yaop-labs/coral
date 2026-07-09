package config

import (
	"fmt"
	"os"
	"time"

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
	Pipeline   PipelineConfig    `yaml:"pipeline"`
	Receivers  ReceiversConfig   `yaml:"receivers"`
	Processors []ProcessorConfig `yaml:"processors"`
	Exporters  []ExporterConfig  `yaml:"exporters"`
	Metrics    MetricsConfig     `yaml:"metrics"`

	// MetricPipeline is the optional metrics path (wisp → coral → amber),
	// independent of the trace pipeline above.
	MetricPipeline *MetricPipelineConfig `yaml:"metric_pipeline"`

	// LogPipeline is the optional logs path, independent of traces and metrics.
	LogPipeline *LogPipelineConfig `yaml:"log_pipeline"`
}

// PipelineConfig configures pipeline concurrency.
type PipelineConfig struct {
	Workers   int `yaml:"workers"`
	QueueSize int `yaml:"queue_size"`
}

// ReceiversConfig configures trace receivers.
type ReceiversConfig struct {
	OTLPGRPC         *EndpointConfig `yaml:"otlp_grpc"`
	OTLPHTTP         *EndpointConfig `yaml:"otlp_http"`
	JaegerThriftHTTP *EndpointConfig `yaml:"jaeger_thrift_http"`
	JaegerThriftUDP  *UDPConfig      `yaml:"jaeger_thrift_udp"`
	JaegerThriftTCP  *EndpointConfig `yaml:"jaeger_thrift_tcp"`
	ZipkinHTTP       *EndpointConfig `yaml:"zipkin_http"`
}

// EndpointConfig configures a TCP or HTTP listener.
type EndpointConfig struct {
	Endpoint string `yaml:"endpoint"`
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

// AttributeAction configures one attributes processor action.
type AttributeAction struct {
	Action string `yaml:"action"`
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
	DefaultKeepRate float64        `yaml:"default_keep_rate"`
	Rules           []SamplingRule `yaml:"rules"`
}

// AmberConfig configures the Amber exporter.
type AmberConfig struct {
	Endpoint string      `yaml:"endpoint"`
	Timeout  Duration    `yaml:"timeout"`
	Retry    RetryConfig `yaml:"retry"`
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
	Endpoint string `yaml:"endpoint"`
}

// MetricPipelineConfig configures the metrics pipeline: OTLP receivers, enrich
// processors, and an amber exporter.
type MetricPipelineConfig struct {
	Receivers  MetricReceiversConfig  `yaml:"receivers"`
	Processors []ProcessorConfig      `yaml:"processors"`
	Exporter   MetricExporterConfig   `yaml:"exporter"`
	Exporters  []MetricExporterConfig `yaml:"exporters"`
}

// MetricReceiversConfig configures the OTLP metric receivers.
type MetricReceiversConfig struct {
	OTLPGRPC *EndpointConfig `yaml:"otlp_grpc"`
	OTLPHTTP *EndpointConfig `yaml:"otlp_http"`
}

func (m MetricReceiversConfig) AnyEnabled() bool {
	return m.OTLPGRPC != nil || m.OTLPHTTP != nil
}

// MetricExporterConfig configures the amber metrics exporter.
type MetricExporterConfig struct {
	Type     string      `yaml:"type"`
	Endpoint string      `yaml:"endpoint"`
	Timeout  Duration    `yaml:"timeout"`
	Retry    RetryConfig `yaml:"retry"`
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
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("config: parse yaml: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) Validate() error {
	traceEnabled := c.Receivers.AnyEnabled()
	if !traceEnabled && c.MetricPipeline == nil && c.LogPipeline == nil {
		return fmt.Errorf("at least one receiver is required: enable a trace receiver, metric_pipeline, or log_pipeline")
	}

	if traceEnabled {
		for i, pc := range c.Processors {
			if pc.Type == "" {
				return fmt.Errorf("processors[%d]: type is required", i)
			}
			switch pc.Type {
			case "validate", "attributes", "batch", "tail_sampling":
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
			case "devnull", "amber", "cros", "s3":
			default:
				return fmt.Errorf("exporters[%d]: unknown type %q", i, ec.Type)
			}
		}
	}

	if c.MetricPipeline != nil {
		if err := c.MetricPipeline.validate(); err != nil {
			return err
		}
	}
	if c.LogPipeline != nil {
		if err := c.LogPipeline.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (m *MetricPipelineConfig) validate() error {
	if !m.Receivers.AnyEnabled() {
		return fmt.Errorf("metric_pipeline.receivers: at least one OTLP receiver is required")
	}
	exporters := m.effectiveExporters()
	if len(exporters) == 0 {
		return fmt.Errorf("metric_pipeline.exporter.endpoint is required")
	}
	for i, exporter := range exporters {
		if exporter.Endpoint == "" {
			return fmt.Errorf("metric_pipeline.exporters[%d].endpoint is required", i)
		}
		switch exporter.metricType() {
		case "amber", "cros":
		default:
			return fmt.Errorf("metric_pipeline.exporters[%d]: unknown type %q", i, exporter.Type)
		}
	}
	for i, pc := range m.Processors {
		if pc.Type == "" {
			return fmt.Errorf("metric_pipeline.processors[%d]: type is required", i)
		}
		if pc.Type != "attributes" {
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

// LogPipelineConfig configures the logs pipeline: OTLP receivers and exporters.
type LogPipelineConfig struct {
	Receivers LogReceiversConfig  `yaml:"receivers"`
	Exporter  LogExporterConfig   `yaml:"exporter"`
	Exporters []LogExporterConfig `yaml:"exporters"`
}

// LogReceiversConfig configures the OTLP log receivers.
type LogReceiversConfig struct {
	OTLPGRPC *EndpointConfig `yaml:"otlp_grpc"`
	OTLPHTTP *EndpointConfig `yaml:"otlp_http"`
}

func (l LogReceiversConfig) AnyEnabled() bool {
	return l.OTLPGRPC != nil || l.OTLPHTTP != nil
}

// LogExporterConfig configures a log exporter.
type LogExporterConfig struct {
	Type     string      `yaml:"type"`
	Endpoint string      `yaml:"endpoint"`
	Timeout  Duration    `yaml:"timeout"`
	Retry    RetryConfig `yaml:"retry"`
}

func (l *LogPipelineConfig) validate() error {
	if !l.Receivers.AnyEnabled() {
		return fmt.Errorf("log_pipeline.receivers: at least one OTLP receiver is required")
	}
	exporters := l.effectiveExporters()
	if len(exporters) == 0 {
		return fmt.Errorf("log_pipeline.exporter.endpoint is required")
	}
	for i, exporter := range exporters {
		if exporter.Endpoint == "" {
			return fmt.Errorf("log_pipeline.exporters[%d].endpoint is required", i)
		}
		switch exporter.logType() {
		case "cros":
		default:
			return fmt.Errorf("log_pipeline.exporters[%d]: unknown type %q", i, exporter.Type)
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
		return "cros"
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
