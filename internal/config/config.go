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

// ProcessorConfig stores a processor type and its raw YAML fields.
type ProcessorConfig struct {
	Type string    `yaml:"type"`
	Raw  yaml.Node `yaml:",inline"`
}

// ExporterConfig stores an exporter type and its raw YAML fields.
type ExporterConfig struct {
	Type string    `yaml:"type"`
	Raw  yaml.Node `yaml:",inline"`
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

// MetricsConfig configures the metrics HTTP endpoint.
type MetricsConfig struct {
	Endpoint string `yaml:"endpoint"`
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
	if !c.Receivers.AnyEnabled() {
		return fmt.Errorf("receivers: at least one receiver is required")
	}
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
		case "devnull", "amber", "s3":
		default:
			return fmt.Errorf("exporters[%d]: unknown type %q", i, ec.Type)
		}
	}
	return nil
}

func (c ReceiversConfig) AnyEnabled() bool {
	return c.OTLPGRPC != nil ||
		c.OTLPHTTP != nil ||
		c.JaegerThriftHTTP != nil ||
		c.JaegerThriftUDP != nil ||
		c.JaegerThriftTCP != nil ||
		c.ZipkinHTTP != nil
}
