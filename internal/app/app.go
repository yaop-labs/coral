// Package app builds a collector from config.Config.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"

	"github.com/yaop-labs/coral/internal/config"
	amberexp "github.com/yaop-labs/coral/internal/exporter/amber"
	crosexp "github.com/yaop-labs/coral/internal/exporter/cros"
	"github.com/yaop-labs/coral/internal/exporter/devnull"
	retryexp "github.com/yaop-labs/coral/internal/exporter/retry"
	s3exp "github.com/yaop-labs/coral/internal/exporter/s3"
	"github.com/yaop-labs/coral/internal/logs"
	"github.com/yaop-labs/coral/internal/metric"
	"github.com/yaop-labs/coral/internal/model"
	"github.com/yaop-labs/coral/internal/pipeline"
	"github.com/yaop-labs/coral/internal/processor"
	"github.com/yaop-labs/coral/internal/processor/sampling"
	jaegerrecv "github.com/yaop-labs/coral/internal/receiver/jaeger"
	otlprecv "github.com/yaop-labs/coral/internal/receiver/otlp"
	zipkinrecv "github.com/yaop-labs/coral/internal/receiver/zipkin"
)

// App is a fully wired but unstarted collector.
type App struct {
	logger   *slog.Logger
	pipeline *pipeline.Pipeline

	metricPipeline *metric.Pipeline // nil unless metric_pipeline is configured
	logPipeline    *logs.Pipeline   // nil unless log_pipeline is configured

	otlpHTTP   *otlprecv.HTTPReceiver
	otlpGRPC   *otlprecv.GRPCReceiver
	startHooks []func(context.Context) error
	stopHooks  []func(context.Context) error
}

func New(cfg config.Config, logger *slog.Logger) (*App, error) {
	return newApp(cfg, logger, nil)
}

// NewWithExporter builds an App using exp instead of the configured exporters.
func NewWithExporter(cfg config.Config, logger *slog.Logger, exp pipeline.Exporter) (*App, error) {
	return newApp(cfg, logger, exp)
}

func newApp(cfg config.Config, logger *slog.Logger, overrideExp pipeline.Exporter) (*App, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	p := pipeline.New(pipeline.Config{
		Workers:   cfg.Pipeline.Workers,
		QueueSize: cfg.Pipeline.QueueSize,
	}, logger)

	a := &App{logger: logger, pipeline: p}

	if err := a.addReceivers(p, cfg.Receivers, logger); err != nil {
		return nil, err
	}
	if cfg.Metrics.Endpoint != "" {
		a.addMetricsServer(p, cfg.Metrics.Endpoint)
		logger.Info("metrics endpoint enabled", "endpoint", cfg.Metrics.Endpoint)
	}

	for i, pc := range cfg.Processors {
		pr, err := buildProcessor(pc, p, a, i)
		if err != nil {
			return nil, fmt.Errorf("processor %q: %w", pc.Type, err)
		}
		p.AddProcessor(pr)
	}

	if overrideExp != nil {
		p.AddExporter(overrideExp)
	} else {
		for _, ec := range cfg.Exporters {
			e, err := buildExporter(ec, a)
			if err != nil {
				return nil, fmt.Errorf("exporter %q: %w", ec.Type, err)
			}
			p.AddExporter(e)
		}
	}

	if cfg.MetricPipeline != nil {
		mp, err := buildMetricPipeline(*cfg.MetricPipeline, cfg.Pipeline, logger)
		if err != nil {
			return nil, fmt.Errorf("metric_pipeline: %w", err)
		}
		a.metricPipeline = mp
		logger.Info("metric pipeline enabled")
	}
	if cfg.LogPipeline != nil {
		lp, err := buildLogPipeline(*cfg.LogPipeline, cfg.Pipeline, logger)
		if err != nil {
			return nil, fmt.Errorf("log_pipeline: %w", err)
		}
		a.logPipeline = lp
		logger.Info("log pipeline enabled")
	}

	return a, nil
}

func buildMetricPipeline(cfg config.MetricPipelineConfig, base config.PipelineConfig, logger *slog.Logger) (*metric.Pipeline, error) {
	mp := metric.NewPipeline(base.Workers, base.QueueSize, logger)

	grpcAddr, httpAddr := "", ""
	if cfg.Receivers.OTLPGRPC != nil {
		grpcAddr = cfg.Receivers.OTLPGRPC.Endpoint
	}
	if cfg.Receivers.OTLPHTTP != nil {
		httpAddr = cfg.Receivers.OTLPHTTP.Endpoint
	}
	mp.AddReceiver(metric.NewOTLPReceiver(grpcAddr, httpAddr, logger))

	for i, pc := range cfg.Processors {
		switch pc.Type {
		case "attributes":
			var ac config.AttributesConfig
			if err := pc.Raw.Decode(&ac); err != nil {
				return nil, fmt.Errorf("processor %d (attributes): %w", i, err)
			}
			actions := make([]metric.AttributeAction, len(ac.Actions))
			for j, a := range ac.Actions {
				actions[j] = metric.AttributeAction{Action: a.Action, Key: a.Key, Value: a.Value}
			}
			mp.AddProcessor(metric.NewAttributesProcessor(actions))
		default:
			return nil, fmt.Errorf("processor %d: unknown type %q", i, pc.Type)
		}
	}

	for _, exporterCfg := range metricExporters(cfg) {
		exp, err := buildMetricExporter(exporterCfg)
		if err != nil {
			return nil, err
		}
		mp.AddExporter(exp)
	}
	return mp, nil
}

func metricExporters(cfg config.MetricPipelineConfig) []config.MetricExporterConfig {
	if len(cfg.Exporters) > 0 {
		return cfg.Exporters
	}
	if cfg.Exporter.Endpoint != "" {
		return []config.MetricExporterConfig{cfg.Exporter}
	}
	return nil
}

func buildMetricExporter(cfg config.MetricExporterConfig) (metric.Exporter, error) {
	retry := metric.RetryPolicy{
		MaxAttempts:    cfg.Retry.MaxAttempts,
		InitialBackoff: cfg.Retry.InitialBackoff.Std(),
		MaxBackoff:     cfg.Retry.MaxBackoff.Std(),
	}
	switch cfg.Type {
	case "", "amber":
		return metric.NewAmberExporter(cfg.Endpoint, cfg.Timeout.Std(), retry)
	case "cros":
		return metric.NewCROSExporter(cfg.Endpoint, cfg.Timeout.Std(), retry)
	default:
		return nil, fmt.Errorf("metric exporter: unknown type %q", cfg.Type)
	}
}

func buildLogPipeline(cfg config.LogPipelineConfig, base config.PipelineConfig, logger *slog.Logger) (*logs.Pipeline, error) {
	lp := logs.NewPipeline(base.Workers, base.QueueSize, logger)

	grpcAddr, httpAddr := "", ""
	if cfg.Receivers.OTLPGRPC != nil {
		grpcAddr = cfg.Receivers.OTLPGRPC.Endpoint
	}
	if cfg.Receivers.OTLPHTTP != nil {
		httpAddr = cfg.Receivers.OTLPHTTP.Endpoint
	}
	lp.AddReceiver(logs.NewOTLPReceiver(grpcAddr, httpAddr, logger))

	for _, exporterCfg := range logExporters(cfg) {
		exp, err := buildLogExporter(exporterCfg)
		if err != nil {
			return nil, err
		}
		lp.AddExporter(exp)
	}
	return lp, nil
}

func logExporters(cfg config.LogPipelineConfig) []config.LogExporterConfig {
	if len(cfg.Exporters) > 0 {
		return cfg.Exporters
	}
	if cfg.Exporter.Endpoint != "" {
		return []config.LogExporterConfig{cfg.Exporter}
	}
	return nil
}

func buildLogExporter(cfg config.LogExporterConfig) (logs.Exporter, error) {
	retry := logs.RetryPolicy{
		MaxAttempts:    cfg.Retry.MaxAttempts,
		InitialBackoff: cfg.Retry.InitialBackoff.Std(),
		MaxBackoff:     cfg.Retry.MaxBackoff.Std(),
	}
	switch cfg.Type {
	case "", "amber":
		return logs.NewAmberExporter(cfg.Endpoint, cfg.Timeout.Std(), retry)
	case "cros":
		return logs.NewCROSExporter(cfg.Endpoint, cfg.Timeout.Std(), retry)
	default:
		return nil, fmt.Errorf("log exporter: unknown type %q", cfg.Type)
	}
}

// OTLPHTTPAddr returns the bound address of the OTLP HTTP receiver (for tests).
func (a *App) OTLPHTTPAddr() string {
	if a.otlpHTTP == nil {
		return ""
	}
	return a.otlpHTTP.Addr()
}

// OTLPGRPCAddr returns the bound address of the OTLP gRPC receiver (for tests).
func (a *App) OTLPGRPCAddr() string {
	if a.otlpGRPC == nil {
		return ""
	}
	return a.otlpGRPC.Addr()
}

func (a *App) Start(ctx context.Context) error {
	for _, hook := range a.startHooks {
		if err := hook(ctx); err != nil {
			return err
		}
	}
	if a.metricPipeline != nil {
		if err := a.metricPipeline.Start(ctx); err != nil {
			return err
		}
	}
	if a.logPipeline != nil {
		if err := a.logPipeline.Start(ctx); err != nil {
			return err
		}
	}
	return a.pipeline.Start(ctx)
}

func (a *App) Shutdown(ctx context.Context) error {
	err := a.pipeline.Shutdown(ctx)
	if a.metricPipeline != nil {
		if mErr := a.metricPipeline.Shutdown(ctx); mErr != nil && err == nil {
			err = mErr
		}
	}
	if a.logPipeline != nil {
		if lErr := a.logPipeline.Shutdown(ctx); lErr != nil && err == nil {
			err = lErr
		}
	}
	for i := len(a.stopHooks) - 1; i >= 0; i-- {
		if stopErr := a.stopHooks[i](ctx); stopErr != nil {
			a.logger.Error("app stop hook error", "err", stopErr)
			if err == nil {
				err = stopErr
			}
		}
	}
	return err
}

func (a *App) addMetricsServer(p *pipeline.Pipeline, endpoint string) {
	var srv *http.Server
	a.startHooks = append(a.startHooks, func(ctx context.Context) error {
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			batchesIn, batchesDropped, spansOut := p.Stats()
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = fmt.Fprintf(w, "# TYPE collector_batches_in counter\ncollector_batches_in %d\n", batchesIn)
			_, _ = fmt.Fprintf(w, "# TYPE collector_batches_dropped counter\ncollector_batches_dropped %d\n", batchesDropped)
			_, _ = fmt.Fprintf(w, "# TYPE collector_spans_out counter\ncollector_spans_out %d\n", spansOut)
		})
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		ln, err := net.Listen("tcp", endpoint)
		if err != nil {
			return fmt.Errorf("metrics: listen %s: %w", endpoint, err)
		}
		srv = &http.Server{Handler: mux}
		go func() {
			if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed && ctx.Err() == nil {
				a.logger.Error("metrics server error", "err", err)
			}
		}()
		return nil
	})
	a.stopHooks = append(a.stopHooks, func(ctx context.Context) error {
		if srv == nil {
			return nil
		}
		return srv.Shutdown(ctx)
	})
}

func (a *App) addReceivers(p *pipeline.Pipeline, cfg config.ReceiversConfig, logger *slog.Logger) error {
	if cfg.OTLPGRPC != nil {
		r, err := otlprecv.NewGRPC(cfg.OTLPGRPC.Endpoint, 0)
		if err != nil {
			return fmt.Errorf("otlp_grpc: %w", err)
		}
		a.otlpGRPC = r
		p.AddReceiver(r)
		logger.Info("otlp grpc receiver enabled", "endpoint", cfg.OTLPGRPC.Endpoint)
	}
	if cfg.OTLPHTTP != nil {
		r, err := otlprecv.NewHTTP(cfg.OTLPHTTP.Endpoint)
		if err != nil {
			return fmt.Errorf("otlp_http: %w", err)
		}
		a.otlpHTTP = r
		p.AddReceiver(r)
		logger.Info("otlp http receiver enabled", "endpoint", cfg.OTLPHTTP.Endpoint)
	}
	if cfg.JaegerThriftUDP != nil {
		r, err := jaegerrecv.NewThriftUDP(cfg.JaegerThriftUDP.Endpoint, cfg.JaegerThriftUDP.MaxPacketSize)
		if err != nil {
			return fmt.Errorf("jaeger_thrift_udp: %w", err)
		}
		p.AddReceiver(r)
		logger.Info("jaeger thrift udp receiver enabled", "endpoint", cfg.JaegerThriftUDP.Endpoint)
	}
	if cfg.JaegerThriftTCP != nil {
		r, err := jaegerrecv.NewThriftTCP(cfg.JaegerThriftTCP.Endpoint)
		if err != nil {
			return fmt.Errorf("jaeger_thrift_tcp: %w", err)
		}
		p.AddReceiver(r)
		logger.Info("jaeger thrift tcp receiver enabled", "endpoint", cfg.JaegerThriftTCP.Endpoint)
	}
	if cfg.JaegerThriftHTTP != nil {
		r, err := jaegerrecv.NewThriftHTTP(cfg.JaegerThriftHTTP.Endpoint)
		if err != nil {
			return fmt.Errorf("jaeger_thrift_http: %w", err)
		}
		p.AddReceiver(r)
		logger.Info("jaeger thrift http receiver enabled", "endpoint", cfg.JaegerThriftHTTP.Endpoint)
	}
	if cfg.ZipkinHTTP != nil {
		r, err := zipkinrecv.New(cfg.ZipkinHTTP.Endpoint)
		if err != nil {
			return fmt.Errorf("zipkin_http: %w", err)
		}
		p.AddReceiver(r)
		logger.Info("zipkin http receiver enabled", "endpoint", cfg.ZipkinHTTP.Endpoint)
	}
	return nil
}

func buildProcessor(pc config.ProcessorConfig, p *pipeline.Pipeline, a *App, processorIndex int) (pipeline.Processor, error) {
	switch pc.Type {
	case "validate":
		var cfg config.ValidateConfig
		if err := pc.Raw.Decode(&cfg); err != nil {
			return nil, err
		}
		return processor.NewValidate(cfg.MaxSpanBytes, cfg.CredsPatterns)

	case "attributes":
		var cfg config.AttributesConfig
		if err := pc.Raw.Decode(&cfg); err != nil {
			return nil, err
		}
		actions := make([]processor.AttributeActionConfig, len(cfg.Actions))
		for i, a := range cfg.Actions {
			actions[i] = processor.AttributeActionConfig{
				Action: a.Action,
				Key:    a.Key,
				Value:  a.Value,
				NewKey: a.NewKey,
			}
		}
		return processor.NewAttributes(actions)

	case "batch":
		var cfg config.BatchConfig
		if err := pc.Raw.Decode(&cfg); err != nil {
			return nil, err
		}
		return processor.NewBatch(cfg.MaxSize, cfg.Timeout.Std(), func(ctx context.Context, b model.Batch) error {
			return p.ExportFrom(ctx, b, processorIndex+1)
		}), nil

	case "tail_sampling":
		var cfg config.TailSamplingConfig
		if err := pc.Raw.Decode(&cfg); err != nil {
			return nil, err
		}
		rules, err := buildSamplingRules(cfg.Rules)
		if err != nil {
			return nil, err
		}
		ts := sampling.NewTail(
			cfg.DecisionWait.Std(),
			cfg.MaxTraces,
			cfg.DefaultKeepRate,
			rules,
			func(ctx context.Context, b model.Batch) error {
				return p.ExportFrom(ctx, b, processorIndex+1)
			},
		)
		a.startHooks = append(a.startHooks, func(ctx context.Context) error {
			ts.Start(ctx)
			return nil
		})
		return ts, nil

	default:
		return nil, fmt.Errorf("unknown processor type %q", pc.Type)
	}
}

func buildSamplingRules(cfgs []config.SamplingRule) ([]sampling.Rule, error) {
	var rules []sampling.Rule
	for _, r := range cfgs {
		switch r.Type {
		case "error":
			rules = append(rules, sampling.ErrorRule{})
		case "debug_tag":
			rules = append(rules, sampling.DebugTagRule{})
		case "duration":
			rules = append(rules, sampling.DurationRule{Threshold: r.Threshold.Std()})
		case "service":
			svcs := make(map[string]struct{}, len(r.Services))
			for _, s := range r.Services {
				svcs[s] = struct{}{}
			}
			rules = append(rules, sampling.ServiceRule{Services: svcs})
		default:
			return nil, fmt.Errorf("unknown rule type %q", r.Type)
		}
	}
	return rules, nil
}

func buildExporter(ec config.ExporterConfig, a *App) (pipeline.Exporter, error) {
	switch ec.Type {
	case "devnull":
		return devnull.New(), nil

	case "amber":
		var cfg config.AmberConfig
		if err := ec.Raw.Decode(&cfg); err != nil {
			return nil, err
		}
		e, err := amberexp.New(cfg.Endpoint, cfg.Timeout.Std())
		if err != nil {
			return nil, err
		}
		return retryexp.Wrap(e, retryexp.Config{
			MaxAttempts:    cfg.Retry.MaxAttempts,
			InitialBackoff: cfg.Retry.InitialBackoff.Std(),
			MaxBackoff:     cfg.Retry.MaxBackoff.Std(),
		}), nil

	case "cros":
		var cfg config.AmberConfig
		if err := ec.Raw.Decode(&cfg); err != nil {
			return nil, err
		}
		e, err := crosexp.New(cfg.Endpoint, cfg.Timeout.Std())
		if err != nil {
			return nil, err
		}
		return retryexp.Wrap(e, retryexp.Config{
			MaxAttempts:    cfg.Retry.MaxAttempts,
			InitialBackoff: cfg.Retry.InitialBackoff.Std(),
			MaxBackoff:     cfg.Retry.MaxBackoff.Std(),
		}), nil

	case "s3":
		var cfg config.S3Config
		if err := ec.Raw.Decode(&cfg); err != nil {
			return nil, err
		}
		e, err := s3exp.New(s3exp.Config{
			Bucket: cfg.Bucket,
			Region: cfg.Region,
			Prefix: cfg.Prefix,
			Format: cfg.Format,
		})
		if err != nil {
			return nil, err
		}
		a.startHooks = append(a.startHooks, e.Init)
		return retryexp.Wrap(e, retryexp.Config{
			MaxAttempts:    cfg.Retry.MaxAttempts,
			InitialBackoff: cfg.Retry.InitialBackoff.Std(),
			MaxBackoff:     cfg.Retry.MaxBackoff.Std(),
		}), nil

	default:
		return nil, fmt.Errorf("unknown exporter type %q", ec.Type)
	}
}
