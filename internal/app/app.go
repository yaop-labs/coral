// Package app builds a collector from config.Config.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"

	"github.com/yaop-labs/coral/internal/config"
	amberexp "github.com/yaop-labs/coral/internal/exporter/amber"
	"github.com/yaop-labs/coral/internal/exporter/devnull"
	fathomexp "github.com/yaop-labs/coral/internal/exporter/fathom"
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
	pipeline *pipeline.Pipeline[model.Batch]

	metricPipeline *metric.Pipeline // nil unless metric_pipeline is configured
	logPipeline    *logs.Pipeline   // nil unless log_pipeline is configured

	// ingress is the unified OTLP endpoint (4317/4318) feeding every signal
	// pipeline; nil when no OTLP receiver is configured. Legacy Jaeger/Zipkin
	// trace receivers remain attached directly to the trace pipeline.
	ingress    *otlprecv.Server
	startHooks []func(context.Context) error
	stopHooks  []func(context.Context) error

	ready atomic.Bool // set once all pipelines and the ingress are started
}

func New(cfg config.Config, logger *slog.Logger) (*App, error) {
	return newApp(cfg, logger, nil)
}

// NewWithExporter builds an App using exp instead of the configured exporters.
func NewWithExporter(cfg config.Config, logger *slog.Logger, exp pipeline.Exporter[model.Batch]) (*App, error) {
	return newApp(cfg, logger, exp)
}

func newApp(cfg config.Config, logger *slog.Logger, overrideExp pipeline.Exporter[model.Batch]) (*App, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	p := pipeline.New[model.Batch](pipeline.Config{
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

	traceActive := overrideExp != nil || len(cfg.Exporters) > 0
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

	a.addIngress(cfg.Receivers, traceActive)
	return a, nil
}

// addIngress builds the unified OTLP endpoint from the top-level receiver
// config, routing each signal to the pipeline that serves it. Signals whose
// pipeline is absent are left unserved (Unimplemented / 404). A config with
// only legacy trace receivers builds no ingress.
func (a *App) addIngress(cfg config.ReceiversConfig, traceActive bool) {
	grpcAddr, httpAddr := "", ""
	if cfg.OTLPGRPC != nil {
		grpcAddr = cfg.OTLPGRPC.Endpoint
	}
	if cfg.OTLPHTTP != nil {
		httpAddr = cfg.OTLPHTTP.Endpoint
	}
	if grpcAddr == "" && httpAddr == "" {
		return
	}
	sink := otlprecv.Sink{}
	if traceActive {
		sink.Traces = a.pipeline.Enqueue
	}
	if a.metricPipeline != nil {
		sink.Metrics = a.metricPipeline.Enqueue
	}
	if a.logPipeline != nil {
		sink.Logs = a.logPipeline.Enqueue
	}
	a.ingress = otlprecv.NewServer(grpcAddr, httpAddr, 0, sink)
	a.logger.Info("otlp ingress enabled", "grpc", grpcAddr, "http", httpAddr)
}

func buildMetricPipeline(cfg config.MetricPipelineConfig, base config.PipelineConfig, logger *slog.Logger) (*metric.Pipeline, error) {
	mp := metric.NewPipeline(base.Workers, base.QueueSize, logger)

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
		case "redact":
			var rc config.RedactConfig
			if err := pc.Raw.Decode(&rc); err != nil {
				return nil, fmt.Errorf("processor %d (redact): %w", i, err)
			}
			rp, err := metric.NewRedactProcessor(rc.CredsPatterns)
			if err != nil {
				return nil, fmt.Errorf("processor %d (redact): %w", i, err)
			}
			mp.AddProcessor(rp)
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
	case "fathom":
		return metric.NewFathomExporter(cfg.Endpoint, cfg.Timeout.Std(), retry)
	default:
		return nil, fmt.Errorf("metric exporter: unknown type %q", cfg.Type)
	}
}

func buildLogPipeline(cfg config.LogPipelineConfig, base config.PipelineConfig, logger *slog.Logger) (*logs.Pipeline, error) {
	lp := logs.NewPipeline(base.Workers, base.QueueSize, logger)

	for i, pc := range cfg.Processors {
		switch pc.Type {
		case "redact":
			var rc config.RedactConfig
			if err := pc.Raw.Decode(&rc); err != nil {
				return nil, fmt.Errorf("processor %d (redact): %w", i, err)
			}
			rp, err := logs.NewRedactProcessor(rc.CredsPatterns)
			if err != nil {
				return nil, fmt.Errorf("processor %d (redact): %w", i, err)
			}
			lp.AddProcessor(rp)
		default:
			return nil, fmt.Errorf("processor %d: unknown type %q", i, pc.Type)
		}
	}

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
	case "fathom":
		return logs.NewFathomExporter(cfg.Endpoint, cfg.Timeout.Std(), retry)
	default:
		return nil, fmt.Errorf("log exporter: unknown type %q", cfg.Type)
	}
}

// OTLPHTTPAddr returns the bound address of the OTLP HTTP ingress (for tests).
func (a *App) OTLPHTTPAddr() string {
	if a.ingress == nil {
		return ""
	}
	return a.ingress.HTTPAddr()
}

// OTLPGRPCAddr returns the bound address of the OTLP gRPC ingress (for tests).
func (a *App) OTLPGRPCAddr() string {
	if a.ingress == nil {
		return ""
	}
	return a.ingress.GRPCAddr()
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
	if err := a.pipeline.Start(ctx); err != nil {
		return err
	}
	// The ingress starts last: every pipeline it feeds is already consuming, so
	// no Enqueue can race a not-yet-started worker pool.
	if a.ingress != nil {
		if err := a.ingress.Start(); err != nil {
			return fmt.Errorf("otlp ingress: %w", err)
		}
	}
	a.ready.Store(true)
	return nil
}

func (a *App) Shutdown(ctx context.Context) error {
	// Stop accepting first: after Stop returns no ingress handler is mid-Enqueue,
	// so it is safe to close the pipeline queues below.
	if a.ingress != nil {
		if err := a.ingress.Stop(ctx); err != nil {
			a.logger.Error("otlp ingress stop error", "err", err)
		}
	}
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

func (a *App) addMetricsServer(p *pipeline.Pipeline[model.Batch], endpoint string) {
	var srv *http.Server
	a.startHooks = append(a.startHooks, func(ctx context.Context) error {
		ln, err := net.Listen("tcp", endpoint)
		if err != nil {
			return fmt.Errorf("metrics: listen %s: %w", endpoint, err)
		}
		srv = &http.Server{Handler: a.selfObsMux(p)}
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

// selfObsMux serves the operational endpoints: Prometheus-text /metrics
// (coral_* names, all signal pipelines) plus liveness /healthz and readiness
// /readyz (contract §9).
func (a *App) selfObsMux(p *pipeline.Pipeline[model.Batch]) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		batchesIn, batchesDropped, spansOut := p.Stats()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w, "# TYPE coral_batches_in counter\ncoral_batches_in %d\n", batchesIn)
		_, _ = fmt.Fprintf(w, "# TYPE coral_batches_dropped counter\ncoral_batches_dropped %d\n", batchesDropped)
		_, _ = fmt.Fprintf(w, "# TYPE coral_spans_out counter\ncoral_spans_out %d\n", spansOut)
		if a.metricPipeline != nil {
			_, _, pointsOut := a.metricPipeline.Stats()
			_, _ = fmt.Fprintf(w, "# TYPE coral_metric_points_out counter\ncoral_metric_points_out %d\n", pointsOut)
		}
		if a.logPipeline != nil {
			_, _, recordsOut := a.logPipeline.Stats()
			_, _ = fmt.Fprintf(w, "# TYPE coral_log_records_out counter\ncoral_log_records_out %d\n", recordsOut)
		}
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if a.ready.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	return mux
}

// addReceivers attaches the legacy trace receivers (Jaeger/Zipkin) directly to
// the trace pipeline. OTLP (all signals) is served by the shared ingress, wired
// separately in addIngress.
func (a *App) addReceivers(p *pipeline.Pipeline[model.Batch], cfg config.ReceiversConfig, logger *slog.Logger) error {
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

func buildProcessor(pc config.ProcessorConfig, p *pipeline.Pipeline[model.Batch], a *App, processorIndex int) (pipeline.Processor[model.Batch], error) {
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
				Scope:  a.Scope,
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

func buildExporter(ec config.ExporterConfig, a *App) (pipeline.Exporter[model.Batch], error) {
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

	case "fathom":
		var cfg config.AmberConfig
		if err := ec.Raw.Decode(&cfg); err != nil {
			return nil, err
		}
		e, err := fathomexp.New(cfg.Endpoint, cfg.Timeout.Std())
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
