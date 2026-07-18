// Package app builds a collector from config.Config.
package app

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/yaop-labs/coral/internal/buildinfo"
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
	"github.com/yaop-labs/gyre"
	"github.com/yaop-labs/reef/bearer"
	"github.com/yaop-labs/reef/credential"
	"github.com/yaop-labs/reef/edge"
	"github.com/yaop-labs/reef/tlsconf"
)

// App is a fully wired but unstarted collector.
type App struct {
	logger   *slog.Logger
	pipeline *pipeline.Pipeline[model.Batch]

	metricPipeline *metric.Pipeline // nil unless metric_pipeline is configured
	logPipeline    *logs.Pipeline   // nil unless log_pipeline is configured
	credentialObs  *credentialMetrics

	// ingress is the unified OTLP endpoint (4317/4318) feeding every signal
	// pipeline; nil when no OTLP receiver is configured. Legacy Jaeger/Zipkin
	// trace receivers remain attached directly to the trace pipeline.
	ingress     *otlprecv.Server
	hooks       []lifecycleHook
	selfObsAddr string

	lifecycleMu sync.Mutex
	statusMu    sync.RWMutex
	state       gyre.State
	since       time.Time
	condition   gyre.Condition

	startedHooks   int
	metricStarted  bool
	logStarted     bool
	traceStarted   bool
	ingressStarted bool
	startAttempted bool
	closed         bool
	closeDone      chan struct{}
	closeErr       error
}

type lifecycleHook struct {
	start func(context.Context) error
	stop  func(context.Context) error
}

var _ gyre.Component = (*App)(nil)

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
		Workers:    cfg.Pipeline.Workers,
		QueueSize:  cfg.Pipeline.QueueSize,
		QueueBytes: cfg.Pipeline.QueueBytes,
	}, logger)

	now := time.Now().UTC()
	a := &App{
		logger:        logger,
		pipeline:      p,
		credentialObs: &credentialMetrics{},
		state:         gyre.StateStarting,
		since:         now,
		condition: gyre.Condition{
			Type:           "Ready",
			Status:         false,
			Reason:         "starting",
			Message:        "component has not started",
			LastTransition: now,
		},
	}
	constructed := false
	defer func() {
		if !constructed {
			a.closeUnstarted()
		}
	}()

	if err := a.addReceivers(p, cfg.Receivers, logger); err != nil {
		return nil, err
	}
	if cfg.Metrics.Endpoint != "" {
		if err := a.addMetricsServer(p, cfg.Metrics); err != nil {
			return nil, fmt.Errorf("metrics: %w", err)
		}
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
		mp, err := buildMetricPipeline(*cfg.MetricPipeline, cfg.Pipeline, logger, a.credentialObs)
		if err != nil {
			return nil, fmt.Errorf("metric_pipeline: %w", err)
		}
		a.metricPipeline = mp
		logger.Info("metric pipeline enabled")
	}
	if cfg.LogPipeline != nil {
		lp, err := buildLogPipeline(*cfg.LogPipeline, cfg.Pipeline, logger, a.credentialObs)
		if err != nil {
			return nil, fmt.Errorf("log_pipeline: %w", err)
		}
		a.logPipeline = lp
		logger.Info("log pipeline enabled")
	}

	if err := a.addIngress(cfg.Receivers, cfg.TenantMap, cfg.TenantLimits, traceActive, validateSpanLimit(cfg.Processors)); err != nil {
		return nil, fmt.Errorf("otlp ingress: %w", err)
	}
	constructed = true
	return a, nil
}

func (a *App) closeUnstarted() {
	if a.logPipeline != nil {
		if err := a.logPipeline.CloseUnstarted(); err != nil {
			a.logger.Error("close unstarted log pipeline", "err", err)
		}
	}
	if a.metricPipeline != nil {
		if err := a.metricPipeline.CloseUnstarted(); err != nil {
			a.logger.Error("close unstarted metric pipeline", "err", err)
		}
	}
	if err := a.pipeline.CloseUnstarted(); err != nil {
		a.logger.Error("close unstarted trace pipeline", "err", err)
	}
}

// addIngress builds the unified OTLP endpoint from the top-level receiver
// config, routing each signal to the pipeline that serves it. Signals whose
// pipeline is absent are left unserved (Unimplemented / 404). A config with
// only legacy trace receivers builds no ingress. spanLimit (>0) enables
// accept-time rejection of oversized spans with a partial_success report.
func (a *App) addIngress(cfg config.ReceiversConfig, tenantMap map[string]string, tenantLimits map[string]config.TenantLimit, traceActive bool, spanLimit int) error {
	grpcAddr, httpAddr := "", ""
	security := otlprecv.SecurityConfig{}
	security.TenantMap = tenantMap
	security.TenantLimits = make(map[string]otlprecv.TenantLimit, len(tenantLimits))
	for name, limit := range tenantLimits {
		security.TenantLimits[name] = otlprecv.TenantLimit{MaxItems: limit.MaxItems, MaxBytes: limit.MaxBytes}
	}
	if cfg.OTLPGRPC != nil {
		grpcAddr = cfg.OTLPGRPC.Endpoint
		security.GRPC = serverEdgeConfig(*cfg.OTLPGRPC, a.credentialObs)
	}
	if cfg.OTLPHTTP != nil {
		httpAddr = cfg.OTLPHTTP.Endpoint
		security.HTTP = serverEdgeConfig(*cfg.OTLPHTTP, a.credentialObs)
	}
	if grpcAddr == "" && httpAddr == "" {
		return nil
	}
	sink := otlprecv.Sink{}
	if traceActive {
		sink.Traces = a.pipeline.Enqueue
		if spanLimit > 0 {
			sink.TraceAdmit = traceSizeAdmit(spanLimit)
		}
	}
	if a.metricPipeline != nil {
		sink.Metrics = a.metricPipeline.Enqueue
	}
	if a.logPipeline != nil {
		sink.Logs = a.logPipeline.Enqueue
	}
	ingress, err := otlprecv.NewSecureServer(grpcAddr, httpAddr, 0, sink, security)
	if err != nil {
		return err
	}
	a.ingress = ingress
	a.logger.Info("otlp ingress enabled", "grpc", grpcAddr, "http", httpAddr)
	return nil
}

// validateSpanLimit returns the span-size limit of a configured validate
// processor (mirroring its default), or 0 if none is configured. The ingress
// uses it to reject oversized spans at accept time and report them via OTLP
// partial_success (contract §4), rather than dropping them silently downstream.
func validateSpanLimit(procs []config.ProcessorConfig) int {
	for _, pc := range procs {
		if pc.Type != "validate" {
			continue
		}
		var vc config.ValidateConfig
		if err := pc.Raw.Decode(&vc); err != nil {
			return 0
		}
		if vc.MaxSpanBytes > 0 {
			return vc.MaxSpanBytes
		}
		return 64 * 1024 // matches processor.ValidateProcessor's default
	}
	return 0
}

// traceSizeAdmit builds an accept-time admit hook that rejects spans over
// maxSpanBytes, so the sender is told (partial_success) rather than silently
// losing them or retrying invalid records.
func traceSizeAdmit(maxSpanBytes int) func(model.Batch) (model.Batch, int, string) {
	return func(b model.Batch) (model.Batch, int, string) {
		kept := b.Spans[:0]
		rejected := 0
		for _, s := range b.Spans {
			if s.SizeBytes() > maxSpanBytes {
				rejected++
				continue
			}
			kept = append(kept, s)
		}
		reason := ""
		if rejected > 0 {
			reason = fmt.Sprintf("rejected %d span(s) exceeding max_span_bytes=%d", rejected, maxSpanBytes)
		}
		return model.Batch{Spans: kept}, rejected, reason
	}
}

func buildMetricPipeline(
	cfg config.MetricPipelineConfig,
	base config.PipelineConfig,
	logger *slog.Logger,
	observer credential.Observer,
) (*metric.Pipeline, error) {
	mp := metric.NewPipeline(base.Workers, base.QueueSize, logger, base.QueueBytes)
	mp.AddProcessor(metric.NewServiceNameProcessor()) // contract §6

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
		exp, err := buildMetricExporter(exporterCfg, observer)
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

func buildMetricExporter(cfg config.MetricExporterConfig, observer credential.Observer) (metric.Exporter, error) {
	retry := metric.RetryPolicy{
		MaxAttempts:    cfg.Retry.MaxAttempts,
		InitialBackoff: cfg.Retry.InitialBackoff.Std(),
		MaxBackoff:     cfg.Retry.MaxBackoff.Std(),
	}
	switch cfg.Type {
	case "", "amber":
		return metric.NewAmberExporter(cfg.Endpoint, cfg.Timeout.Std(), retry, clientEdgeConfig(
			cfg.Endpoint, cfg.TLS, cfg.Auth, cfg.EdgePolicyConfig, observer,
		))
	case "fathom":
		return metric.NewFathomExporter(cfg.Endpoint, cfg.Timeout.Std(), retry, clientEdgeConfig(
			cfg.Endpoint, cfg.TLS, cfg.Auth, cfg.EdgePolicyConfig, observer,
		))
	default:
		return nil, fmt.Errorf("metric exporter: unknown type %q", cfg.Type)
	}
}

func buildLogPipeline(
	cfg config.LogPipelineConfig,
	base config.PipelineConfig,
	logger *slog.Logger,
	observer credential.Observer,
) (*logs.Pipeline, error) {
	lp := logs.NewPipeline(base.Workers, base.QueueSize, logger, base.QueueBytes)
	lp.AddProcessor(logs.NewServiceNameProcessor()) // contract §6

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
		exp, err := buildLogExporter(exporterCfg, observer)
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

func buildLogExporter(cfg config.LogExporterConfig, observer credential.Observer) (logs.Exporter, error) {
	retry := logs.RetryPolicy{
		MaxAttempts:    cfg.Retry.MaxAttempts,
		InitialBackoff: cfg.Retry.InitialBackoff.Std(),
		MaxBackoff:     cfg.Retry.MaxBackoff.Std(),
	}
	switch cfg.Type {
	case "", "amber":
		return logs.NewAmberExporter(cfg.Endpoint, cfg.Timeout.Std(), retry, clientEdgeConfig(
			cfg.Endpoint, cfg.TLS, cfg.Auth, cfg.EdgePolicyConfig, observer,
		))
	case "fathom":
		return logs.NewFathomExporter(cfg.Endpoint, cfg.Timeout.Std(), retry, clientEdgeConfig(
			cfg.Endpoint, cfg.TLS, cfg.Auth, cfg.EdgePolicyConfig, observer,
		))
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

// SelfObservabilityAddr returns the bound operational HTTP address for tests.
func (a *App) SelfObservabilityAddr() string { return a.selfObsAddr }

// Name returns Coral's stable Gyre component identity.
func (a *App) Name() string { return "coral" }

// Version returns the same release identity exposed by --version and
// coral_build_info.
func (a *App) Version() string { return buildinfo.Current().Version }

// Ready reports whether Coral can currently accept its advertised workload.
func (a *App) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return gyre.E(gyre.CodeUnavailable, a.Name(), "ready", true, err)
	}
	snapshot := a.Status()
	if snapshot.State == gyre.StateReady {
		return nil
	}
	reason := string(snapshot.State)
	if len(snapshot.Conditions) != 0 && snapshot.Conditions[0].Reason != "" {
		reason = snapshot.Conditions[0].Reason
	}
	return gyre.E(
		gyre.CodeUnavailable,
		a.Name(),
		"ready",
		true,
		fmt.Errorf("component is not ready: %s", reason),
	)
}

// Status returns a bounded, secret-free Gyre lifecycle snapshot.
func (a *App) Status() gyre.Snapshot {
	a.statusMu.RLock()
	defer a.statusMu.RUnlock()
	return gyre.Snapshot{
		Name:       a.Name(),
		Version:    a.Version(),
		State:      a.state,
		Generation: 0, // static configuration; reload is a later capability.
		Since:      a.since,
		Conditions: []gyre.Condition{a.condition},
	}
}

func (a *App) Start(ctx context.Context) error {
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	if a.closed {
		return gyre.E(
			gyre.CodeShuttingDown,
			a.Name(),
			"start",
			false,
			errors.New("component is closed"),
		)
	}
	if a.startAttempted {
		if a.Status().State == gyre.StateReady {
			return nil
		}
		return gyre.E(
			gyre.CodeUnavailable,
			a.Name(),
			"start",
			false,
			errors.New("component start was already attempted"),
		)
	}
	a.startAttempted = true
	a.transition(gyre.StateStarting, "starting", "component startup is in progress")

	for _, hook := range a.hooks {
		if err := hook.start(ctx); err != nil {
			return a.startFailed("start_hook", err)
		}
		a.startedHooks++
	}
	if a.metricPipeline != nil {
		if err := a.metricPipeline.Start(ctx); err != nil {
			return a.startFailed("metric_pipeline", err)
		}
		a.metricStarted = true
	}
	if a.logPipeline != nil {
		if err := a.logPipeline.Start(ctx); err != nil {
			return a.startFailed("log_pipeline", err)
		}
		a.logStarted = true
	}
	if err := a.pipeline.Start(ctx); err != nil {
		return a.startFailed("trace_pipeline", err)
	}
	a.traceStarted = true
	// The ingress starts last: every pipeline it feeds is already consuming, so
	// no Enqueue can race a not-yet-started worker pool.
	if a.ingress != nil {
		if err := a.ingress.Start(); err != nil {
			return a.startFailed("otlp_ingress", err)
		}
		a.ingressStarted = true
	}
	a.transition(gyre.StateReady, "ready", "component is accepting telemetry")
	return nil
}

func (a *App) startFailed(operation string, cause error) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cleanupErr := a.shutdownStarted(cleanupCtx)
	a.closed = true
	a.closeDone = make(chan struct{})
	a.closeErr = cleanupErr
	close(a.closeDone)
	a.transition(gyre.StateFailed, operation+"_failed", "startup failed and started resources were rolled back")
	return gyre.E(
		gyre.CodeUnavailable,
		a.Name(),
		operation,
		true,
		errors.Join(cause, cleanupErr),
	)
}

// Close implements Gyre's idempotent, context-bounded shutdown contract.
// Cleanup continues in the background if the caller's deadline expires.
func (a *App) Close(ctx context.Context) error {
	a.lifecycleMu.Lock()
	if a.closeDone == nil {
		a.closed = true
		a.closeDone = make(chan struct{})
		if !a.startAttempted {
			a.transition(gyre.StateStopping, "close_before_start", "component was closed before startup")
			a.transition(gyre.StateStopped, "stopped", "component is stopped")
			close(a.closeDone)
		} else {
			a.transition(gyre.StateStopping, "shutdown", "component shutdown is in progress")
			go a.finishClose(ctx)
		}
	}
	done := a.closeDone
	a.lifecycleMu.Unlock()

	select {
	case <-done:
		a.lifecycleMu.Lock()
		err := a.closeErr
		a.lifecycleMu.Unlock()
		return err
	case <-ctx.Done():
		return gyre.E(gyre.CodeShuttingDown, a.Name(), "close", true, ctx.Err())
	}
}

func (a *App) finishClose(ctx context.Context) {
	err := a.shutdownStarted(ctx)
	if err != nil {
		a.transition(gyre.StateFailed, "shutdown_failed", "one or more resources failed to stop")
	} else {
		a.transition(gyre.StateStopped, "stopped", "component is stopped")
	}
	a.lifecycleMu.Lock()
	a.closeErr = err
	close(a.closeDone)
	a.lifecycleMu.Unlock()
}

func (a *App) shutdownStarted(ctx context.Context) error {
	var errs []error
	// Stop accepting first: after Stop returns no ingress handler is mid-Enqueue,
	// so it is safe to close the pipeline queues below.
	if a.ingressStarted {
		if err := a.ingress.Stop(ctx); err != nil {
			a.logger.Error("otlp ingress stop error", "err", err)
			errs = append(errs, fmt.Errorf("otlp ingress: %w", err))
		}
		a.ingressStarted = false
	}
	if a.traceStarted {
		if err := a.pipeline.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("trace pipeline: %w", err))
		}
		a.traceStarted = false
	}
	if a.logStarted {
		if err := a.logPipeline.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("log pipeline: %w", err))
		}
		a.logStarted = false
	}
	if a.metricStarted {
		if err := a.metricPipeline.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("metric pipeline: %w", err))
		}
		a.metricStarted = false
	}
	for i := a.startedHooks - 1; i >= 0; i-- {
		if a.hooks[i].stop == nil {
			continue
		}
		if stopErr := a.hooks[i].stop(ctx); stopErr != nil {
			a.logger.Error("app stop hook error", "err", stopErr)
			errs = append(errs, fmt.Errorf("stop hook %d: %w", i, stopErr))
		}
	}
	a.startedHooks = 0
	return errors.Join(errs...)
}

// Shutdown is retained as a compatibility alias for existing Coral callers.
func (a *App) Shutdown(ctx context.Context) error { return a.Close(ctx) }

func (a *App) transition(state gyre.State, reason, message string) {
	now := time.Now().UTC()
	a.statusMu.Lock()
	defer a.statusMu.Unlock()
	a.state = state
	a.since = now
	a.condition = gyre.Condition{
		Type:           "Ready",
		Status:         state == gyre.StateReady,
		Reason:         reason,
		Message:        message,
		LastTransition: now,
	}
}

func (a *App) addMetricsServer(p *pipeline.Pipeline[model.Batch], cfg config.MetricsConfig) error {
	var srv *http.Server
	var secured *edge.HTTPServer
	edgeConfig := edge.ServerConfig{
		Bind:                           cfg.Endpoint,
		TLS:                            cfg.TLS,
		Auth:                           cfg.Auth,
		Insecure:                       cfg.Insecure,
		DangerAllowBearerOverPlaintext: cfg.DangerAllowBearerOverPlaintext,
		ReloadInterval:                 cfg.CredentialReloadInterval.Std(),
		Observer:                       a.credentialObs,
	}
	warnings, err := edge.ValidateServer(edgeConfig)
	if err != nil {
		return err
	}
	for _, warning := range warnings {
		a.logger.Warn("reef self-observability configuration warning", "warning", string(warning))
	}
	a.hooks = append(a.hooks, lifecycleHook{
		start: func(ctx context.Context) error {
			ln, err := net.Listen("tcp", cfg.Endpoint)
			if err != nil {
				return fmt.Errorf("metrics: listen %s: %w", cfg.Endpoint, err)
			}
			a.selfObsAddr = ln.Addr().String()
			secured, err = edge.NewHTTPServer(edgeConfig)
			if err != nil {
				_ = ln.Close()
				return err
			}
			for _, warning := range secured.Warnings {
				a.logger.Warn("reef self-observability configuration warning", "warning", string(warning))
			}
			if secured.TLSConfig != nil {
				ln = tls.NewListener(ln, secured.TLSConfig.Clone())
			}
			srv = &http.Server{
				Handler:           secured.Middleware(a.selfObsMux(p)),
				ReadHeaderTimeout: 5 * time.Second,
				ReadTimeout:       10 * time.Second,
				WriteTimeout:      10 * time.Second,
				IdleTimeout:       60 * time.Second,
				MaxHeaderBytes:    16 << 10,
			}
			go func() {
				if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed && ctx.Err() == nil {
					a.transition(gyre.StateFailed, "self_observability_failed", "self-observability server exited")
					a.logger.Error("metrics server error", "err", err)
				}
			}()
			return nil
		},
		stop: func(ctx context.Context) error {
			if srv == nil {
				return nil
			}
			return errors.Join(srv.Shutdown(ctx), secured.Close())
		},
	})
	return nil
}

// selfObsMux serves the operational endpoints: Prometheus-text /metrics
// (coral_* names, all signal pipelines) plus liveness /healthz and readiness
// /readyz (contract §9).
func (a *App) selfObsMux(p *pipeline.Pipeline[model.Batch]) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		build := buildinfo.Current()
		batchesIn, batchesDropped, spansOut := p.Stats()
		traceQueueDepth, traceQueueCapacity := p.QueueDepth()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w, "# TYPE coral_build_info gauge\ncoral_build_info{version=\"%s\",revision=\"%s\",modified=\"%s\",go_version=\"%s\"} 1\n",
			prometheusLabelValue(build.Version),
			prometheusLabelValue(build.Revision),
			fmt.Sprintf("%t", build.Modified),
			prometheusLabelValue(build.GoVersion),
		)
		state := string(a.Status().State)
		ready := 0
		if state == string(gyre.StateReady) {
			ready = 1
		}
		_, _ = fmt.Fprintf(w, "# TYPE coral_ready gauge\ncoral_ready %d\n", ready)
		_, _ = fmt.Fprintf(w, "# TYPE coral_readiness_state gauge\ncoral_readiness_state{state=\"%s\"} 1\n",
			prometheusLabelValue(state))
		_, _ = fmt.Fprintln(w, "# TYPE coral_pipeline_queue_depth gauge")
		_, _ = fmt.Fprintln(w, "# TYPE coral_pipeline_queue_capacity gauge")
		_, _ = fmt.Fprintln(w, "# TYPE coral_pipeline_items_processed_total counter")
		_, _ = fmt.Fprintln(w, "# TYPE coral_pipeline_batches_dispatched_total counter")
		_, _ = fmt.Fprintln(w, "# TYPE coral_pipeline_items_dispatched_total counter")
		_, _ = fmt.Fprintln(w, "# TYPE coral_pipeline_batches_delivered_total counter")
		_, _ = fmt.Fprintln(w, "# TYPE coral_pipeline_items_delivered_total counter")
		_, _ = fmt.Fprintln(w, "# TYPE coral_pipeline_processor_failures_total counter")
		_, _ = fmt.Fprintln(w, "# TYPE coral_pipeline_exporter_failures_total counter")
		_, _ = fmt.Fprintln(w, "# TYPE coral_pipeline_exporter_queue_drops_total counter")
		_, _ = fmt.Fprintln(w, "# TYPE coral_pipeline_drain_in_progress gauge")
		_, _ = fmt.Fprintln(w, "# TYPE coral_pipeline_drain_forced gauge")
		_, _ = fmt.Fprintln(w, "# TYPE coral_pipeline_drain_duration_seconds gauge")
		_, _ = fmt.Fprintln(w, "# TYPE coral_pipeline_drain_outcome gauge")
		writeQueueMetrics(w, "traces", traceQueueDepth, traceQueueCapacity)
		writePipelineMetrics(w, "traces", p)
		_, _ = fmt.Fprintf(w, "# TYPE coral_batches_in counter\ncoral_batches_in %d\n", batchesIn)
		_, _ = fmt.Fprintf(w, "# TYPE coral_batches_dropped counter\ncoral_batches_dropped %d\n", batchesDropped)
		// Compatibility metric: "out" historically meant processed through the
		// processor chain, not confirmed delivery. New consumers should use the
		// explicitly named coral_pipeline_* metrics above.
		_, _ = fmt.Fprintf(w, "# TYPE coral_spans_out counter\ncoral_spans_out %d\n", spansOut)
		_, _ = fmt.Fprintf(w, "# TYPE coral_trace_exporter_batches_dropped counter\ncoral_trace_exporter_batches_dropped %d\n", p.ExporterDrops())
		if a.metricPipeline != nil {
			_, _, pointsOut := a.metricPipeline.Stats()
			depth, capacity := a.metricPipeline.QueueDepth()
			writeQueueMetrics(w, "metrics", depth, capacity)
			writePipelineMetrics(w, "metrics", a.metricPipeline)
			_, _ = fmt.Fprintf(w, "# TYPE coral_metric_points_out counter\ncoral_metric_points_out %d\n", pointsOut)
			_, _ = fmt.Fprintf(w, "# TYPE coral_metric_exporter_batches_dropped counter\ncoral_metric_exporter_batches_dropped %d\n", a.metricPipeline.ExporterDrops())
		}
		if a.logPipeline != nil {
			_, _, recordsOut := a.logPipeline.Stats()
			depth, capacity := a.logPipeline.QueueDepth()
			writeQueueMetrics(w, "logs", depth, capacity)
			writePipelineMetrics(w, "logs", a.logPipeline)
			_, _ = fmt.Fprintf(w, "# TYPE coral_log_records_out counter\ncoral_log_records_out %d\n", recordsOut)
			_, _ = fmt.Fprintf(w, "# TYPE coral_log_exporter_batches_dropped counter\ncoral_log_exporter_batches_dropped %d\n", a.logPipeline.ExporterDrops())
		}
		if a.ingress != nil {
			req, errs, accSpans, accPoints, accRecords := a.ingress.Stats()
			rejSpans, rejPoints, rejRecords := a.ingress.Rejected()
			_, _ = fmt.Fprintf(w, "# TYPE coral_otlp_requests counter\ncoral_otlp_requests %d\n", req)
			_, _ = fmt.Fprintf(w, "# TYPE coral_otlp_errors counter\ncoral_otlp_errors %d\n", errs)
			_, _ = fmt.Fprintf(w, "# TYPE coral_otlp_accepted_spans counter\ncoral_otlp_accepted_spans %d\n", accSpans)
			_, _ = fmt.Fprintf(w, "# TYPE coral_otlp_accepted_points counter\ncoral_otlp_accepted_points %d\n", accPoints)
			_, _ = fmt.Fprintf(w, "# TYPE coral_otlp_accepted_records counter\ncoral_otlp_accepted_records %d\n", accRecords)
			_, _ = fmt.Fprintf(w, "# TYPE coral_otlp_rejected_spans counter\ncoral_otlp_rejected_spans %d\n", rejSpans)
			_, _ = fmt.Fprintf(w, "# TYPE coral_otlp_rejected_points counter\ncoral_otlp_rejected_points %d\n", rejPoints)
			_, _ = fmt.Fprintf(w, "# TYPE coral_otlp_rejected_records counter\ncoral_otlp_rejected_records %d\n", rejRecords)
		}
		a.credentialObs.writePrometheus(w)
	})
	mux.Handle("/", gyre.HTTPHandler(a))
	return mux
}

func writeQueueMetrics(w http.ResponseWriter, signal string, depth, capacity int) {
	_, _ = fmt.Fprintf(w, "coral_pipeline_queue_depth{signal=%q} %d\n",
		signal, depth)
	_, _ = fmt.Fprintf(w, "coral_pipeline_queue_capacity{signal=%q} %d\n",
		signal, capacity)
}

func writePipelineMetrics[T pipeline.Signal](w http.ResponseWriter, signal string, p *pipeline.Pipeline[T]) {
	delivery := p.DeliveryStats()
	drain := p.DrainStats()
	inProgress := 0
	if drain.InProgress {
		inProgress = 1
	}
	forced := 0
	if drain.Forced {
		forced = 1
	}
	_, _ = fmt.Fprintf(w, "coral_pipeline_items_processed_total{signal=%q} %d\n", signal, delivery.ItemsProcessed)
	_, _ = fmt.Fprintf(w, "coral_pipeline_batches_dispatched_total{signal=%q} %d\n", signal, delivery.BatchesDispatched)
	_, _ = fmt.Fprintf(w, "coral_pipeline_items_dispatched_total{signal=%q} %d\n", signal, delivery.ItemsDispatched)
	_, _ = fmt.Fprintf(w, "coral_pipeline_batches_delivered_total{signal=%q} %d\n", signal, delivery.BatchesDelivered)
	_, _ = fmt.Fprintf(w, "coral_pipeline_items_delivered_total{signal=%q} %d\n", signal, delivery.ItemsDelivered)
	_, _ = fmt.Fprintf(w, "coral_pipeline_processor_failures_total{signal=%q} %d\n", signal, delivery.ProcessorFailures)
	_, _ = fmt.Fprintf(w, "coral_pipeline_exporter_failures_total{signal=%q} %d\n", signal, delivery.ExporterFailures)
	_, _ = fmt.Fprintf(w, "coral_pipeline_exporter_queue_drops_total{signal=%q} %d\n", signal, delivery.ExporterDrops)
	_, _ = fmt.Fprintf(w, "coral_pipeline_drain_in_progress{signal=%q} %d\n", signal, inProgress)
	_, _ = fmt.Fprintf(w, "coral_pipeline_drain_forced{signal=%q} %d\n", signal, forced)
	_, _ = fmt.Fprintf(w, "coral_pipeline_drain_duration_seconds{signal=%q} %g\n", signal, drain.Duration.Seconds())
	_, _ = fmt.Fprintf(w, "coral_pipeline_drain_outcome{signal=%q,outcome=%q} 1\n", signal, drain.Outcome)
}

func prometheusLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return strings.ReplaceAll(value, `"`, `\"`)
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
			}, cfg.MaxBytes,
		)
		a.hooks = append(a.hooks, lifecycleHook{
			start: func(ctx context.Context) error {
				ts.Start(ctx)
				return nil
			},
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
		e, err := amberexp.New(cfg.Endpoint, cfg.Timeout.Std(), clientEdgeConfig(
			cfg.Endpoint, cfg.TLS, cfg.Auth, cfg.EdgePolicyConfig, a.credentialObs,
		))
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
		e, err := fathomexp.New(cfg.Endpoint, cfg.Timeout.Std(), clientEdgeConfig(
			cfg.Endpoint, cfg.TLS, cfg.Auth, cfg.EdgePolicyConfig, a.credentialObs,
		))
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
		a.hooks = append(a.hooks, lifecycleHook{start: e.Init})
		return retryexp.Wrap(e, retryexp.Config{
			MaxAttempts:    cfg.Retry.MaxAttempts,
			InitialBackoff: cfg.Retry.InitialBackoff.Std(),
			MaxBackoff:     cfg.Retry.MaxBackoff.Std(),
		}), nil

	default:
		return nil, fmt.Errorf("unknown exporter type %q", ec.Type)
	}
}

func serverEdgeConfig(cfg config.OTLPEndpointConfig, observer credential.Observer) edge.ServerConfig {
	return edge.ServerConfig{
		TLS:                            cfg.TLS,
		Auth:                           cfg.Auth,
		Insecure:                       cfg.Insecure,
		DangerAllowBearerOverPlaintext: cfg.DangerAllowBearerOverPlaintext,
		ReloadInterval:                 cfg.CredentialReloadInterval.Std(),
		Observer:                       observer,
	}
}

func clientEdgeConfig(
	target string,
	tlsConfig *tlsconf.ClientConfig,
	authConfig *bearer.ClientConfig,
	policy config.EdgePolicyConfig,
	observer credential.Observer,
) edge.ClientConfig {
	return edge.ClientConfig{
		Target:                         target,
		TLS:                            tlsConfig,
		Auth:                           authConfig,
		Insecure:                       policy.Insecure,
		DangerAllowBearerOverPlaintext: policy.DangerAllowBearerOverPlaintext,
		ReloadInterval:                 policy.CredentialReloadInterval.Std(),
		Observer:                       observer,
	}
}
