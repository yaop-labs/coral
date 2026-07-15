package otlp

import (
	"context"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/yaop-labs/coral/internal/logs"
	"github.com/yaop-labs/coral/internal/metric"
	"github.com/yaop-labs/coral/internal/model"
	"github.com/yaop-labs/coral/internal/otlphttp"
)

const defaultMaxRecvBytes = 16 << 20

// Sink holds the per-signal callbacks that hand an accepted batch to its
// pipeline. A nil callback means that signal is not served: its gRPC service is
// left unregistered (standard clients see Unimplemented) and its HTTP route
// returns 404.
//
// The optional *Admit hooks run synchronously at accept time: they return the
// records to admit plus the count and reason of records rejected as permanently
// invalid. Rejections are reported to the sender via OTLP partial_success
// (contract §4) so it does not retry them; admitted records are enqueued as
// usual. A nil Admit hook admits everything.
type Sink struct {
	Traces  func(context.Context, model.Batch) error
	Metrics func(context.Context, metric.Batch) error
	Logs    func(context.Context, logs.Batch) error

	TraceAdmit  func(model.Batch) (admit model.Batch, rejected int, reason string)
	MetricAdmit func(metric.Batch) (admit metric.Batch, rejected int, reason string)
	LogAdmit    func(logs.Batch) (admit logs.Batch, rejected int, reason string)
}

// Server is the unified OTLP ingress: one gRPC server and one HTTP mux serving
// traces, metrics, and logs on the platform's standard 4317/4318 ports
// (contract §2). It replaces the former per-signal receivers so a stock OTel
// SDK — which sends every signal to a single endpoint — just works.
type Server struct {
	grpcAddr string
	httpAddr string
	maxRecv  int
	sink     Sink

	grpcSrv *grpc.Server
	httpSrv *http.Server
	grpcLn  net.Listener
	httpLn  net.Listener

	mu    sync.Mutex
	ready chan struct{} // closed once listeners are bound (or bind failed)

	requests       atomic.Uint64
	errs           atomic.Uint64
	tracesAccepted atomic.Uint64
	pointsAccepted atomic.Uint64
	logsAccepted   atomic.Uint64
	tracesRejected atomic.Uint64
	pointsRejected atomic.Uint64
	logsRejected   atomic.Uint64
}

// NewServer builds an ingress bound to grpcAddr and/or httpAddr (either may be
// empty to disable that transport). sink selects which signals are served.
func NewServer(grpcAddr, httpAddr string, maxRecvBytes int, sink Sink) *Server {
	if maxRecvBytes <= 0 {
		maxRecvBytes = defaultMaxRecvBytes
	}
	return &Server{
		grpcAddr: grpcAddr,
		httpAddr: httpAddr,
		maxRecv:  maxRecvBytes,
		sink:     sink,
		ready:    make(chan struct{}),
	}
}

// Start binds the listeners and begins serving, returning once both are bound
// (or a bind fails). It does not block; call Stop to shut down. Start must run
// after the target pipelines are started, since it feeds them via Sink.
func (s *Server) Start() error {
	defer close(s.ready)

	if s.grpcAddr != "" {
		ln, err := net.Listen("tcp", s.grpcAddr)
		if err != nil {
			return err
		}
		srv := grpc.NewServer(grpc.MaxRecvMsgSize(s.maxRecv))
		if s.sink.Traces != nil {
			coltracepb.RegisterTraceServiceServer(srv, &grpcTraceService{s: s})
		}
		if s.sink.Metrics != nil {
			colmetricspb.RegisterMetricsServiceServer(srv, &grpcMetricsService{s: s})
		}
		if s.sink.Logs != nil {
			collogspb.RegisterLogsServiceServer(srv, &grpcLogsService{s: s})
		}
		s.mu.Lock()
		s.grpcLn, s.grpcSrv = ln, srv
		s.mu.Unlock()
		go func() { _ = srv.Serve(ln) }()
	}

	if s.httpAddr != "" {
		ln, err := net.Listen("tcp", s.httpAddr)
		if err != nil {
			return err
		}
		mux := http.NewServeMux()
		if s.sink.Traces != nil {
			mux.HandleFunc("/v1/traces", s.handleTraces)
		}
		if s.sink.Metrics != nil {
			mux.HandleFunc("/v1/metrics", s.handleMetrics)
		}
		if s.sink.Logs != nil {
			mux.HandleFunc("/v1/logs", s.handleLogs)
		}
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		s.mu.Lock()
		s.httpLn = ln
		s.httpSrv = &http.Server{Handler: mux, ReadTimeout: 10 * time.Second}
		s.mu.Unlock()
		go func() { _ = s.httpSrv.Serve(ln) }()
	}
	return nil
}

// Stop gracefully drains in-flight requests, then closes both transports. After
// Stop returns no handler is mid-flight, so the fed pipelines can be shut down.
func (s *Server) Stop(ctx context.Context) error {
	<-s.ready
	s.mu.Lock()
	grpcSrv, httpSrv := s.grpcSrv, s.httpSrv
	s.mu.Unlock()

	if grpcSrv != nil {
		done := make(chan struct{})
		go func() { grpcSrv.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-ctx.Done():
			grpcSrv.Stop()
		}
	}
	if httpSrv != nil {
		return httpSrv.Shutdown(ctx)
	}
	return nil
}

// GRPCAddr returns the bound gRPC listener address (useful with :0 in tests).
func (s *Server) GRPCAddr() string {
	<-s.ready
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.grpcLn == nil {
		return ""
	}
	return s.grpcLn.Addr().String()
}

// HTTPAddr returns the bound HTTP listener address (useful with :0 in tests).
func (s *Server) HTTPAddr() string {
	<-s.ready
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.httpLn == nil {
		return ""
	}
	return s.httpLn.Addr().String()
}

// Stats reports ingress counters for observability.
func (s *Server) Stats() (requests, errs, traces, points, logs uint64) {
	return s.requests.Load(), s.errs.Load(),
		s.tracesAccepted.Load(), s.pointsAccepted.Load(), s.logsAccepted.Load()
}

// Rejected reports records refused at accept time and reported via
// partial_success (spans, data points, log records).
func (s *Server) Rejected() (traces, points, logs uint64) {
	return s.tracesRejected.Load(), s.pointsRejected.Load(), s.logsRejected.Load()
}

// --- accept-time admission ---

// admitTraces applies the trace admit hook (if any), enqueues the admitted
// spans, and reports how many were rejected as invalid (partial_success).
func (s *Server) admitTraces(ctx context.Context, spans []model.Span) (rejected int, reason string, err error) {
	b := model.Batch{Spans: spans}
	if s.sink.TraceAdmit != nil {
		b, rejected, reason = s.sink.TraceAdmit(b)
	}
	if b.Len() > 0 {
		if err = s.sink.Traces(ctx, b); err != nil {
			return 0, "", err
		}
		s.tracesAccepted.Add(uint64(b.Len()))
	}
	if rejected > 0 {
		s.tracesRejected.Add(uint64(rejected))
	}
	return rejected, reason, nil
}

func (s *Server) admitMetrics(ctx context.Context, rm []*metricspb.ResourceMetrics) (rejected int, reason string, err error) {
	b := metric.Batch{ResourceMetrics: rm}
	if s.sink.MetricAdmit != nil {
		b, rejected, reason = s.sink.MetricAdmit(b)
	}
	if b.Len() > 0 {
		if err = s.sink.Metrics(ctx, b); err != nil {
			return 0, "", err
		}
		s.pointsAccepted.Add(uint64(b.Len()))
	}
	if rejected > 0 {
		s.pointsRejected.Add(uint64(rejected))
	}
	return rejected, reason, nil
}

func (s *Server) admitLogs(ctx context.Context, rl []*logspb.ResourceLogs) (rejected int, reason string, err error) {
	b := logs.Batch{ResourceLogs: rl}
	if s.sink.LogAdmit != nil {
		b, rejected, reason = s.sink.LogAdmit(b)
	}
	if b.Len() > 0 {
		if err = s.sink.Logs(ctx, b); err != nil {
			return 0, "", err
		}
		s.logsAccepted.Add(uint64(b.Len()))
	}
	if rejected > 0 {
		s.logsRejected.Add(uint64(rejected))
	}
	return rejected, reason, nil
}

// --- gRPC services ---

type grpcTraceService struct {
	coltracepb.UnimplementedTraceServiceServer
	s *Server
}

func (g *grpcTraceService) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	g.s.requests.Add(1)
	spans := spansFromResourceSpans(req.GetResourceSpans())
	rejected, reason, err := g.s.admitTraces(ctx, spans)
	if err != nil {
		g.s.errs.Add(1)
		return nil, status.Error(codes.Unavailable, "pipeline unavailable")
	}
	resp := &coltracepb.ExportTraceServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &coltracepb.ExportTracePartialSuccess{
			RejectedSpans: int64(rejected), ErrorMessage: reason,
		}
	}
	return resp, nil
}

type grpcMetricsService struct {
	colmetricspb.UnimplementedMetricsServiceServer
	s *Server
}

func (g *grpcMetricsService) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	g.s.requests.Add(1)
	rejected, reason, err := g.s.admitMetrics(ctx, req.GetResourceMetrics())
	if err != nil {
		g.s.errs.Add(1)
		return nil, status.Error(codes.Unavailable, "pipeline unavailable")
	}
	resp := &colmetricspb.ExportMetricsServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &colmetricspb.ExportMetricsPartialSuccess{
			RejectedDataPoints: int64(rejected), ErrorMessage: reason,
		}
	}
	return resp, nil
}

type grpcLogsService struct {
	collogspb.UnimplementedLogsServiceServer
	s *Server
}

func (g *grpcLogsService) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	g.s.requests.Add(1)
	rejected, reason, err := g.s.admitLogs(ctx, req.GetResourceLogs())
	if err != nil {
		g.s.errs.Add(1)
		return nil, status.Error(codes.Unavailable, "pipeline unavailable")
	}
	resp := &collogspb.ExportLogsServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &collogspb.ExportLogsPartialSuccess{
			RejectedLogRecords: int64(rejected), ErrorMessage: reason,
		}
	}
	return resp, nil
}

// --- HTTP handlers ---

func (s *Server) handleTraces(w http.ResponseWriter, req *http.Request) {
	s.requests.Add(1)
	body, enc, ok := otlphttp.ReadBody(w, req, int64(s.maxRecv))
	if !ok {
		s.errs.Add(1)
		return
	}
	var pb coltracepb.ExportTraceServiceRequest
	if err := otlphttp.Unmarshal(enc, body, &pb); err != nil {
		s.errs.Add(1)
		http.Error(w, "bad payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	spans := spansFromResourceSpans(pb.GetResourceSpans())
	rejected, reason, err := s.admitTraces(req.Context(), spans)
	if err != nil {
		s.errs.Add(1)
		http.Error(w, "pipeline unavailable", http.StatusServiceUnavailable)
		return
	}
	resp := &coltracepb.ExportTraceServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &coltracepb.ExportTracePartialSuccess{
			RejectedSpans: int64(rejected), ErrorMessage: reason,
		}
	}
	writeProtoResponse(w, resp)
}

func (s *Server) handleMetrics(w http.ResponseWriter, req *http.Request) {
	s.requests.Add(1)
	body, enc, ok := otlphttp.ReadBody(w, req, int64(s.maxRecv))
	if !ok {
		s.errs.Add(1)
		return
	}
	var pb colmetricspb.ExportMetricsServiceRequest
	if err := otlphttp.Unmarshal(enc, body, &pb); err != nil {
		s.errs.Add(1)
		http.Error(w, "bad payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	rejected, reason, err := s.admitMetrics(req.Context(), pb.GetResourceMetrics())
	if err != nil {
		s.errs.Add(1)
		http.Error(w, "pipeline unavailable", http.StatusServiceUnavailable)
		return
	}
	resp := &colmetricspb.ExportMetricsServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &colmetricspb.ExportMetricsPartialSuccess{
			RejectedDataPoints: int64(rejected), ErrorMessage: reason,
		}
	}
	writeProtoResponse(w, resp)
}

func (s *Server) handleLogs(w http.ResponseWriter, req *http.Request) {
	s.requests.Add(1)
	body, enc, ok := otlphttp.ReadBody(w, req, int64(s.maxRecv))
	if !ok {
		s.errs.Add(1)
		return
	}
	var pb collogspb.ExportLogsServiceRequest
	if err := otlphttp.Unmarshal(enc, body, &pb); err != nil {
		s.errs.Add(1)
		http.Error(w, "bad payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	rejected, reason, err := s.admitLogs(req.Context(), pb.GetResourceLogs())
	if err != nil {
		s.errs.Add(1)
		http.Error(w, "pipeline unavailable", http.StatusServiceUnavailable)
		return
	}
	resp := &collogspb.ExportLogsServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &collogspb.ExportLogsPartialSuccess{
			RejectedLogRecords: int64(rejected), ErrorMessage: reason,
		}
	}
	writeProtoResponse(w, resp)
}

func writeProtoResponse(w http.ResponseWriter, m proto.Message) {
	resp, _ := proto.Marshal(m)
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(resp)
}
