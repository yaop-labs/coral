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
type Sink struct {
	Traces  func(context.Context, model.Batch) error
	Metrics func(context.Context, metric.Batch) error
	Logs    func(context.Context, logs.Batch) error
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

// --- gRPC services ---

type grpcTraceService struct {
	coltracepb.UnimplementedTraceServiceServer
	s *Server
}

func (g *grpcTraceService) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	g.s.requests.Add(1)
	spans := spansFromResourceSpans(req.GetResourceSpans())
	if len(spans) == 0 {
		return &coltracepb.ExportTraceServiceResponse{}, nil
	}
	if err := g.s.sink.Traces(ctx, model.Batch{Spans: spans}); err != nil {
		g.s.errs.Add(1)
		return nil, status.Error(codes.Unavailable, "pipeline unavailable")
	}
	g.s.tracesAccepted.Add(uint64(len(spans)))
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

type grpcMetricsService struct {
	colmetricspb.UnimplementedMetricsServiceServer
	s *Server
}

func (g *grpcMetricsService) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	g.s.requests.Add(1)
	b := metric.Batch{ResourceMetrics: req.GetResourceMetrics()}
	if n := b.Len(); n > 0 {
		if err := g.s.sink.Metrics(ctx, b); err != nil {
			g.s.errs.Add(1)
			return nil, status.Error(codes.Unavailable, "pipeline unavailable")
		}
		g.s.pointsAccepted.Add(uint64(n))
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

type grpcLogsService struct {
	collogspb.UnimplementedLogsServiceServer
	s *Server
}

func (g *grpcLogsService) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	g.s.requests.Add(1)
	b := logs.Batch{ResourceLogs: req.GetResourceLogs()}
	if n := b.Len(); n > 0 {
		if err := g.s.sink.Logs(ctx, b); err != nil {
			g.s.errs.Add(1)
			return nil, status.Error(codes.Unavailable, "pipeline unavailable")
		}
		g.s.logsAccepted.Add(uint64(n))
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
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
	if len(spans) > 0 {
		if err := s.sink.Traces(req.Context(), model.Batch{Spans: spans}); err != nil {
			s.errs.Add(1)
			http.Error(w, "pipeline unavailable", http.StatusServiceUnavailable)
			return
		}
		s.tracesAccepted.Add(uint64(len(spans)))
	}
	writeProtoResponse(w, &coltracepb.ExportTraceServiceResponse{})
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
	b := metric.Batch{ResourceMetrics: pb.GetResourceMetrics()}
	if n := b.Len(); n > 0 {
		if err := s.sink.Metrics(req.Context(), b); err != nil {
			s.errs.Add(1)
			http.Error(w, "pipeline unavailable", http.StatusServiceUnavailable)
			return
		}
		s.pointsAccepted.Add(uint64(n))
	}
	writeProtoResponse(w, &colmetricspb.ExportMetricsServiceResponse{})
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
	b := logs.Batch{ResourceLogs: pb.GetResourceLogs()}
	if n := b.Len(); n > 0 {
		if err := s.sink.Logs(req.Context(), b); err != nil {
			s.errs.Add(1)
			http.Error(w, "pipeline unavailable", http.StatusServiceUnavailable)
			return
		}
		s.logsAccepted.Add(uint64(n))
	}
	writeProtoResponse(w, &collogspb.ExportLogsServiceResponse{})
}

func writeProtoResponse(w http.ResponseWriter, m proto.Message) {
	resp, _ := proto.Marshal(m)
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(resp)
}
