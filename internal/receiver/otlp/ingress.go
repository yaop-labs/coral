package otlp

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/yaop-labs/coral/internal/journal"
	"github.com/yaop-labs/coral/internal/logs"
	"github.com/yaop-labs/coral/internal/metric"
	"github.com/yaop-labs/coral/internal/model"
	"github.com/yaop-labs/coral/internal/otlphttp"
	"github.com/yaop-labs/reef/bearer"
	"github.com/yaop-labs/reef/credential"
	"github.com/yaop-labs/reef/edge"
	"github.com/yaop-labs/reef/grpcreef"
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

type tenantContextKey struct{}

// TenantFromContext returns the authenticated Reef principal used as the
// default tenant identity. It is intentionally separate from payload data.
func TenantFromContext(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(tenantContextKey{}).(string)
	return v, ok && v != ""
}

func tenantContext(ctx context.Context) context.Context {
	principal, ok := bearer.PrincipalFromContext(ctx)
	if !ok || principal == "" {
		return ctx
	}
	return context.WithValue(ctx, tenantContextKey{}, principal)
}

func tenantContextWithPolicy(ctx context.Context, policy map[string]string) (context.Context, bool) {
	if len(policy) == 0 {
		return tenantContext(ctx), true
	}
	principal, ok := bearer.PrincipalFromContext(ctx)
	if !ok || principal == "" {
		return ctx, false
	}
	tenant, ok := policy[principal]
	if !ok || tenant == "" {
		return ctx, false
	}
	return context.WithValue(ctx, tenantContextKey{}, tenant), true
}

func quotaExceeded(ctx context.Context, limits map[string]TenantLimit, items int, bytes int64) bool {
	tenant, ok := TenantFromContext(ctx)
	if !ok {
		return false
	}
	l, ok := limits[tenant]
	if !ok {
		return false
	}
	return (l.MaxItems > 0 && items > l.MaxItems) || (l.MaxBytes > 0 && bytes > l.MaxBytes)
}

// Server is the unified OTLP ingress: one gRPC server and one HTTP mux serving
// traces, metrics, and logs on the platform's standard 4317/4318 ports
// (contract §2). It replaces the former per-signal receivers so a stock OTel
// SDK — which sends every signal to a single endpoint — just works.
type Server struct {
	grpcAddr       string
	httpAddr       string
	maxRecv        int
	sink           Sink
	tenantMap      map[string]string
	tenantLimits   map[string]TenantLimit
	tenantStatsMu  sync.Mutex
	tenantStats    map[string]TenantCounters
	tenantInFlight map[string]int
	dedup          *dedupWindow
	journal        *journal.Journal
	grpcSecurity   edge.ServerConfig
	httpSecurity   edge.ServerConfig

	grpcSrv       *grpc.Server
	httpSrv       *http.Server
	grpcLn        net.Listener
	httpLn        net.Listener
	httpCancel    context.CancelFunc
	httpWG        sync.WaitGroup
	httpHandlerMu sync.Mutex
	httpStopping  bool
	grpcEdge      *grpcreef.ServerEdge
	httpEdge      *edge.HTTPServer

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
	dedupHits      atomic.Uint64
	dedupConflicts atomic.Uint64
}

// NewServer builds an ingress bound to grpcAddr and/or httpAddr (either may be
// empty to disable that transport). sink selects which signals are served.
func NewServer(grpcAddr, httpAddr string, maxRecvBytes int, sink Sink) *Server {
	s, _ := newServer(grpcAddr, httpAddr, maxRecvBytes, sink, SecurityConfig{})
	return s
}

type SecurityConfig struct {
	GRPC            edge.ServerConfig
	HTTP            edge.ServerConfig
	TenantMap       map[string]string
	TenantLimits    map[string]TenantLimit
	JournalPath     string
	JournalMaxBytes int64
}

type TenantLimit struct {
	MaxItems      int
	MaxBytes      int64
	MaxConcurrent int
}

type TenantCounters struct{ Accepted, Rejected, QuotaRejected uint64 }

func makeTenantStats(m map[string]string) map[string]TenantCounters {
	out := make(map[string]TenantCounters, len(m))
	for _, tenant := range m {
		if tenant != "" {
			out[tenant] = TenantCounters{}
		}
	}
	return out
}
func (s *Server) recordTenant(tenant string, accepted, rejected, quota bool) {
	s.tenantStatsMu.Lock()
	defer s.tenantStatsMu.Unlock()
	c, ok := s.tenantStats[tenant]
	if !ok {
		return
	}
	if accepted {
		c.Accepted++
	}
	if rejected {
		c.Rejected++
	}
	if quota {
		c.QuotaRejected++
	}
	s.tenantStats[tenant] = c
}

func (s *Server) acquireTenant(ctx context.Context) (func(), bool) {
	tenant, ok := TenantFromContext(ctx)
	if !ok {
		return func() {}, true
	}
	limit := s.tenantLimits[tenant].MaxConcurrent
	if limit <= 0 {
		return func() {}, true
	}
	s.tenantStatsMu.Lock()
	defer s.tenantStatsMu.Unlock()
	if s.tenantInFlight == nil {
		s.tenantInFlight = make(map[string]int)
	}
	if s.tenantStats == nil {
		s.tenantStats = make(map[string]TenantCounters)
	}
	if s.tenantInFlight[tenant] >= limit {
		s.tenantStats[tenant] = TenantCounters{QuotaRejected: s.tenantStats[tenant].QuotaRejected + 1}
		return func() {}, false
	}
	s.tenantInFlight[tenant]++
	return func() { s.tenantStatsMu.Lock(); s.tenantInFlight[tenant]--; s.tenantStatsMu.Unlock() }, true
}
func (s *Server) TenantStats() map[string]TenantCounters {
	s.tenantStatsMu.Lock()
	defer s.tenantStatsMu.Unlock()
	out := make(map[string]TenantCounters, len(s.tenantStats))
	for k, v := range s.tenantStats {
		out[k] = v
	}
	return out
}

// NewSecureServer builds an ingress with optional TLS/mTLS and bearer-token
// authentication independently configurable for gRPC and HTTP.
func NewSecureServer(grpcAddr, httpAddr string, maxRecvBytes int, sink Sink, security SecurityConfig) (*Server, error) {
	return newServer(grpcAddr, httpAddr, maxRecvBytes, sink, security)
}

func newServer(grpcAddr, httpAddr string, maxRecvBytes int, sink Sink, security SecurityConfig) (*Server, error) {
	if maxRecvBytes <= 0 {
		maxRecvBytes = defaultMaxRecvBytes
	}
	security.GRPC.Bind = grpcAddr
	security.HTTP.Bind = httpAddr
	if grpcAddr != "" {
		warnings, err := edge.ValidateServer(security.GRPC)
		if err != nil {
			return nil, fmt.Errorf("grpc: %w", err)
		}
		logEdgeWarnings("grpc", warnings)
	}
	if httpAddr != "" {
		warnings, err := edge.ValidateServer(security.HTTP)
		if err != nil {
			return nil, fmt.Errorf("http: %w", err)
		}
		logEdgeWarnings("http", warnings)
	}
	var admissionJournal *journal.Journal
	var journalErr error
	if security.JournalPath != "" {
		admissionJournal, journalErr = journal.Open(security.JournalPath, security.JournalMaxBytes)
		if journalErr != nil {
			return nil, fmt.Errorf("journal: %w", journalErr)
		}
	}
	return &Server{
		grpcAddr:       grpcAddr,
		httpAddr:       httpAddr,
		maxRecv:        maxRecvBytes,
		sink:           sink,
		tenantMap:      security.TenantMap,
		tenantLimits:   security.TenantLimits,
		tenantStats:    makeTenantStats(security.TenantMap),
		tenantInFlight: make(map[string]int),
		journal:        admissionJournal,
		dedup:          newDedupWindow(100000, 15*time.Minute),
		grpcSecurity:   security.GRPC,
		httpSecurity:   security.HTTP,
		ready:          make(chan struct{}),
	}, nil
}

func logEdgeWarnings(transport string, warnings []edge.Warning) {
	for _, warning := range warnings {
		slog.Warn("reef ingress configuration warning", "transport", transport, "warning", string(warning))
	}
}

func wispUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if _, err := parseWispHeaders(firstMetadata(md, "x-wisp-envelope-id"), firstMetadata(md, "x-wisp-signal-kind")); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return handler(ctx, req)
}

func firstMetadata(md metadata.MD, key string) string {
	v := md.Get(key)
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

func (s *Server) dedupGRPC(ctx context.Context, signal string, payload proto.Message) (dedupResult, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	id := firstMetadata(md, "x-wisp-envelope-id")
	if id == "" {
		return dedupNew, nil
	}
	identity, err := parseWispHeaders(id, signal)
	if err != nil {
		return dedupNew, err
	}
	principal, _ := bearer.PrincipalFromContext(ctx)
	result := s.dedup.lookup(principal, signal, hex.EncodeToString(identity.EnvelopeID[:]), mustMarshal(payload))
	if result == dedupHit {
		s.dedupHits.Add(1)
	}
	if result == dedupConflict {
		s.dedupConflicts.Add(1)
	}
	return result, nil
}

func (s *Server) rememberGRPC(ctx context.Context, signal string, payload proto.Message) {
	md, _ := metadata.FromIncomingContext(ctx)
	id := firstMetadata(md, "x-wisp-envelope-id")
	if id == "" {
		return
	}
	identity, err := parseWispHeaders(id, signal)
	if err != nil {
		return
	}
	principal, _ := bearer.PrincipalFromContext(ctx)
	s.dedup.remember(principal, signal, hex.EncodeToString(identity.EnvelopeID[:]), mustMarshal(payload))
}

func mustMarshal(m proto.Message) []byte { b, _ := proto.Marshal(m); return b }

func validateWispHTTP(w http.ResponseWriter, req *http.Request) bool {
	if _, err := parseWispHeaders(req.Header.Get("x-wisp-envelope-id"), req.Header.Get("x-wisp-signal-kind")); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func (s *Server) dedupHTTP(req *http.Request, signal string, body []byte) (dedupResult, string, error) {
	id := req.Header.Get("x-wisp-envelope-id")
	if id == "" {
		return dedupNew, "", nil
	}
	identity, err := parseWispHeaders(id, signal)
	if err != nil {
		return dedupNew, "", err
	}
	principal, _ := bearer.PrincipalFromContext(req.Context())
	key := hex.EncodeToString(identity.EnvelopeID[:])
	result := s.dedup.lookup(principal, signal, key, body)
	if result == dedupHit {
		s.dedupHits.Add(1)
	}
	if result == dedupConflict {
		s.dedupConflicts.Add(1)
	}
	return result, key, nil
}

func (s *Server) rememberHTTP(req *http.Request, signal, key string, body []byte) {
	if key == "" {
		return
	}
	principal, _ := bearer.PrincipalFromContext(req.Context())
	s.dedup.remember(principal, signal, key, body)
}

func (s *Server) appendAdmission(ctx context.Context, signal string, payload []byte) error {
	if s.journal == nil {
		return nil
	}
	tenant, _ := TenantFromContext(ctx)
	if tenant == "" {
		principal, _ := bearer.PrincipalFromContext(ctx)
		tenant = principal
		if mapped, ok := s.tenantMap[principal]; ok {
			tenant = mapped
		}
	}
	return s.journal.Append(journal.EncodeEnvelope(journal.Envelope{Signal: signal, Tenant: tenant, Payload: payload, CreatedUnixNano: time.Now().UnixNano()}))
}

// ReplayAdmission replays durable admission records. The caller supplies the
// handoff function so replay policy stays explicit and bounded at startup.
func (s *Server) ReplayAdmission(fn func([]byte) error) error {
	if s.journal == nil {
		return nil
	}
	return s.journal.Replay(fn)
}

func (s *Server) ReplayRouted(fn func(journal.Envelope) error) error {
	if s.journal == nil {
		return nil
	}
	return s.journal.Recover(func(payload []byte) error {
		env, err := journal.DecodeEnvelope(payload)
		if err != nil {
			return err
		}
		return fn(env)
	})
}

func (s *Server) CompactJournal() error {
	if s.journal == nil {
		return nil
	}
	return s.journal.Compact()
}

func (s *Server) CompactJournalOlderThan(age time.Duration) error {
	if s.journal == nil {
		return nil
	}
	return s.journal.CompactOlderThan(age)
}

func (s *Server) JournalStats() (bytes, maxBytes int64) {
	if s.journal == nil {
		return 0, 0
	}
	return s.journal.Stats()
}

// Start binds the listeners and begins serving, returning once both are bound
// (or a bind fails). It does not block; call Stop to shut down. Start must run
// after the target pipelines are started, since it feeds them via Sink.
func (s *Server) Start() error {
	defer close(s.ready)

	// Bind every configured listener before starting either server. This makes
	// startup atomic: a failure on the second port cannot leave the first one
	// serving in the background.
	var grpcLn, httpLn net.Listener
	var err error
	if s.grpcAddr != "" {
		grpcLn, err = net.Listen("tcp", s.grpcAddr)
		if err != nil {
			return err
		}
	}
	if s.httpAddr != "" {
		httpLn, err = net.Listen("tcp", s.httpAddr)
		if err != nil {
			if grpcLn != nil {
				_ = grpcLn.Close()
			}
			return err
		}
	}

	if grpcLn != nil {
		managed, err := grpcreef.NewServerEdge(s.grpcSecurity)
		if err != nil {
			_ = grpcLn.Close()
			if httpLn != nil {
				_ = httpLn.Close()
			}
			return fmt.Errorf("grpc security: %w", err)
		}
		s.grpcEdge = managed
		logEdgeWarnings("grpc", managed.Warnings)
	}
	if httpLn != nil {
		managed, err := edge.NewHTTPServer(s.httpSecurity)
		if err != nil {
			if grpcLn != nil {
				_ = grpcLn.Close()
			}
			_ = httpLn.Close()
			_ = s.grpcEdge.Close()
			s.grpcEdge = nil
			return fmt.Errorf("http security: %w", err)
		}
		s.httpEdge = managed
		logEdgeWarnings("http", managed.Warnings)
	}

	if grpcLn != nil {
		serverOpts := append([]grpc.ServerOption{grpc.MaxRecvMsgSize(s.maxRecv), grpc.UnaryInterceptor(wispUnaryInterceptor)}, s.grpcEdge.Options...)
		srv := grpc.NewServer(serverOpts...)
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
		s.grpcLn, s.grpcSrv = grpcLn, srv
		s.mu.Unlock()
		go func() { _ = srv.Serve(grpcLn) }()
	}

	if httpLn != nil {
		if s.httpEdge.TLSConfig != nil {
			httpLn = tls.NewListener(httpLn, s.httpEdge.TLSConfig.Clone())
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
		handlerCtx, cancel := context.WithCancel(context.Background())
		secured := s.httpEdge.Middleware(mux)
		tracked := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.httpHandlerMu.Lock()
			if s.httpStopping {
				s.httpHandlerMu.Unlock()
				http.Error(w, "server shutting down", http.StatusServiceUnavailable)
				return
			}
			s.httpWG.Add(1)
			s.httpHandlerMu.Unlock()
			defer s.httpWG.Done()
			secured.ServeHTTP(w, r)
		})
		s.mu.Lock()
		s.httpLn = httpLn
		s.httpCancel = cancel
		s.httpSrv = &http.Server{
			Handler:           tracked,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
			MaxHeaderBytes:    16 << 10,
			BaseContext:       func(net.Listener) context.Context { return handlerCtx },
		}
		httpSrv := s.httpSrv
		s.mu.Unlock()
		go func() { _ = httpSrv.Serve(httpLn) }()
	}
	return nil
}

// Stop gracefully drains in-flight requests, then closes both transports. After
// Stop returns no handler is mid-flight, so the fed pipelines can be shut down.
func (s *Server) Stop(ctx context.Context) error {
	<-s.ready
	s.mu.Lock()
	grpcSrv, httpSrv, httpCancel := s.grpcSrv, s.httpSrv, s.httpCancel
	s.mu.Unlock()

	var errs []error
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
		s.httpHandlerMu.Lock()
		s.httpStopping = true
		s.httpHandlerMu.Unlock()
		if err := httpSrv.Shutdown(ctx); err != nil {
			// Shutdown does not cancel active handlers when its context expires.
			// Cancel their request contexts and close their connections so no
			// handler can remain blocked in Enqueue while pipeline queues close.
			if httpCancel != nil {
				httpCancel()
			}
			_ = httpSrv.Close()
			s.httpWG.Wait()
			errs = append(errs, err)
		} else {
			if httpCancel != nil {
				httpCancel()
			}
			s.httpWG.Wait()
		}
	}
	if s.httpEdge != nil {
		errs = append(errs, s.httpEdge.Close())
	}
	if s.grpcEdge != nil {
		errs = append(errs, s.grpcEdge.Close())
	}
	if s.journal != nil {
		errs = append(errs, s.journal.Close())
	}
	return errors.Join(errs...)
}

// ReloadCredentials immediately checks all file-backed credentials on both
// transports. Background last-known-good reload remains active.
func (s *Server) ReloadCredentials() error {
	var errs []error
	if s.grpcEdge != nil {
		errs = append(errs, s.grpcEdge.ReloadCredentials())
	}
	if s.httpEdge != nil {
		errs = append(errs, s.httpEdge.ReloadCredentials())
	}
	return errors.Join(errs...)
}

// CredentialStatus returns bounded, secret-free Reef lifecycle snapshots.
func (s *Server) CredentialStatus() []credential.Status {
	var statuses []credential.Status
	if s.grpcEdge != nil {
		statuses = append(statuses, s.grpcEdge.CredentialStatus()...)
	}
	if s.httpEdge != nil {
		statuses = append(statuses, s.httpEdge.CredentialStatus()...)
	}
	return statuses
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

func (s *Server) DedupStats() (hits, conflicts uint64) {
	return s.dedupHits.Load(), s.dedupConflicts.Load()
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
	var ok bool
	ctx, ok = tenantContextWithPolicy(ctx, s.tenantMap)
	if !ok {
		return 0, "", errors.New("tenant principal is not authorized")
	}
	release, allowed := s.acquireTenant(ctx)
	if !allowed {
		return 0, "", errors.New("tenant concurrency quota exceeded")
	}
	defer release()
	b := model.Batch{Spans: spans}
	if quotaExceeded(ctx, s.tenantLimits, b.Len(), int64(b.SizeBytes())) {
		if tenant, ok := TenantFromContext(ctx); ok {
			s.recordTenant(tenant, false, false, true)
		}
		return 0, "", errors.New("tenant quota exceeded")
	}
	if s.sink.TraceAdmit != nil {
		b, rejected, reason = s.sink.TraceAdmit(b)
	}
	if b.Len() > 0 {
		if err = s.sink.Traces(ctx, b); err != nil {
			return 0, "", err
		}
		s.tracesAccepted.Add(uint64(b.Len()))
		if tenant, ok := TenantFromContext(ctx); ok {
			s.recordTenant(tenant, true, rejected > 0, false)
		}
	}
	if rejected > 0 {
		s.tracesRejected.Add(uint64(rejected))
	}
	return rejected, reason, nil
}

func (s *Server) admitMetrics(ctx context.Context, rm []*metricspb.ResourceMetrics) (rejected int, reason string, err error) {
	var ok bool
	ctx, ok = tenantContextWithPolicy(ctx, s.tenantMap)
	if !ok {
		return 0, "", errors.New("tenant principal is not authorized")
	}
	release, allowed := s.acquireTenant(ctx)
	if !allowed {
		return 0, "", errors.New("tenant concurrency quota exceeded")
	}
	defer release()
	b := metric.Batch{ResourceMetrics: rm}
	if quotaExceeded(ctx, s.tenantLimits, b.Len(), int64(b.SizeBytes())) {
		if tenant, ok := TenantFromContext(ctx); ok {
			s.recordTenant(tenant, false, false, true)
		}
		return 0, "", errors.New("tenant quota exceeded")
	}
	if s.sink.MetricAdmit != nil {
		b, rejected, reason = s.sink.MetricAdmit(b)
	}
	if b.Len() > 0 {
		if err = s.sink.Metrics(ctx, b); err != nil {
			return 0, "", err
		}
		s.pointsAccepted.Add(uint64(b.Len()))
		if tenant, ok := TenantFromContext(ctx); ok {
			s.recordTenant(tenant, true, rejected > 0, false)
		}
	}
	if rejected > 0 {
		s.pointsRejected.Add(uint64(rejected))
	}
	return rejected, reason, nil
}

func (s *Server) admitLogs(ctx context.Context, rl []*logspb.ResourceLogs) (rejected int, reason string, err error) {
	var ok bool
	ctx, ok = tenantContextWithPolicy(ctx, s.tenantMap)
	if !ok {
		return 0, "", errors.New("tenant principal is not authorized")
	}
	release, allowed := s.acquireTenant(ctx)
	if !allowed {
		return 0, "", errors.New("tenant concurrency quota exceeded")
	}
	defer release()
	b := logs.Batch{ResourceLogs: rl}
	if quotaExceeded(ctx, s.tenantLimits, b.Len(), int64(b.SizeBytes())) {
		if tenant, ok := TenantFromContext(ctx); ok {
			s.recordTenant(tenant, false, false, true)
		}
		return 0, "", errors.New("tenant quota exceeded")
	}
	if s.sink.LogAdmit != nil {
		b, rejected, reason = s.sink.LogAdmit(b)
	}
	if b.Len() > 0 {
		if err = s.sink.Logs(ctx, b); err != nil {
			return 0, "", err
		}
		s.logsAccepted.Add(uint64(b.Len()))
		if tenant, ok := TenantFromContext(ctx); ok {
			s.recordTenant(tenant, true, rejected > 0, false)
		}
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
	if result, err := g.s.dedupGRPC(ctx, "traces", req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if result == dedupHit {
		return &coltracepb.ExportTraceServiceResponse{}, nil
	} else if result == dedupConflict {
		return nil, status.Error(codes.InvalidArgument, "wisp envelope id payload conflict")
	}
	spans := spansFromResourceSpans(req.GetResourceSpans())
	rejected, reason, err := g.s.admitTraces(ctx, spans)
	if err != nil {
		g.s.errs.Add(1)
		return nil, status.Error(codes.Unavailable, "pipeline unavailable")
	}
	if err := g.s.appendAdmission(ctx, "traces", mustMarshal(req)); err != nil {
		return nil, status.Error(codes.Unavailable, "admission journal unavailable")
	}
	g.s.rememberGRPC(ctx, "traces", req)
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
	if result, err := g.s.dedupGRPC(ctx, "metrics", req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if result == dedupHit {
		return &colmetricspb.ExportMetricsServiceResponse{}, nil
	} else if result == dedupConflict {
		return nil, status.Error(codes.InvalidArgument, "wisp envelope id payload conflict")
	}
	rejected, reason, err := g.s.admitMetrics(ctx, req.GetResourceMetrics())
	if err != nil {
		g.s.errs.Add(1)
		return nil, status.Error(codes.Unavailable, "pipeline unavailable")
	}
	if err := g.s.appendAdmission(ctx, "metrics", mustMarshal(req)); err != nil {
		return nil, status.Error(codes.Unavailable, "admission journal unavailable")
	}
	g.s.rememberGRPC(ctx, "metrics", req)
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
	if result, err := g.s.dedupGRPC(ctx, "logs", req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if result == dedupHit {
		return &collogspb.ExportLogsServiceResponse{}, nil
	} else if result == dedupConflict {
		return nil, status.Error(codes.InvalidArgument, "wisp envelope id payload conflict")
	}
	rejected, reason, err := g.s.admitLogs(ctx, req.GetResourceLogs())
	if err != nil {
		g.s.errs.Add(1)
		return nil, status.Error(codes.Unavailable, "pipeline unavailable")
	}
	if err := g.s.appendAdmission(ctx, "logs", mustMarshal(req)); err != nil {
		return nil, status.Error(codes.Unavailable, "admission journal unavailable")
	}
	g.s.rememberGRPC(ctx, "logs", req)
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
	if !validateWispHTTP(w, req) {
		s.errs.Add(1)
		return
	}
	body, enc, ok := otlphttp.ReadBody(w, req, int64(s.maxRecv))
	if !ok {
		s.errs.Add(1)
		return
	}
	dedup, dedupKey, dedupErr := s.dedupHTTP(req, "traces", body)
	if dedupErr != nil {
		http.Error(w, dedupErr.Error(), http.StatusBadRequest)
		return
	}
	if dedup == dedupConflict {
		http.Error(w, "wisp envelope id payload conflict", http.StatusBadRequest)
		return
	}
	if dedup == dedupHit {
		writeResponse(w, enc, &coltracepb.ExportTraceServiceResponse{})
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
	if err := s.appendAdmission(req.Context(), "traces", body); err != nil {
		http.Error(w, "admission journal unavailable", http.StatusServiceUnavailable)
		return
	}
	s.rememberHTTP(req, "traces", dedupKey, body)
	resp := &coltracepb.ExportTraceServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &coltracepb.ExportTracePartialSuccess{
			RejectedSpans: int64(rejected), ErrorMessage: reason,
		}
	}
	writeResponse(w, enc, resp)
}

func (s *Server) handleMetrics(w http.ResponseWriter, req *http.Request) {
	s.requests.Add(1)
	if !validateWispHTTP(w, req) {
		s.errs.Add(1)
		return
	}
	body, enc, ok := otlphttp.ReadBody(w, req, int64(s.maxRecv))
	if !ok {
		s.errs.Add(1)
		return
	}
	dedup, dedupKey, dedupErr := s.dedupHTTP(req, "metrics", body)
	if dedupErr != nil {
		http.Error(w, dedupErr.Error(), http.StatusBadRequest)
		return
	}
	if dedup == dedupConflict {
		http.Error(w, "wisp envelope id payload conflict", http.StatusBadRequest)
		return
	}
	if dedup == dedupHit {
		writeResponse(w, enc, &colmetricspb.ExportMetricsServiceResponse{})
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
	if err := s.appendAdmission(req.Context(), "metrics", body); err != nil {
		http.Error(w, "admission journal unavailable", http.StatusServiceUnavailable)
		return
	}
	s.rememberHTTP(req, "metrics", dedupKey, body)
	resp := &colmetricspb.ExportMetricsServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &colmetricspb.ExportMetricsPartialSuccess{
			RejectedDataPoints: int64(rejected), ErrorMessage: reason,
		}
	}
	writeResponse(w, enc, resp)
}

func (s *Server) handleLogs(w http.ResponseWriter, req *http.Request) {
	s.requests.Add(1)
	if !validateWispHTTP(w, req) {
		s.errs.Add(1)
		return
	}
	body, enc, ok := otlphttp.ReadBody(w, req, int64(s.maxRecv))
	if !ok {
		s.errs.Add(1)
		return
	}
	dedup, dedupKey, dedupErr := s.dedupHTTP(req, "logs", body)
	if dedupErr != nil {
		http.Error(w, dedupErr.Error(), http.StatusBadRequest)
		return
	}
	if dedup == dedupConflict {
		http.Error(w, "wisp envelope id payload conflict", http.StatusBadRequest)
		return
	}
	if dedup == dedupHit {
		writeResponse(w, enc, &collogspb.ExportLogsServiceResponse{})
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
	if err := s.appendAdmission(req.Context(), "logs", body); err != nil {
		http.Error(w, "admission journal unavailable", http.StatusServiceUnavailable)
		return
	}
	s.rememberHTTP(req, "logs", dedupKey, body)
	resp := &collogspb.ExportLogsServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &collogspb.ExportLogsPartialSuccess{
			RejectedLogRecords: int64(rejected), ErrorMessage: reason,
		}
	}
	writeResponse(w, enc, resp)
}

func writeResponse(w http.ResponseWriter, enc otlphttp.Encoding, m proto.Message) {
	var resp []byte
	if enc == otlphttp.EncodingJSON {
		resp, _ = protojson.Marshal(m)
		w.Header().Set("Content-Type", "application/json")
	} else {
		resp, _ = proto.Marshal(m)
		w.Header().Set("Content-Type", "application/x-protobuf")
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(resp)
}
