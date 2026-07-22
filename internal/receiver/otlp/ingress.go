package otlp

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/yaop-labs/coral/internal/delivery"
	amberexp "github.com/yaop-labs/coral/internal/exporter/amber"
	"github.com/yaop-labs/coral/internal/exporter/backoff"
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

var (
	errTenantQuota             = errors.New("tenant quota exceeded")
	errTenantConcurrency       = errors.New("tenant concurrency quota exceeded")
	errTenantRate              = errors.New("tenant request rate quota exceeded")
	errLogRecordTooLarge       = errors.New("log record exceeds tenant limit")
	errMetricAttributesTooMany = errors.New("metric attributes exceed tenant limit")
)

func admissionOverload(err error) bool {
	return errors.Is(err, errTenantQuota) || errors.Is(err, errTenantConcurrency) || errors.Is(err, errTenantRate)
}

func admissionPermanent(err error) bool {
	return errors.Is(err, errLogRecordTooLarge) || errors.Is(err, errMetricAttributesTooMany)
}

func metricAttributeCount(m proto.Message, limit int) int {
	count := 0
	var walk func(protoreflect.Message)
	walk = func(msg protoreflect.Message) {
		msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
				if fd.IsList() {
					list := v.List()
					for i := 0; i < list.Len(); i++ {
						if fd.Name() == "attributes" {
							count++
						}
						walk(list.Get(i).Message())
					}
				} else if fd.IsMap() {
					return true
				} else {
					walk(v.Message())
				}
			}
			return count <= limit
		})
	}
	walk(m.ProtoReflect())
	return count
}

func metricAttributeKeyCount(m proto.Message, limit int) int {
	keys := make(map[string]struct{})
	var walk func(protoreflect.Message)
	walk = func(msg protoreflect.Message) {
		msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
				return true
			}
			if fd.IsList() {
				list := v.List()
				for i := 0; i < list.Len(); i++ {
					item := list.Get(i).Message()
					if fd.Name() == "attributes" {
						item.Range(func(kfd protoreflect.FieldDescriptor, kv protoreflect.Value) bool {
							if kfd.Name() == "key" {
								keys[kv.String()] = struct{}{}
							}
							return true
						})
					}
					walk(item)
				}
			} else if !fd.IsMap() {
				walk(v.Message())
			}
			return len(keys) <= limit
		})
	}
	walk(m.ProtoReflect())
	return len(keys)
}

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
	grpcAddr        string
	httpAddr        string
	maxRecv         int
	sink            Sink
	tenantMap       map[string]string
	tenantLimits    map[string]TenantLimit
	tenantStatsMu   sync.Mutex
	tenantStats     map[string]TenantCounters
	tenantInFlight  map[string]int
	tenantRate      map[string][]time.Time
	dedup           *dedupWindow
	journal         *journal.Journal
	quarantine      *journal.Journal
	receipts        *journal.Journal
	deliveryMu      sync.Mutex
	journalPending  map[string]pendingDelivery
	journalReady    map[string]struct{}
	journalReplay   bool
	deliveryAttempt atomic.Uint64
	journalRetry    map[string]retryDelivery
	retryInitial    time.Duration
	retryMax        time.Duration
	retryWake       chan struct{}
	retryCancel     context.CancelFunc
	retryWG         sync.WaitGroup
	journalAckMu    sync.Mutex
	journalAckErrs  atomic.Uint64
	journalClose    sync.Once
	journalCloseErr error
	grpcSecurity    edge.ServerConfig
	httpSecurity    edge.ServerConfig

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

	requests            atomic.Uint64
	errs                atomic.Uint64
	tracesAccepted      atomic.Uint64
	pointsAccepted      atomic.Uint64
	logsAccepted        atomic.Uint64
	tracesRejected      atomic.Uint64
	pointsRejected      atomic.Uint64
	logsRejected        atomic.Uint64
	logLimitRejected    atomic.Uint64
	metricLimitRejected atomic.Uint64
	dedupHits           atomic.Uint64
	dedupConflicts      atomic.Uint64
	dedupMisses         atomic.Uint64
	redispatchAttempts  atomic.Uint64
	redispatchSuccesses atomic.Uint64
	redispatchFailures  atomic.Uint64
	quarantinedTotal    atomic.Uint64
}

type pendingDelivery struct {
	attempt   uint64
	remaining int
}

type retryDelivery struct {
	attempts  uint64
	due       time.Time
	inFlight  bool
	permanent bool
	reason    string
}

type deliveryIDContextKey struct{}

func withDeliveryID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, deliveryIDContextKey{}, id)
}

func deliveryIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(deliveryIDContextKey{}).(string); ok {
		return id
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	return firstMetadata(md, "x-wisp-envelope-id")
}

func normalizedDeliveryID(ctx context.Context, signal string) string {
	id := deliveryIDFromContext(ctx)
	if id == "" {
		return ""
	}
	identity, err := parseWispHeaders(id, signal)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(identity.EnvelopeID[:])
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
	MaxItems               int
	MaxBytes               int64
	MaxConcurrent          int
	MaxRequestsPerSecond   int
	MaxLogRecordBytes      int
	MaxLogAttributes       int
	MaxLogAttributeKeys    int
	MaxMetricAttributes    int
	MaxMetricAttributeKeys int
	MaxMetricSeries        int
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

func (s *Server) acquireTenant(ctx context.Context) (func(), error) {
	tenant, ok := TenantFromContext(ctx)
	if !ok {
		return func() {}, nil
	}
	limit := s.tenantLimits[tenant].MaxConcurrent
	rate := s.tenantLimits[tenant].MaxRequestsPerSecond
	if limit <= 0 && rate <= 0 {
		return func() {}, nil
	}
	s.tenantStatsMu.Lock()
	defer s.tenantStatsMu.Unlock()
	if s.tenantInFlight == nil {
		s.tenantInFlight = make(map[string]int)
	}
	if s.tenantRate == nil {
		s.tenantRate = make(map[string][]time.Time)
	}
	if s.tenantStats == nil {
		s.tenantStats = make(map[string]TenantCounters)
	}
	if limit > 0 && s.tenantInFlight[tenant] >= limit {
		c := s.tenantStats[tenant]
		c.QuotaRejected++
		s.tenantStats[tenant] = c
		return func() {}, errTenantConcurrency
	}
	if rate > 0 {
		now := time.Now()
		cutoff := now.Add(-time.Second)
		timestamps := s.tenantRate[tenant]
		first := 0
		for first < len(timestamps) && !timestamps[first].After(cutoff) {
			first++
		}
		timestamps = timestamps[first:]
		if len(timestamps) >= rate {
			c := s.tenantStats[tenant]
			c.QuotaRejected++
			s.tenantStats[tenant] = c
			return func() {}, errTenantRate
		}
		s.tenantRate[tenant] = append(timestamps, now)
	}
	if limit > 0 {
		s.tenantInFlight[tenant]++
		return func() { s.tenantStatsMu.Lock(); s.tenantInFlight[tenant]--; s.tenantStatsMu.Unlock() }, nil
	}
	return func() {}, nil
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
	var admissionJournal, quarantineJournal, receiptJournal *journal.Journal
	var journalErr error
	if security.JournalPath != "" {
		admissionJournal, journalErr = journal.Open(security.JournalPath, security.JournalMaxBytes)
		if journalErr != nil {
			return nil, fmt.Errorf("journal: %w", journalErr)
		}
		quarantineJournal, journalErr = journal.Open(security.JournalPath+".quarantine", security.JournalMaxBytes)
		if journalErr != nil {
			_ = admissionJournal.Close()
			return nil, fmt.Errorf("quarantine journal: %w", journalErr)
		}
		receiptJournal, journalErr = journal.Open(security.JournalPath+".receipts", security.JournalMaxBytes)
		if journalErr != nil {
			_ = quarantineJournal.Close()
			_ = admissionJournal.Close()
			return nil, fmt.Errorf("receipt journal: %w", journalErr)
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
		tenantRate:     make(map[string][]time.Time),
		journal:        admissionJournal,
		quarantine:     quarantineJournal,
		receipts:       receiptJournal,
		dedup:          newDedupWindow(100000, 15*time.Minute),
		journalPending: make(map[string]pendingDelivery),
		journalReady:   make(map[string]struct{}),
		journalRetry:   make(map[string]retryDelivery),
		retryInitial:   200 * time.Millisecond,
		retryMax:       5 * time.Second,
		retryWake:      make(chan struct{}, 1),
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

func (s *Server) dedupTenant(ctx context.Context) string {
	principal, _ := bearer.PrincipalFromContext(ctx)
	if mapped, ok := s.tenantMap[principal]; ok && mapped != "" {
		return mapped
	}
	return principal
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
	tenant := s.dedupTenant(ctx)
	result := s.dedup.lookup(tenant, signal, hex.EncodeToString(identity.EnvelopeID[:]), mustMarshal(payload))
	if result == dedupHit {
		s.dedupHits.Add(1)
	}
	if result == dedupConflict {
		s.dedupConflicts.Add(1)
	}
	if result == dedupNew {
		s.dedupMisses.Add(1)
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
	s.dedup.remember(s.dedupTenant(ctx), signal, hex.EncodeToString(identity.EnvelopeID[:]), mustMarshal(payload))
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
	key := hex.EncodeToString(identity.EnvelopeID[:])
	result := s.dedup.lookup(s.dedupTenant(req.Context()), signal, key, body)
	if result == dedupHit {
		s.dedupHits.Add(1)
	}
	if result == dedupConflict {
		s.dedupConflicts.Add(1)
	}
	if result == dedupNew {
		s.dedupMisses.Add(1)
	}
	return result, key, nil
}

func (s *Server) rememberHTTP(req *http.Request, signal, key string, body []byte) {
	if key == "" {
		return
	}
	s.dedup.remember(s.dedupTenant(req.Context()), signal, key, body)
}

func (s *Server) appendAdmission(ctx context.Context, signal string, payload, requestPayload []byte, units int) (journal.Envelope, uint64, error) {
	if s.journal == nil {
		return journal.Envelope{}, 0, nil
	}
	tenant, _ := TenantFromContext(ctx)
	if tenant == "" {
		principal, _ := bearer.PrincipalFromContext(ctx)
		tenant = principal
		if mapped, ok := s.tenantMap[principal]; ok {
			tenant = mapped
		}
	}
	if len(requestPayload) == 0 {
		requestPayload = payload
	}
	env, err := s.journal.AppendEnvelope(journal.Envelope{
		Signal: signal, Tenant: tenant, DeliveryID: normalizedDeliveryID(ctx, signal),
		RequestDigest: digestHex(requestPayload), Payload: payload, CreatedUnixNano: time.Now().UnixNano(),
	})
	if err != nil {
		return journal.Envelope{}, 0, err
	}
	attempt := s.TrackJournalRecord(env.RecordID, units)
	return env, attempt, nil
}

// TrackJournalRecord registers the number of terminal signal items expected
// before recordID can be acknowledged. It is exported for startup replay.
func (s *Server) TrackJournalRecord(recordID string, units int) uint64 {
	if recordID == "" || units <= 0 {
		return 0
	}
	attempt := s.deliveryAttempt.Add(1)
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	s.journalPending[recordID] = pendingDelivery{attempt: attempt, remaining: units}
	return attempt
}

// DeliveryConfirmed consumes item-level completion only after every required
// destination accepted the exporter batch. Stateful processors may split one
// journal record across several callbacks; the record is acknowledged only
// when all registered units are terminal.
func (s *Server) DeliveryConfirmed(meta delivery.Metadata) {
	if s.journal == nil || len(meta.Records) == 0 {
		return
	}
	s.deliveryMu.Lock()
	for _, contribution := range meta.Records {
		pending, exists := s.journalPending[contribution.RecordID]
		if !exists || contribution.Units <= 0 || (contribution.Attempt != 0 && contribution.Attempt != pending.attempt) {
			continue
		}
		pending.remaining -= contribution.Units
		if pending.remaining > 0 {
			s.journalPending[contribution.RecordID] = pending
			continue
		}
		delete(s.journalPending, contribution.RecordID)
		delete(s.journalRetry, contribution.RecordID)
		s.journalReady[contribution.RecordID] = struct{}{}
	}
	replaying := s.journalReplay
	s.deliveryMu.Unlock()
	if !replaying {
		s.flushJournalAcknowledgements()
	}
}

// DeliveryFailed retains required-destination failures for bounded live
// redispatch. Permanent protocol failures are moved to durable quarantine by
// the same single worker, keeping exporter goroutines free of disk rewrites.
func (s *Server) DeliveryFailed(meta delivery.Metadata, cause error) {
	if s.journal == nil || cause == nil {
		return
	}
	permanent := backoff.IsPermanent(cause)
	reason := durableFailureReason(cause)
	now := time.Now()
	s.deliveryMu.Lock()
	for _, contribution := range meta.Records {
		pending, exists := s.journalPending[contribution.RecordID]
		if !exists || (contribution.Attempt != 0 && contribution.Attempt != pending.attempt) {
			continue
		}
		entry := s.journalRetry[contribution.RecordID]
		entry.inFlight = false
		entry.permanent = permanent
		entry.reason = reason
		if permanent {
			entry.due = now
		} else {
			entry.attempts++
			entry.due = now.Add(s.retryDelay(entry.attempts))
		}
		s.journalRetry[contribution.RecordID] = entry
	}
	s.deliveryMu.Unlock()
	s.wakeRetryWorker()
}

func durableFailureReason(err error) string {
	if err == nil {
		return "unknown required delivery failure"
	}
	reason := strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' {
			return ' '
		}
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, err.Error())
	if len(reason) > 1024 {
		reason = reason[:1024]
	}
	return reason
}

func (s *Server) retryDelay(attempt uint64) time.Duration {
	delay := s.retryInitial
	if delay <= 0 {
		delay = 200 * time.Millisecond
	}
	maxDelay := s.retryMax
	if maxDelay < delay {
		maxDelay = delay
	}
	for i := uint64(1); i < attempt && delay < maxDelay; i++ {
		if delay > maxDelay/2 {
			delay = maxDelay
			break
		}
		delay *= 2
	}
	if delay <= 1 {
		return delay
	}
	half := int64(delay) / 2
	return time.Duration(int64(delay) - half + rand.Int64N(half+1))
}

func (s *Server) wakeRetryWorker() {
	select {
	case s.retryWake <- struct{}{}:
	default:
	}
}

func (s *Server) beginJournalReplay() {
	s.deliveryMu.Lock()
	s.journalReplay = true
	s.deliveryMu.Unlock()
}

func (s *Server) endJournalReplay() {
	s.deliveryMu.Lock()
	s.journalReplay = false
	s.deliveryMu.Unlock()
	s.flushJournalAcknowledgements()
}

func (s *Server) flushJournalAcknowledgements() {
	if s.journal == nil {
		return
	}
	s.journalAckMu.Lock()
	defer s.journalAckMu.Unlock()
	for {
		s.deliveryMu.Lock()
		if s.journalReplay || len(s.journalReady) == 0 {
			s.deliveryMu.Unlock()
			return
		}
		var recordID string
		for id := range s.journalReady {
			recordID = id
			break
		}
		s.deliveryMu.Unlock()

		if err := s.acknowledgeJournalRecord(recordID); err != nil {
			s.journalAckErrs.Add(1)
			return
		}
		s.deliveryMu.Lock()
		delete(s.journalReady, recordID)
		s.deliveryMu.Unlock()
	}
}

func (s *Server) acknowledgeJournalRecord(recordID string) error {
	env, found, err := s.journal.LookupEnvelope(recordID)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if env.DeliveryID != "" && s.receipts != nil {
		digest := env.RequestDigest
		if digest == "" {
			digest = digestHex(env.Payload)
		}
		receipt := journal.Envelope{
			Signal: env.Signal, Tenant: env.Tenant, DeliveryID: env.DeliveryID,
			RecordID: env.RecordID, RequestDigest: digest,
			CreatedUnixNano: time.Now().UnixNano(),
		}
		existing, exists, lookupErr := s.receipts.LookupEnvelope(recordID)
		if lookupErr != nil {
			return lookupErr
		}
		if exists {
			if existing.DeliveryID != receipt.DeliveryID || existing.RequestDigest != receipt.RequestDigest || existing.Signal != receipt.Signal || existing.Tenant != receipt.Tenant {
				return errors.New("delivery receipt identity conflict")
			}
		} else if _, appendErr := s.receipts.AppendEnvelope(receipt); appendErr != nil {
			if errors.Is(appendErr, journal.ErrFull) {
				if compactErr := s.receipts.CompactOlderThan(s.dedup.ttl); compactErr != nil {
					return errors.Join(appendErr, compactErr)
				}
				_, appendErr = s.receipts.AppendEnvelope(receipt)
			}
			if appendErr != nil {
				return appendErr
			}
		}
		if err := s.dedup.rememberDigest(receipt.Tenant, receipt.Signal, receipt.DeliveryID, receipt.RequestDigest); err != nil {
			return err
		}
	}
	_, err = s.journal.Acknowledge(recordID)
	return err
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
	// Recover the interrupted tail before migrating legacy envelopes. Migration
	// requires a complete record stream and must never turn a recoverable crash
	// tail into a startup failure.
	if err := s.journal.Recover(func([]byte) error { return nil }); err != nil {
		return err
	}
	if err := s.journal.EnsureRecordIDs(); err != nil {
		return err
	}
	quarantined := make(map[string]struct{})
	if s.quarantine != nil {
		if err := s.quarantine.Recover(func(raw []byte) error {
			env, err := journal.DecodeEnvelope(raw)
			if err == nil && env.RecordID != "" {
				quarantined[env.RecordID] = struct{}{}
				slog.Error("journal quarantine recovered",
					"record_id", env.RecordID,
					"signal", env.Signal,
					"reason", env.FailureReason,
				)
			}
			return err
		}); err != nil {
			return fmt.Errorf("quarantine recovery: %w", err)
		}
	}
	receipted := make(map[string]journal.Envelope)
	if s.receipts != nil {
		if err := s.receipts.Recover(func([]byte) error { return nil }); err != nil {
			return fmt.Errorf("receipt recovery: %w", err)
		}
		if err := s.receipts.CompactOlderThan(s.dedup.ttl); err != nil {
			return fmt.Errorf("receipt compaction: %w", err)
		}
		if err := s.receipts.Replay(func(raw []byte) error {
			env, err := journal.DecodeEnvelope(raw)
			if err != nil {
				return err
			}
			receipted[env.RecordID] = env
			return s.dedup.rememberDigest(env.Tenant, env.Signal, env.DeliveryID, env.RequestDigest)
		}); err != nil {
			return fmt.Errorf("receipt replay: %w", err)
		}
	}
	if len(quarantined) > 0 || len(receipted) > 0 {
		var remove []string
		if err := s.journal.Replay(func(raw []byte) error {
			env, err := journal.DecodeEnvelope(raw)
			if err != nil {
				return err
			}
			if _, exists := quarantined[env.RecordID]; exists {
				remove = append(remove, env.RecordID)
				return nil
			}
			if receipt, exists := receipted[env.RecordID]; exists {
				digest := env.RequestDigest
				if digest == "" {
					digest = digestHex(env.Payload)
				}
				if receipt.Signal != env.Signal || receipt.Tenant != env.Tenant ||
					receipt.DeliveryID != env.DeliveryID || receipt.RequestDigest != digest {
					return errors.New("delivery receipt identity conflict during recovery")
				}
				remove = append(remove, env.RecordID)
			}
			return nil
		}); err != nil {
			return err
		}
		for _, recordID := range remove {
			if _, err := s.journal.Acknowledge(recordID); err != nil {
				return fmt.Errorf("reconcile quarantined record: %w", err)
			}
		}
	}
	s.beginJournalReplay()
	defer s.endJournalReplay()
	return s.journal.Recover(func(payload []byte) error {
		env, err := journal.DecodeEnvelope(payload)
		if err != nil {
			return err
		}
		if env.DeliveryID != "" {
			if env.RequestDigest != "" {
				if err := s.dedup.rememberDigest(env.Tenant, env.Signal, env.DeliveryID, env.RequestDigest); err != nil {
					return err
				}
			} else {
				s.dedup.remember(env.Tenant, env.Signal, env.DeliveryID, env.Payload)
			}
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

// DurabilitySnapshot is a bounded, secret-free view of the active journal,
// retry state, receipts, and quarantine.
type DurabilitySnapshot struct {
	Enabled             bool
	Healthy             bool
	Reason              string
	ActiveBytes         int64
	ActiveMaxBytes      int64
	ActiveRecords       uint64
	ActiveOldestAge     time.Duration
	ReceiptBytes        int64
	ReceiptRecords      uint64
	QuarantineBytes     int64
	QuarantineRecords   uint64
	PendingAttempts     int
	RetryScheduled      int
	AckReady            int
	AckErrors           uint64
	RedispatchAttempts  uint64
	RedispatchSuccesses uint64
	RedispatchFailures  uint64
	QuarantinedTotal    uint64
}

func (s *Server) DurabilityStats() DurabilitySnapshot {
	if s.journal == nil {
		return DurabilitySnapshot{Healthy: true}
	}
	snapshot := DurabilitySnapshot{Enabled: true, Healthy: true}
	snapshot.ActiveBytes, snapshot.ActiveMaxBytes = s.journal.Stats()
	var oldest time.Time
	var err error
	snapshot.ActiveRecords, oldest, err = s.journal.RecordStats()
	if err != nil {
		snapshot.Healthy = false
		snapshot.Reason = "journal_stats_failed"
	}
	if !oldest.IsZero() {
		snapshot.ActiveOldestAge = time.Since(oldest)
		if snapshot.ActiveOldestAge < 0 {
			snapshot.ActiveOldestAge = 0
		}
	}
	if s.receipts != nil {
		snapshot.ReceiptBytes, _ = s.receipts.Stats()
		snapshot.ReceiptRecords, _, err = s.receipts.RecordStats()
		if err != nil && snapshot.Healthy {
			snapshot.Healthy = false
			snapshot.Reason = "receipt_stats_failed"
		}
	}
	if s.quarantine != nil {
		snapshot.QuarantineBytes, _ = s.quarantine.Stats()
		snapshot.QuarantineRecords, _, err = s.quarantine.RecordStats()
		if err != nil && snapshot.Healthy {
			snapshot.Healthy = false
			snapshot.Reason = "quarantine_stats_failed"
		}
	}
	s.deliveryMu.Lock()
	snapshot.PendingAttempts = len(s.journalPending)
	snapshot.RetryScheduled = len(s.journalRetry)
	snapshot.AckReady = len(s.journalReady)
	s.deliveryMu.Unlock()
	snapshot.AckErrors = s.journalAckErrs.Load()
	snapshot.RedispatchAttempts = s.redispatchAttempts.Load()
	snapshot.RedispatchSuccesses = s.redispatchSuccesses.Load()
	snapshot.RedispatchFailures = s.redispatchFailures.Load()
	snapshot.QuarantinedTotal = s.quarantinedTotal.Load()
	if snapshot.Healthy && snapshot.ActiveMaxBytes > 0 && snapshot.ActiveBytes*10 >= snapshot.ActiveMaxBytes*9 {
		snapshot.Healthy = false
		snapshot.Reason = "journal_pressure"
	}
	if snapshot.Healthy && snapshot.QuarantineRecords > 0 {
		snapshot.Healthy = false
		snapshot.Reason = "quarantine_not_empty"
	}
	if snapshot.Healthy && snapshot.RetryScheduled > 0 {
		snapshot.Healthy = false
		snapshot.Reason = "required_delivery_retry"
	}
	if snapshot.Healthy && snapshot.AckReady > 0 {
		snapshot.Healthy = false
		snapshot.Reason = "journal_ack_pending"
	}
	return snapshot
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
	s.startRetryWorker()
	return nil
}

func (s *Server) startRetryWorker() {
	if s.journal == nil || s.retryCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.retryCancel = cancel
	s.retryWG.Add(1)
	go func() {
		defer s.retryWG.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-s.retryWake:
			}
			s.processDueRetries(ctx)
		}
	}()
	s.wakeRetryWorker()
}

func (s *Server) processDueRetries(ctx context.Context) {
	s.flushJournalAcknowledgements()
	now := time.Now()
	type dueRecord struct {
		id    string
		entry retryDelivery
	}
	var due []dueRecord
	s.deliveryMu.Lock()
	for id, entry := range s.journalRetry {
		if entry.inFlight || entry.due.After(now) {
			continue
		}
		entry.inFlight = true
		s.journalRetry[id] = entry
		due = append(due, dueRecord{id: id, entry: entry})
	}
	s.deliveryMu.Unlock()

	for _, item := range due {
		if item.entry.permanent {
			if err := s.quarantineRecord(item.id, item.entry.reason); err != nil {
				s.redispatchFailures.Add(1)
				s.rescheduleRecord(item.id, true, item.entry.reason)
			}
			continue
		}
		s.redispatchAttempts.Add(1)
		env, found, err := s.journal.LookupEnvelope(item.id)
		if err != nil || !found {
			if err != nil {
				s.redispatchFailures.Add(1)
				s.rescheduleRecord(item.id, false, durableFailureReason(err))
			} else {
				s.clearDeliveryState(item.id)
			}
			continue
		}
		dispatchCtx, cancel := context.WithTimeout(ctx, time.Second)
		err = ReplayEnvelope(dispatchCtx, env, ReplaySinks{
			Track:   s.TrackJournalRecord,
			Traces:  s.sink.Traces,
			Metrics: s.sink.Metrics,
			Logs:    s.sink.Logs,
		})
		cancel()
		if err != nil {
			s.redispatchFailures.Add(1)
			s.rescheduleRecord(item.id, false, durableFailureReason(err))
			continue
		}
		s.redispatchSuccesses.Add(1)
	}
}

func (s *Server) rescheduleRecord(recordID string, permanent bool, reason string) {
	s.deliveryMu.Lock()
	entry, exists := s.journalRetry[recordID]
	if exists {
		entry.inFlight = false
		entry.permanent = permanent
		entry.reason = reason
		entry.attempts++
		entry.due = time.Now().Add(s.retryDelay(entry.attempts))
		s.journalRetry[recordID] = entry
	}
	s.deliveryMu.Unlock()
}

func (s *Server) clearDeliveryState(recordID string) {
	s.deliveryMu.Lock()
	delete(s.journalPending, recordID)
	delete(s.journalRetry, recordID)
	delete(s.journalReady, recordID)
	s.deliveryMu.Unlock()
}

func (s *Server) quarantineRecord(recordID, reason string) error {
	if s.quarantine == nil {
		return errors.New("quarantine journal unavailable")
	}
	env, found, err := s.journal.LookupEnvelope(recordID)
	if err != nil || !found {
		if !found && err == nil {
			s.clearDeliveryState(recordID)
		}
		return err
	}
	existing, exists, err := s.quarantine.LookupEnvelope(recordID)
	if err != nil {
		return err
	}
	if exists {
		if existing.Signal != env.Signal || existing.Tenant != env.Tenant || existing.DeliveryID != env.DeliveryID {
			return errors.New("quarantine record identity conflict")
		}
	} else {
		env.FailureReason = reason
		env.QuarantinedUnixNano = time.Now().UnixNano()
		if _, err := s.quarantine.AppendEnvelope(env); err != nil {
			return err
		}
	}
	if _, err := s.journal.Acknowledge(recordID); err != nil {
		return err
	}
	slog.Error("journal record moved to quarantine",
		"record_id", recordID,
		"signal", env.Signal,
		"reason", reason,
	)
	s.quarantinedTotal.Add(1)
	s.clearDeliveryState(recordID)
	return nil
}

// StopServing gracefully drains in-flight requests and closes both transports
// without closing the journal. The app uses this split lifecycle so exporter
// drain can still acknowledge records before durable state is closed.
func (s *Server) StopServing(ctx context.Context) error {
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
	if s.retryCancel != nil {
		s.retryCancel()
		s.retryWG.Wait()
	}
	return errors.Join(errs...)
}

// CloseJournal closes durable state after every fed pipeline has drained.
func (s *Server) CloseJournal() error {
	s.journalClose.Do(func() {
		var errs []error
		if s.receipts != nil {
			errs = append(errs, s.receipts.Close())
		}
		if s.quarantine != nil {
			errs = append(errs, s.quarantine.Close())
		}
		if s.journal != nil {
			errs = append(errs, s.journal.Close())
		}
		s.journalCloseErr = errors.Join(errs...)
	})
	return s.journalCloseErr
}

// Stop retains the standalone server contract: stop serving, then close its
// journal. App uses StopServing and CloseJournal separately.
func (s *Server) Stop(ctx context.Context) error {
	return errors.Join(s.StopServing(ctx), s.CloseJournal())
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

func (s *Server) DedupStats() (hits, conflicts, misses, evictions uint64) {
	return s.dedupHits.Load(), s.dedupConflicts.Load(), s.dedupMisses.Load(), s.dedup.Evictions()
}

// Rejected reports records refused at accept time and reported via
// partial_success (spans, data points, log records).
func (s *Server) Rejected() (traces, points, logs uint64) {
	return s.tracesRejected.Load(), s.pointsRejected.Load(), s.logsRejected.Load()
}

func (s *Server) LogLimitRejected() uint64    { return s.logLimitRejected.Load() }
func (s *Server) MetricLimitRejected() uint64 { return s.metricLimitRejected.Load() }

// --- accept-time admission ---

// admitTraces applies the trace admit hook (if any), enqueues the admitted
// spans, and reports how many were rejected as invalid (partial_success).
func (s *Server) admitTraces(ctx context.Context, spans []model.Span, requestPayload ...[]byte) (rejected int, reason string, err error) {
	var ok bool
	ctx, ok = tenantContextWithPolicy(ctx, s.tenantMap)
	if !ok {
		return 0, "", errors.New("tenant principal is not authorized")
	}
	release, admissionErr := s.acquireTenant(ctx)
	if admissionErr != nil {
		return 0, "", admissionErr
	}
	defer release()
	b := model.Batch{Spans: spans}
	if quotaExceeded(ctx, s.tenantLimits, b.Len(), int64(b.SizeBytes())) {
		if tenant, ok := TenantFromContext(ctx); ok {
			s.recordTenant(tenant, false, false, true)
		}
		return 0, "", errTenantQuota
	}
	if s.sink.TraceAdmit != nil {
		b, rejected, reason = s.sink.TraceAdmit(b)
	}
	if b.Len() > 0 {
		payload := mustMarshal(amberexp.TraceRequest(b))
		var request []byte
		if len(requestPayload) > 0 {
			request = requestPayload[0]
		}
		env, attempt, appendErr := s.appendAdmission(ctx, "traces", payload, request, b.Len())
		if appendErr != nil {
			return 0, "", appendErr
		}
		tenant, _ := TenantFromContext(ctx)
		if env.Tenant != "" {
			tenant = env.Tenant
		}
		for i := range b.Spans {
			b.Spans[i].JournalRecordID = env.RecordID
			b.Spans[i].DeliveryAttempt = attempt
			b.Spans[i].Tenant = tenant
		}
		if err = s.sink.Traces(ctx, b); err != nil {
			s.DeliveryFailed(b.DeliveryMetadata(), err)
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

func (s *Server) admitMetrics(ctx context.Context, rm []*metricspb.ResourceMetrics, requestPayload ...[]byte) (rejected int, reason string, err error) {
	var ok bool
	ctx, ok = tenantContextWithPolicy(ctx, s.tenantMap)
	if !ok {
		return 0, "", errors.New("tenant principal is not authorized")
	}
	release, admissionErr := s.acquireTenant(ctx)
	if admissionErr != nil {
		return 0, "", admissionErr
	}
	defer release()
	if tenant, ok := TenantFromContext(ctx); ok {
		if limit := s.tenantLimits[tenant].MaxMetricAttributes; limit > 0 {
			request := &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: rm}
			if metricAttributeCount(request, limit) > limit {
				s.metricLimitRejected.Add(1)
				return 0, "", errMetricAttributesTooMany
			}
		}
		if limit := s.tenantLimits[tenant].MaxMetricAttributeKeys; limit > 0 {
			request := &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: rm}
			if metricAttributeKeyCount(request, limit) > limit {
				s.metricLimitRejected.Add(1)
				return 0, "", errMetricAttributesTooMany
			}
		}
		if limit := s.tenantLimits[tenant].MaxMetricSeries; limit > 0 {
			series := 0
			for _, resource := range rm {
				for _, scope := range resource.GetScopeMetrics() {
					series += len(scope.GetMetrics())
				}
			}
			if series > limit {
				s.metricLimitRejected.Add(1)
				return 0, "", errMetricAttributesTooMany
			}
		}
	}
	b := metric.Batch{ResourceMetrics: rm}
	if quotaExceeded(ctx, s.tenantLimits, b.Len(), int64(b.SizeBytes())) {
		if tenant, ok := TenantFromContext(ctx); ok {
			s.recordTenant(tenant, false, false, true)
		}
		return 0, "", errTenantQuota
	}
	if s.sink.MetricAdmit != nil {
		b, rejected, reason = s.sink.MetricAdmit(b)
	}
	if b.Len() > 0 {
		payload := mustMarshal(&colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: b.ResourceMetrics})
		var request []byte
		if len(requestPayload) > 0 {
			request = requestPayload[0]
		}
		env, attempt, appendErr := s.appendAdmission(ctx, "metrics", payload, request, b.Len())
		if appendErr != nil {
			return 0, "", appendErr
		}
		b.RecordID = env.RecordID
		b.DeliveryAttempt = attempt
		b.Tenant, _ = TenantFromContext(ctx)
		if env.Tenant != "" {
			b.Tenant = env.Tenant
		}
		b.JournalUnits = b.Len()
		if err = s.sink.Metrics(ctx, b); err != nil {
			s.DeliveryFailed(b.DeliveryMetadata(), err)
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

func (s *Server) admitLogs(ctx context.Context, rl []*logspb.ResourceLogs, requestPayload ...[]byte) (rejected int, reason string, err error) {
	var ok bool
	ctx, ok = tenantContextWithPolicy(ctx, s.tenantMap)
	if !ok {
		return 0, "", errors.New("tenant principal is not authorized")
	}
	release, admissionErr := s.acquireTenant(ctx)
	if admissionErr != nil {
		return 0, "", admissionErr
	}
	defer release()
	b := logs.Batch{ResourceLogs: rl}
	if tenant, ok := TenantFromContext(ctx); ok {
		limits := s.tenantLimits[tenant]
		if limit := limits.MaxLogRecordBytes; limit > 0 {
			for _, resource := range rl {
				for _, scope := range resource.GetScopeLogs() {
					for _, record := range scope.GetLogRecords() {
						if proto.Size(record) > limit {
							s.logLimitRejected.Add(1)
							return 0, "", errLogRecordTooLarge
						}
					}
				}
			}
		}
		if limit := limits.MaxLogAttributes; limit > 0 {
			for _, resource := range rl {
				if len(resource.GetResource().GetAttributes()) > limit {
					s.logLimitRejected.Add(1)
					return 0, "", errLogRecordTooLarge
				}
				for _, scope := range resource.GetScopeLogs() {
					if len(scope.GetScope().GetAttributes()) > limit {
						s.logLimitRejected.Add(1)
						return 0, "", errLogRecordTooLarge
					}
					for _, record := range scope.GetLogRecords() {
						if len(record.GetAttributes()) > limit {
							s.logLimitRejected.Add(1)
							return 0, "", errLogRecordTooLarge
						}
					}
				}
			}
		}
		if limit := limits.MaxLogAttributeKeys; limit > 0 {
			keys := make(map[string]struct{})
			addKeys := func(attrs []*commonpb.KeyValue) bool {
				for _, attr := range attrs {
					keys[attr.GetKey()] = struct{}{}
					if len(keys) > limit {
						return false
					}
				}
				return true
			}
			for _, resource := range rl {
				if !addKeys(resource.GetResource().GetAttributes()) {
					s.logLimitRejected.Add(1)
					return 0, "", errLogRecordTooLarge
				}
				for _, scope := range resource.GetScopeLogs() {
					if !addKeys(scope.GetScope().GetAttributes()) {
						s.logLimitRejected.Add(1)
						return 0, "", errLogRecordTooLarge
					}
					for _, record := range scope.GetLogRecords() {
						if !addKeys(record.GetAttributes()) {
							s.logLimitRejected.Add(1)
							return 0, "", errLogRecordTooLarge
						}
					}
				}
			}
		}
	}
	if quotaExceeded(ctx, s.tenantLimits, b.Len(), int64(b.SizeBytes())) {
		if tenant, ok := TenantFromContext(ctx); ok {
			s.recordTenant(tenant, false, false, true)
		}
		return 0, "", errTenantQuota
	}
	if s.sink.LogAdmit != nil {
		b, rejected, reason = s.sink.LogAdmit(b)
	}
	if b.Len() > 0 {
		payload := mustMarshal(&collogspb.ExportLogsServiceRequest{ResourceLogs: b.ResourceLogs})
		var request []byte
		if len(requestPayload) > 0 {
			request = requestPayload[0]
		}
		env, attempt, appendErr := s.appendAdmission(ctx, "logs", payload, request, b.Len())
		if appendErr != nil {
			return 0, "", appendErr
		}
		b.RecordID = env.RecordID
		b.DeliveryAttempt = attempt
		b.Tenant, _ = TenantFromContext(ctx)
		if env.Tenant != "" {
			b.Tenant = env.Tenant
		}
		b.JournalUnits = b.Len()
		if err = s.sink.Logs(ctx, b); err != nil {
			s.DeliveryFailed(b.DeliveryMetadata(), err)
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
	rejected, reason, err := g.s.admitTraces(ctx, spans, mustMarshal(req))
	if err != nil {
		g.s.errs.Add(1)
		if admissionPermanent(err) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if admissionOverload(err) {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		return nil, status.Error(codes.Unavailable, "pipeline unavailable")
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
	rejected, reason, err := g.s.admitMetrics(ctx, req.GetResourceMetrics(), mustMarshal(req))
	if err != nil {
		g.s.errs.Add(1)
		if admissionPermanent(err) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if admissionOverload(err) {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		return nil, status.Error(codes.Unavailable, "pipeline unavailable")
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
	rejected, reason, err := g.s.admitLogs(ctx, req.GetResourceLogs(), mustMarshal(req))
	if err != nil {
		g.s.errs.Add(1)
		if admissionPermanent(err) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if admissionOverload(err) {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		return nil, status.Error(codes.Unavailable, "pipeline unavailable")
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
	var pb coltracepb.ExportTraceServiceRequest
	if err := otlphttp.Unmarshal(enc, body, &pb); err != nil {
		s.errs.Add(1)
		http.Error(w, "bad payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	canonical := mustMarshal(&pb)
	dedup, dedupKey, dedupErr := s.dedupHTTP(req, "traces", canonical)
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
	spans := spansFromResourceSpans(pb.GetResourceSpans())
	admitCtx := withDeliveryID(req.Context(), dedupKey)
	rejected, reason, err := s.admitTraces(admitCtx, spans, canonical)
	if err != nil {
		s.errs.Add(1)
		if admissionPermanent(err) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else if admissionOverload(err) {
			http.Error(w, err.Error(), http.StatusTooManyRequests)
		} else {
			http.Error(w, "pipeline unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	s.rememberHTTP(req, "traces", dedupKey, canonical)
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
	var pb colmetricspb.ExportMetricsServiceRequest
	if err := otlphttp.Unmarshal(enc, body, &pb); err != nil {
		s.errs.Add(1)
		http.Error(w, "bad payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	canonical := mustMarshal(&pb)
	dedup, dedupKey, dedupErr := s.dedupHTTP(req, "metrics", canonical)
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
	admitCtx := withDeliveryID(req.Context(), dedupKey)
	rejected, reason, err := s.admitMetrics(admitCtx, pb.GetResourceMetrics(), canonical)
	if err != nil {
		s.errs.Add(1)
		if admissionPermanent(err) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else if admissionOverload(err) {
			http.Error(w, err.Error(), http.StatusTooManyRequests)
		} else {
			http.Error(w, "pipeline unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	s.rememberHTTP(req, "metrics", dedupKey, canonical)
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
	var pb collogspb.ExportLogsServiceRequest
	if err := otlphttp.Unmarshal(enc, body, &pb); err != nil {
		s.errs.Add(1)
		http.Error(w, "bad payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	canonical := mustMarshal(&pb)
	dedup, dedupKey, dedupErr := s.dedupHTTP(req, "logs", canonical)
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
	admitCtx := withDeliveryID(req.Context(), dedupKey)
	rejected, reason, err := s.admitLogs(admitCtx, pb.GetResourceLogs(), canonical)
	if err != nil {
		s.errs.Add(1)
		if admissionPermanent(err) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else if admissionOverload(err) {
			http.Error(w, err.Error(), http.StatusTooManyRequests)
		} else {
			http.Error(w, "pipeline unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	s.rememberHTTP(req, "logs", dedupKey, canonical)
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
