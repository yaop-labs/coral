package metric

import (
	"context"
	"log/slog"
	"net"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"

	"github.com/yaop-labs/coral/internal/otlphttp"
)

const maxBodyBytes = 16 << 20

// OTLPReceiver accepts OTLP metrics over gRPC (MetricsService) and HTTP
// (/v1/metrics) and emits them into the metric pipeline.
type OTLPReceiver struct {
	grpcAddr string
	httpAddr string
	logger   *slog.Logger

	emit    func(context.Context, Batch) error
	grpcSrv *grpc.Server
	httpSrv *http.Server
	grpcLn  net.Listener
	ready   chan struct{}
}

func NewOTLPReceiver(grpcAddr, httpAddr string, logger *slog.Logger) *OTLPReceiver {
	return &OTLPReceiver{grpcAddr: grpcAddr, httpAddr: httpAddr, logger: logger, ready: make(chan struct{})}
}

func (r *OTLPReceiver) Start(ctx context.Context, emit func(context.Context, Batch) error) error {
	r.emit = emit

	if r.grpcAddr != "" {
		ln, err := net.Listen("tcp", r.grpcAddr)
		if err != nil {
			close(r.ready)
			return err
		}
		r.grpcLn = ln
		r.grpcSrv = grpc.NewServer(grpc.MaxRecvMsgSize(maxBodyBytes))
		colmetricspb.RegisterMetricsServiceServer(r.grpcSrv, &grpcMetricsService{r: r})
		go func() { _ = r.grpcSrv.Serve(ln) }()
		r.logger.Info("metric otlp grpc receiver listening", "addr", ln.Addr().String())
	}

	if r.httpAddr != "" {
		ln, err := net.Listen("tcp", r.httpAddr)
		if err != nil {
			close(r.ready)
			return err
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/v1/metrics", r.handleHTTP)
		r.httpSrv = &http.Server{Handler: mux}
		go func() { _ = r.httpSrv.Serve(ln) }()
		r.logger.Info("metric otlp http receiver listening", "addr", ln.Addr().String())
	}

	close(r.ready)
	<-ctx.Done()
	return nil
}

func (r *OTLPReceiver) Stop(ctx context.Context) error {
	if r.grpcSrv != nil {
		r.grpcSrv.GracefulStop()
	}
	if r.httpSrv != nil {
		return r.httpSrv.Shutdown(ctx)
	}
	return nil
}

// GRPCAddr returns the bound gRPC address (for tests using :0).
func (r *OTLPReceiver) GRPCAddr() string {
	<-r.ready
	if r.grpcLn == nil {
		return ""
	}
	return r.grpcLn.Addr().String()
}

func (r *OTLPReceiver) ingest(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) error {
	b := Batch{ResourceMetrics: req.GetResourceMetrics()}
	if b.Empty() {
		return nil
	}
	return r.emit(ctx, b)
}

type grpcMetricsService struct {
	colmetricspb.UnimplementedMetricsServiceServer
	r *OTLPReceiver
}

func (s *grpcMetricsService) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	if err := s.r.ingest(ctx, req); err != nil {
		return nil, err
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

func (r *OTLPReceiver) handleHTTP(w http.ResponseWriter, req *http.Request) {
	body, enc, ok := otlphttp.ReadBody(w, req, maxBodyBytes)
	if !ok {
		return
	}
	var pb colmetricspb.ExportMetricsServiceRequest
	if err := otlphttp.Unmarshal(enc, body, &pb); err != nil {
		http.Error(w, "bad payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := r.ingest(req.Context(), &pb); err != nil {
		http.Error(w, "pipeline unavailable", http.StatusServiceUnavailable)
		return
	}
	resp, _ := proto.Marshal(&colmetricspb.ExportMetricsServiceResponse{})
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(resp)
}
