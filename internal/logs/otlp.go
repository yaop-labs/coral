package logs

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

const maxBodyBytes = 16 << 20

// OTLPReceiver accepts OTLP logs over gRPC (LogsService) and HTTP (/v1/logs).
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
		collogspb.RegisterLogsServiceServer(r.grpcSrv, &grpcLogsService{r: r})
		go func() { _ = r.grpcSrv.Serve(ln) }()
		r.logger.Info("log otlp grpc receiver listening", "addr", ln.Addr().String())
	}

	if r.httpAddr != "" {
		ln, err := net.Listen("tcp", r.httpAddr)
		if err != nil {
			close(r.ready)
			return err
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/v1/logs", r.handleHTTP)
		r.httpSrv = &http.Server{Handler: mux}
		go func() { _ = r.httpSrv.Serve(ln) }()
		r.logger.Info("log otlp http receiver listening", "addr", ln.Addr().String())
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

func (r *OTLPReceiver) ingest(ctx context.Context, req *collogspb.ExportLogsServiceRequest) error {
	b := Batch{ResourceLogs: req.GetResourceLogs()}
	if b.Empty() {
		return nil
	}
	return r.emit(ctx, b)
}

type grpcLogsService struct {
	collogspb.UnimplementedLogsServiceServer
	r *OTLPReceiver
}

func (s *grpcLogsService) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	if err := s.r.ingest(ctx, req); err != nil {
		return nil, err
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

func (r *OTLPReceiver) handleHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxBodyBytes))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var pb collogspb.ExportLogsServiceRequest
	if err := proto.Unmarshal(body, &pb); err != nil {
		http.Error(w, "bad protobuf: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := r.ingest(req.Context(), &pb); err != nil {
		http.Error(w, "pipeline unavailable", http.StatusServiceUnavailable)
		return
	}
	resp, _ := proto.Marshal(&collogspb.ExportLogsServiceResponse{})
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(resp)
}
