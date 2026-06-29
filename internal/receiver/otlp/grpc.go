package otlp

import (
	"context"
	"errors"
	"net"
	"sync/atomic"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"

	"github.com/yaop-labs/coral/internal/model"
)

// GRPCReceiver accepts OTLP traces over gRPC and emits model.Batch.
type GRPCReceiver struct {
	endpoint    string
	maxRecvSize int

	emit  func(context.Context, model.Batch) error
	srv   *grpc.Server
	ln    net.Listener
	ready chan struct{} // closed once ln and srv are set

	spansAccepted atomic.Uint64
	requests      atomic.Uint64
	errs          atomic.Uint64
}

func NewGRPC(endpoint string, maxRecvSize int) (*GRPCReceiver, error) {
	if endpoint == "" {
		return nil, errors.New("otlp grpc: endpoint required")
	}
	if maxRecvSize <= 0 {
		maxRecvSize = 16 << 20
	}
	return &GRPCReceiver{
		endpoint:    endpoint,
		maxRecvSize: maxRecvSize,
		ready:       make(chan struct{}),
	}, nil
}

func (r *GRPCReceiver) Start(ctx context.Context, emit func(context.Context, model.Batch) error) error {
	r.emit = emit

	ln, err := net.Listen("tcp", r.endpoint)
	if err != nil {
		close(r.ready)
		return err
	}

	srv := grpc.NewServer(grpc.MaxRecvMsgSize(r.maxRecvSize))
	coltracepb.RegisterTraceServiceServer(srv, &grpcTraceService{recv: r})

	r.ln = ln
	r.srv = srv
	close(r.ready) // signal Stop() that fields are initialized

	go func() { _ = srv.Serve(ln) }()

	<-ctx.Done()
	return nil
}

func (r *GRPCReceiver) Stop(ctx context.Context) error {
	select {
	case <-r.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	if r.srv == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		r.srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		r.srv.Stop()
		return ctx.Err()
	}
}

func (r *GRPCReceiver) Addr() string {
	<-r.ready
	if r.ln == nil {
		return ""
	}
	return r.ln.Addr().String()
}

func (r *GRPCReceiver) Stats() (requests, errs, spansAccepted uint64) {
	return r.requests.Load(), r.errs.Load(), r.spansAccepted.Load()
}

type grpcTraceService struct {
	coltracepb.UnimplementedTraceServiceServer
	recv *GRPCReceiver
}

func (s *grpcTraceService) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	s.recv.requests.Add(1)
	spans := spansFromResourceSpans(req.GetResourceSpans())
	if len(spans) == 0 {
		return &coltracepb.ExportTraceServiceResponse{}, nil
	}
	if err := s.recv.emit(ctx, model.Batch{Spans: spans}); err != nil {
		s.recv.errs.Add(1)
		return nil, err
	}
	s.recv.spansAccepted.Add(uint64(len(spans)))
	return &coltracepb.ExportTraceServiceResponse{}, nil
}
