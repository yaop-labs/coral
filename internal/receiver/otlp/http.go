package otlp

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/hnlbs/collector/internal/model"
)

// HTTPReceiver accepts OTLP traces over HTTP/protobuf at /v1/traces.
type HTTPReceiver struct {
	endpoint    string
	readTimeout time.Duration

	emit  func(context.Context, model.Batch) error
	srv   *http.Server
	ln    net.Listener
	ready chan struct{}

	spansAccepted atomic.Uint64
	requests      atomic.Uint64
	errs          atomic.Uint64
}

func NewHTTP(endpoint string) (*HTTPReceiver, error) {
	if endpoint == "" {
		return nil, errors.New("otlp http: endpoint required")
	}
	return &HTTPReceiver{
		endpoint:    endpoint,
		readTimeout: 10 * time.Second,
		ready:       make(chan struct{}),
	}, nil
}

func (r *HTTPReceiver) Start(ctx context.Context, emit func(context.Context, model.Batch) error) error {
	r.emit = emit

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", r.handleTraces)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	ln, err := net.Listen("tcp", r.endpoint)
	if err != nil {
		close(r.ready)
		return err
	}

	r.ln = ln
	r.srv = &http.Server{Handler: mux, ReadTimeout: r.readTimeout}
	close(r.ready)

	go func() { _ = r.srv.Serve(ln) }()

	<-ctx.Done()
	return nil
}

func (r *HTTPReceiver) Stop(ctx context.Context) error {
	select {
	case <-r.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	if r.srv == nil {
		return nil
	}
	return r.srv.Shutdown(ctx)
}

func (r *HTTPReceiver) Addr() string {
	<-r.ready
	if r.ln == nil {
		return ""
	}
	return r.ln.Addr().String()
}

func (r *HTTPReceiver) handleTraces(w http.ResponseWriter, req *http.Request) {
	r.requests.Add(1)
	if req.Method != http.MethodPost {
		r.errs.Add(1)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, 16<<20))
	if err != nil {
		r.errs.Add(1)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var pb coltracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &pb); err != nil {
		r.errs.Add(1)
		http.Error(w, "bad protobuf: "+err.Error(), http.StatusBadRequest)
		return
	}

	spans := spansFromResourceSpans(pb.GetResourceSpans())
	if len(spans) > 0 {
		if err := r.emit(req.Context(), model.Batch{Spans: spans}); err != nil {
			r.errs.Add(1)
			http.Error(w, "pipeline unavailable", http.StatusServiceUnavailable)
			return
		}
		r.spansAccepted.Add(uint64(len(spans)))
	}

	resp, _ := proto.Marshal(&coltracepb.ExportTraceServiceResponse{})
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(resp)
}
