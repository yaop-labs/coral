package jaeger

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"

	"github.com/hnlbs/collector/internal/model"
)

// ThriftHTTPReceiver accepts Jaeger spans over HTTP at POST /api/traces.
// The body is a Thrift-binary-encoded Batch struct (same as TCP, no frame prefix).
type ThriftHTTPReceiver struct {
	endpoint string
	srv      *http.Server
	ln       net.Listener
	ready    chan struct{}
}

func NewThriftHTTP(endpoint string) (*ThriftHTTPReceiver, error) {
	if endpoint == "" {
		return nil, errors.New("jaeger thrift http: endpoint required")
	}
	return &ThriftHTTPReceiver{endpoint: endpoint, ready: make(chan struct{})}, nil
}

func (r *ThriftHTTPReceiver) Start(ctx context.Context, emit func(context.Context, model.Batch) error) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/traces", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(req.Body, 16<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		spans, err := DecodeBatch(body)
		if err != nil {
			http.Error(w, "decode: "+err.Error(), http.StatusBadRequest)
			return
		}
		if len(spans) > 0 {
			if err := emit(req.Context(), model.Batch{Spans: spans}); err != nil {
				http.Error(w, "pipeline unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusAccepted)
	})

	ln, err := net.Listen("tcp", r.endpoint)
	if err != nil {
		close(r.ready)
		return err
	}
	r.ln = ln
	r.srv = &http.Server{Handler: mux}
	close(r.ready)

	go func() { _ = r.srv.Serve(ln) }()

	<-ctx.Done()
	return nil
}

func (r *ThriftHTTPReceiver) Stop(ctx context.Context) error {
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

func (r *ThriftHTTPReceiver) Addr() string {
	<-r.ready
	if r.ln == nil {
		return ""
	}
	return r.ln.Addr().String()
}
