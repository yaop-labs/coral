// Package zipkin implements a Zipkin v2 HTTP receiver.
// It decodes Zipkin JSON directly.
package zipkin

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"

	"github.com/hnlbs/collector/internal/model"
)

// HTTPReceiver accepts Zipkin v2 spans at POST /api/v2/spans.
type HTTPReceiver struct {
	endpoint string
	srv      *http.Server
	ln       net.Listener
	ready    chan struct{}
}

func New(endpoint string) (*HTTPReceiver, error) {
	if endpoint == "" {
		return nil, errors.New("zipkin http: endpoint required")
	}
	return &HTTPReceiver{endpoint: endpoint, ready: make(chan struct{})}, nil
}

func (r *HTTPReceiver) Start(ctx context.Context, emit func(context.Context, model.Batch) error) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v2/spans", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(io.LimitReader(req.Body, 16<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		spans, err := decodeSpans(body)
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
