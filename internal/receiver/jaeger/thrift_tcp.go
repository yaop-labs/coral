package jaeger

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/hnlbs/collector/internal/model"
)

// ThriftTCPReceiver accepts Jaeger spans over a TCP connection using the
// Thrift framed transport. Each frame is a 4-byte big-endian length prefix
// followed by the Thrift-binary-encoded Batch struct.
type ThriftTCPReceiver struct {
	endpoint string
	ln       net.Listener
	ready    chan struct{}

	mu   sync.Mutex
	emit func(context.Context, model.Batch) error
}

func NewThriftTCP(endpoint string) (*ThriftTCPReceiver, error) {
	if endpoint == "" {
		return nil, errors.New("jaeger thrift tcp: endpoint required")
	}
	return &ThriftTCPReceiver{endpoint: endpoint, ready: make(chan struct{})}, nil
}

func (r *ThriftTCPReceiver) Start(ctx context.Context, emit func(context.Context, model.Batch) error) error {
	r.mu.Lock()
	r.emit = emit
	r.mu.Unlock()

	ln, err := net.Listen("tcp", r.endpoint)
	if err != nil {
		close(r.ready)
		return err
	}
	r.ln = ln
	close(r.ready)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			go r.handleConn(ctx, conn)
		}
	}()

	<-ctx.Done()
	return nil
}

func (r *ThriftTCPReceiver) Stop(ctx context.Context) error {
	select {
	case <-r.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	if r.ln == nil {
		return nil
	}
	return r.ln.Close()
}

func (r *ThriftTCPReceiver) Addr() string {
	<-r.ready
	if r.ln == nil {
		return ""
	}
	return r.ln.Addr().String()
}

func (r *ThriftTCPReceiver) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
			return
		}
		frameLen := binary.BigEndian.Uint32(lenBuf[:])
		if frameLen == 0 || frameLen > 16<<20 {
			return
		}

		frame := make([]byte, frameLen)
		if _, err := io.ReadFull(conn, frame); err != nil {
			return
		}

		spans, err := DecodeBatch(frame)
		if err != nil || len(spans) == 0 {
			continue
		}

		r.mu.Lock()
		emit := r.emit
		r.mu.Unlock()

		if emit != nil {
			_ = emit(ctx, model.Batch{Spans: spans})
		}
	}
}
