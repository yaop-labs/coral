package jaeger

import (
	"context"
	"errors"
	"net"
	"sync/atomic"

	"github.com/yaop-labs/coral/internal/model"
)

// ThriftUDPReceiver listens for Jaeger spans on a UDP port.
// The Jaeger agent sends Thrift-binary-encoded Batch structs as UDP datagrams.
// Spans are dropped when the pipeline cannot accept them.
type ThriftUDPReceiver struct {
	endpoint      string
	maxPacketSize int

	conn  *net.UDPConn
	ready chan struct{}

	dropped atomic.Uint64
	spans   atomic.Uint64
}

func NewThriftUDP(endpoint string, maxPacketSize int) (*ThriftUDPReceiver, error) {
	if endpoint == "" {
		return nil, errors.New("jaeger thrift udp: endpoint required")
	}
	if maxPacketSize <= 0 {
		maxPacketSize = 65000
	}
	return &ThriftUDPReceiver{
		endpoint:      endpoint,
		maxPacketSize: maxPacketSize,
		ready:         make(chan struct{}),
	}, nil
}

func (r *ThriftUDPReceiver) Start(ctx context.Context, emit func(context.Context, model.Batch) error) error {
	addr, err := net.ResolveUDPAddr("udp", r.endpoint)
	if err != nil {
		close(r.ready)
		return err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		close(r.ready)
		return err
	}
	r.conn = conn
	close(r.ready)

	buf := make([]byte, r.maxPacketSize)
	go func() {
		for {
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			if ctx.Err() != nil {
				return
			}

			packet := make([]byte, n)
			copy(packet, buf[:n])
			r.handlePacket(ctx, packet, emit)
		}
	}()

	<-ctx.Done()
	return nil
}

// handlePacket decodes one datagram and emits its spans. It recovers from any
// panic in the Thrift decoder so a single malformed packet can never take down
// the whole process — defense-in-depth on top of the decoder's bounds checks.
func (r *ThriftUDPReceiver) handlePacket(ctx context.Context, packet []byte, emit func(context.Context, model.Batch) error) {
	defer func() {
		if rec := recover(); rec != nil {
			r.dropped.Add(1)
		}
	}()

	spans, err := DecodeBatch(packet)
	if err != nil || len(spans) == 0 {
		return
	}
	if err := emit(ctx, model.Batch{Spans: spans}); err != nil {
		r.dropped.Add(uint64(len(spans)))
	} else {
		r.spans.Add(uint64(len(spans)))
	}
}

func (r *ThriftUDPReceiver) Stop(ctx context.Context) error {
	select {
	case <-r.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	if r.conn == nil {
		return nil
	}
	return r.conn.Close()
}

func (r *ThriftUDPReceiver) Addr() string {
	<-r.ready
	if r.conn == nil {
		return ""
	}
	return r.conn.LocalAddr().String()
}

func (r *ThriftUDPReceiver) Stats() (spans, dropped uint64) {
	return r.spans.Load(), r.dropped.Load()
}
