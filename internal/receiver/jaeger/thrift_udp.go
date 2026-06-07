package jaeger

import (
	"context"
	"errors"
	"net"
	"sync/atomic"

	"github.com/hnlbs/collector/internal/model"
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

			packet := make([]byte, n)
			copy(packet, buf[:n])

			spans, err := DecodeBatch(packet)
			if err != nil || len(spans) == 0 {
				continue
			}

			b := model.Batch{Spans: spans}
			select {
			case <-ctx.Done():
				return
			default:
				if err := emit(ctx, b); err != nil {
					r.dropped.Add(uint64(len(spans)))
				} else {
					r.spans.Add(uint64(len(spans)))
				}
			}
		}
	}()

	<-ctx.Done()
	return nil
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
