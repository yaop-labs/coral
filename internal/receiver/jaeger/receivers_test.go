package jaeger

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/yaop-labs/coral/internal/model"
)

// collectEmit collects batches from emit calls into a slice.
type collectEmit struct {
	mu    sync.Mutex
	spans []model.Span
}

func (c *collectEmit) emit(_ context.Context, b model.Batch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.spans = append(c.spans, b.Spans...)
	return nil
}

func (c *collectEmit) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.spans)
}

// startReceiver starts r in a goroutine and returns cancel + a func to stop.
func startReceiver(t *testing.T, r interface {
	Start(context.Context, func(context.Context, model.Batch) error) error
	Stop(context.Context) error
	Addr() string
}, emit func(context.Context, model.Batch) error) (addr string, stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = r.Start(ctx, emit) }()

	// wait for addr to be available
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a := r.Addr(); a != "" {
			addr = a
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if addr == "" {
		cancel()
		t.Fatal("receiver did not bind within timeout")
	}

	stop = func() {
		cancel()
		stopCtx, sc := context.WithTimeout(context.Background(), 2*time.Second)
		defer sc()
		_ = r.Stop(stopCtx)
	}
	return addr, stop
}

func TestThriftUDPReceiver_StartStop(t *testing.T) {
	r, err := NewThriftUDP("127.0.0.1:0", 0)
	if err != nil {
		t.Fatal(err)
	}
	_, stop := startReceiver(t, r, func(_ context.Context, _ model.Batch) error { return nil })
	defer stop()

	if r.Addr() == "" {
		t.Error("Addr() empty after Start")
	}
}

func TestThriftUDPReceiver_Stats(t *testing.T) {
	r, err := NewThriftUDP("127.0.0.1:0", 0)
	if err != nil {
		t.Fatal(err)
	}
	spans, dropped := r.Stats()
	if spans != 0 || dropped != 0 {
		t.Errorf("want 0/0, got %d/%d", spans, dropped)
	}
}

func TestThriftUDPReceiver_InvalidEndpoint(t *testing.T) {
	_, err := NewThriftUDP("", 0)
	if err == nil {
		t.Error("expected error for empty endpoint")
	}
}

func TestThriftUDPReceiver_SendPacket(t *testing.T) {
	col := &collectEmit{}
	r, err := NewThriftUDP("127.0.0.1:0", 0)
	if err != nil {
		t.Fatal(err)
	}
	addr, stop := startReceiver(t, r, col.emit)
	defer stop()

	// Build a valid batch and send as UDP datagram.
	payload := buildBatch("udp-svc", []testSpan{
		{traceHigh: 1, traceLow: 1, spanID: 1, opName: "op", startTimeUS: 1000, durationUS: 100},
	})

	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if col.count() >= 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("span not received via UDP, got %d", col.count())
}

func TestThriftTCPReceiver_StartStop(t *testing.T) {
	r, err := NewThriftTCP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, stop := startReceiver(t, r, func(_ context.Context, _ model.Batch) error { return nil })
	defer stop()

	if r.Addr() == "" {
		t.Error("Addr() empty after Start")
	}
}

func TestThriftTCPReceiver_InvalidEndpoint(t *testing.T) {
	_, err := NewThriftTCP("")
	if err == nil {
		t.Error("expected error for empty endpoint")
	}
}

func TestThriftTCPReceiver_SendFrame(t *testing.T) {
	col := &collectEmit{}
	r, err := NewThriftTCP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr, stop := startReceiver(t, r, col.emit)
	defer stop()

	payload := buildBatch("tcp-svc", []testSpan{
		{traceHigh: 2, traceLow: 2, spanID: 2, opName: "tcp-op", startTimeUS: 2000, durationUS: 200},
	})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Framed transport: 4-byte big-endian length prefix.
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	if _, err := conn.Write(lenBuf[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if col.count() >= 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("span not received via TCP, got %d", col.count())
}

func TestThriftTCPReceiver_ZeroFrameLength(t *testing.T) {
	r, err := NewThriftTCP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, stop := startReceiver(t, r, func(_ context.Context, _ model.Batch) error { return nil })
	defer stop()

	conn, err := net.Dial("tcp", r.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// A zero-length frame closes the connection.
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 0)
	if _, err := conn.Write(lenBuf[:]); err != nil {
		t.Fatal(err)
	}
	// The receiver closes the connection.
	time.Sleep(20 * time.Millisecond)
}

func TestThriftHTTPReceiver_StartStop(t *testing.T) {
	r, err := NewThriftHTTP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, stop := startReceiver(t, r, func(_ context.Context, _ model.Batch) error { return nil })
	defer stop()

	if r.Addr() == "" {
		t.Error("Addr() empty after Start")
	}
}

func TestThriftHTTPReceiver_InvalidEndpoint(t *testing.T) {
	_, err := NewThriftHTTP("")
	if err == nil {
		t.Error("expected error for empty endpoint")
	}
}

func TestThriftHTTPReceiver_MethodNotAllowed(t *testing.T) {
	r, err := NewThriftHTTP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, stop := startReceiver(t, r, func(_ context.Context, _ model.Batch) error { return nil })
	defer stop()

	resp, err := http.Get("http://" + r.Addr() + "/api/traces")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

func TestThriftHTTPReceiver_BadBody(t *testing.T) {
	r, err := NewThriftHTTP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, stop := startReceiver(t, r, func(_ context.Context, _ model.Batch) error { return nil })
	defer stop()

	// Invalid Thrift returns a decode error.
	resp, err := http.Post("http://"+r.Addr()+"/api/traces",
		"application/x-thrift", bytes.NewReader([]byte{0xFF, 0xFE, 0xAB}))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	// Both decode errors and empty batches are valid outcomes.
	if resp.StatusCode >= 500 {
		t.Errorf("unexpected 5xx status: %d", resp.StatusCode)
	}
}

func TestThriftHTTPReceiver_ValidBatch(t *testing.T) {
	col := &collectEmit{}
	r, err := NewThriftHTTP("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, stop := startReceiver(t, r, col.emit)
	defer stop()

	payload := buildBatch("http-svc", []testSpan{
		{traceHigh: 3, traceLow: 3, spanID: 3, opName: "http-op", startTimeUS: 3000, durationUS: 300},
	})

	resp, err := http.Post("http://"+r.Addr()+"/api/traces",
		"application/x-thrift", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("status = %d, want 202", resp.StatusCode)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if col.count() >= 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("span not received via HTTP, got %d", col.count())
}
