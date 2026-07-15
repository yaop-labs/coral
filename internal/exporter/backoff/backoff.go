// Package backoff is the shared retry engine for coral's exporters. It
// implements the platform contract §4 response semantics: 4xx (except 429) are
// permanent and never retried; 429/502/503/504 are transient and retried with
// jittered exponential backoff that honors a server Retry-After hint. One
// implementation serves every signal (traces, metrics, logs).
package backoff

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// Policy bounds retry attempts and backoff growth.
type Policy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// ApplyDefaults fills unset fields with sane values.
func (p *Policy) ApplyDefaults() {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 3
	}
	if p.InitialBackoff <= 0 {
		p.InitialBackoff = 200 * time.Millisecond
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = 5 * time.Second
	}
	if p.MaxBackoff < p.InitialBackoff {
		p.MaxBackoff = p.InitialBackoff
	}
}

// Do runs fn up to p.MaxAttempts. It returns immediately on success, on a
// Permanent error, or when ctx is done. Between transient failures it sleeps a
// jittered exponential backoff, or the server-suggested Retry-After when that
// is larger.
func (p Policy) Do(ctx context.Context, fn func(context.Context) error) error {
	p.ApplyDefaults()
	wait := p.InitialBackoff
	var err error
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		err = fn(ctx)
		if err == nil {
			return nil
		}
		if isPermanent(err) {
			return err
		}
		if attempt == p.MaxAttempts {
			break
		}
		sleep := jitter(wait)
		if after, ok := retryAfter(err); ok && after > sleep {
			sleep = after
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
		wait *= 2
		if wait > p.MaxBackoff {
			wait = p.MaxBackoff
		}
	}
	return err
}

// jitter scales d by a random factor in [0.5, 1.0], de-synchronizing retries
// across exporters without collapsing the backoff to near-zero.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := int64(d) / 2
	return time.Duration(int64(d) - half + rand.Int64N(half+1))
}

// permanent marks an error as non-retryable.
type permanent struct{ err error }

func (p *permanent) Error() string { return p.err.Error() }
func (p *permanent) Unwrap() error { return p.err }

// Permanent wraps err so Do stops immediately instead of retrying. Use it for
// failures that cannot succeed on a retry (bad payload, 4xx, marshal errors).
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanent{err: err}
}

func isPermanent(err error) bool {
	var p *permanent
	return errors.As(err, &p)
}

// retryable carries an optional server-suggested wait (Retry-After).
type retryable struct {
	err   error
	after time.Duration
}

func (r *retryable) Error() string { return r.err.Error() }
func (r *retryable) Unwrap() error { return r.err }

func retryAfter(err error) (time.Duration, bool) {
	var r *retryable
	if errors.As(err, &r) && r.after > 0 {
		return r.after, true
	}
	return 0, false
}

// StatusError classifies an OTLP/HTTP response status per contract §4: nil for
// 2xx, a Permanent error for non-retryable statuses (4xx, 5xx other than the
// transient set), or a retryable error carrying any Retry-After for
// 429/502/503/504. msg is an optional response-body snippet for diagnostics.
func StatusError(statusCode int, header http.Header, msg string) error {
	if statusCode < 300 {
		return nil
	}
	err := fmt.Errorf("status %d: %s", statusCode, msg)
	switch statusCode {
	case http.StatusTooManyRequests, // 429
		http.StatusBadGateway,         // 502
		http.StatusServiceUnavailable, // 503
		http.StatusGatewayTimeout:     // 504
		return &retryable{err: err, after: parseRetryAfter(header)}
	default:
		return &permanent{err: err}
	}
}

// parseRetryAfter reads a Retry-After header in either delay-seconds or
// HTTP-date form; it returns 0 when absent or in the past.
func parseRetryAfter(h http.Header) time.Duration {
	if h == nil {
		return 0
	}
	v := h.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}
