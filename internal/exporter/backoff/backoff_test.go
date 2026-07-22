package backoff

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestStatusError_PermanentVsRetryable(t *testing.T) {
	for _, code := range []int{400, 401, 403, 404, 413, 500, 501} {
		if err := StatusError(code, nil, "x"); !IsPermanent(err) {
			t.Errorf("status %d should be permanent (not retried)", code)
		}
	}
	for _, code := range []int{429, 502, 503, 504} {
		err := StatusError(code, nil, "x")
		if err == nil || IsPermanent(err) {
			t.Errorf("status %d should be retryable", code)
		}
	}
	if StatusError(200, nil, "") != nil {
		t.Error("2xx should classify as success (nil)")
	}
}

func TestDo_StopsOnPermanent(t *testing.T) {
	calls := 0
	err := Policy{MaxAttempts: 5, InitialBackoff: time.Millisecond}.Do(
		context.Background(),
		func(context.Context) error {
			calls++
			return Permanent(errors.New("bad payload"))
		})
	if err == nil {
		t.Fatal("expected the permanent error to surface")
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (permanent must not be retried)", calls)
	}
}

func TestDo_RetriesTransientUpToMax(t *testing.T) {
	calls := 0
	err := Policy{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}.Do(
		context.Background(),
		func(context.Context) error {
			calls++
			return StatusError(503, nil, "overloaded")
		})
	if err == nil {
		t.Fatal("expected error after exhausting attempts")
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestDo_SucceedsAfterTransient(t *testing.T) {
	calls := 0
	err := Policy{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}.Do(
		context.Background(),
		func(context.Context) error {
			calls++
			if calls < 2 {
				return StatusError(503, nil, "warming up")
			}
			return nil
		})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestParseRetryAfter(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "2")
	if got := parseRetryAfter(h); got != 2*time.Second {
		t.Fatalf("parseRetryAfter(seconds) = %v, want 2s", got)
	}
	if got := parseRetryAfter(http.Header{}); got != 0 {
		t.Fatalf("parseRetryAfter(absent) = %v, want 0", got)
	}
}
