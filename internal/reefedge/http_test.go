package reefedge

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yaop-labs/reef/bearer"
	"github.com/yaop-labs/reef/edge"
)

func TestHTTPClientDoesNotFollowCredentialBearingRedirect(t *testing.T) {
	var escapedAuthorization atomic.Value
	escapedAuthorization.Store("")
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		escapedAuthorization.Store(r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	defer destination.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	client, managed, err := NewHTTPClient(time.Second, edge.ClientConfig{
		Target: origin.URL,
		Auth: &bearer.ClientConfig{
			Token: "secret",
		},
		DangerAllowBearerOverPlaintext: true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer managed.Close()
	resp, err := client.Get(origin.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want redirect response", resp.StatusCode)
	}
	if got := escapedAuthorization.Load().(string); got != "" {
		t.Fatalf("authorization escaped target origin: %q", got)
	}
}

func TestHTTPClientRejectsDifferentOrigin(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer origin.Close()
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer other.Close()

	client, managed, err := NewHTTPClient(time.Second, edge.ClientConfig{Target: origin.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer managed.Close()
	_, err = client.Get(other.URL)
	if err == nil {
		t.Fatal("expected different-origin request to fail")
	}
}
