package otlp

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/yaop-labs/coral/internal/journal"
)

func TestDedupWindowTenantSignalAndConflict(t *testing.T) {
	d := newDedupWindow(2, time.Minute)
	if d.check("a", "traces", "id", []byte("x")) != dedupNew {
		t.Fatal()
	}
	if d.check("a", "traces", "id", []byte("x")) != dedupHit {
		t.Fatal()
	}
	if d.check("a", "traces", "id", []byte("y")) != dedupConflict {
		t.Fatal()
	}
	if d.check("b", "traces", "id", []byte("x")) != dedupNew {
		t.Fatal()
	}
}

func TestDedupWindowTTLAndBoundedEviction(t *testing.T) {
	now := time.Unix(100, 0)
	d := newDedupWindow(2, time.Second)
	d.now = func() time.Time { return now }
	if d.check("t", "logs", "one", []byte("1")) != dedupNew {
		t.Fatal("first id not new")
	}
	if d.check("t", "logs", "two", []byte("2")) != dedupNew {
		t.Fatal("second id not new")
	}
	if d.check("t", "logs", "three", []byte("3")) != dedupNew {
		t.Fatal("third id not new")
	}
	if len(d.items) != 2 {
		t.Fatalf("window size = %d, want 2", len(d.items))
	}
	now = now.Add(2 * time.Second)
	if d.lookup("t", "logs", "two", []byte("2")) != dedupNew {
		t.Fatal("expired id remained a hit")
	}
	if len(d.items) != 0 {
		t.Fatalf("expired entries = %d, want 0", len(d.items))
	}
}

func TestDedupLookupDoesNotRemember(t *testing.T) {
	d := newDedupWindow(4, time.Minute)
	if d.lookup("tenant", "metrics", "id", []byte("payload")) != dedupNew {
		t.Fatal("lookup unexpectedly hit")
	}
	if d.lookup("tenant", "metrics", "id", []byte("payload")) != dedupNew {
		t.Fatal("lookup remembered an identity")
	}
	if d.check("tenant", "metrics", "id", []byte("payload")) != dedupNew {
		t.Fatal("check did not admit new identity")
	}
	if d.lookup("tenant", "metrics", "id", []byte("payload")) != dedupHit {
		t.Fatal("lookup missed remembered identity")
	}
}

func TestReplayRoutedRestoresDeliveryIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.journal")
	j, err := journal.Open(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("durable-payload")
	if err := j.Append(journal.EncodeEnvelope(journal.Envelope{Signal: "traces", Tenant: "tenant-a", DeliveryID: "0123456789abcdef0123456789abcdef", Payload: payload, CreatedUnixNano: time.Now().UnixNano()})); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()

	s, err := NewSecureServer("", "", 0, Sink{}, SecurityConfig{JournalPath: path, JournalMaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.journal.Close()
	if err := s.ReplayRouted(func(journal.Envelope) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := s.dedup.lookup("tenant-a", "traces", "0123456789abcdef0123456789abcdef", payload); got != dedupHit {
		t.Fatalf("restored dedup result = %v, want hit", got)
	}
}
