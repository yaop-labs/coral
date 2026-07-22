package otlp

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
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

func TestReplayRoutedRecoversTailBeforeLegacyIDMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.journal")
	j, err := journal.Open(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Append(journal.EncodeEnvelope(journal.Envelope{Signal: "traces", Payload: []byte("legacy")})); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	s, err := NewSecureServer("", "", 0, Sink{}, SecurityConfig{JournalPath: path, JournalMaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.journal.Close()
	var got journal.Envelope
	if err := s.ReplayRouted(func(env journal.Envelope) error {
		got = env
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got.RecordID == "" || string(got.Payload) != "legacy" {
		t.Fatalf("recovered envelope = %+v", got)
	}
}

func TestReplayRoutedReconcilesInterruptedQuarantineMove(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.journal")
	active, err := journal.Open(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := active.AppendEnvelope(journal.Envelope{
		Signal: "traces", Payload: []byte("durable"), CreatedUnixNano: time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := active.Close(); err != nil {
		t.Fatal(err)
	}
	quarantine, err := journal.Open(path+".quarantine", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	stored.FailureReason = "permanent partial success"
	stored.QuarantinedUnixNano = time.Now().UnixNano()
	if _, err := quarantine.AppendEnvelope(stored); err != nil {
		t.Fatal(err)
	}
	if err := quarantine.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := NewSecureServer("", "", 0, Sink{}, SecurityConfig{JournalPath: path, JournalMaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.CloseJournal()
	dispatched := 0
	if err := s.ReplayRouted(func(journal.Envelope) error {
		dispatched++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	stats := s.DurabilityStats()
	if dispatched != 0 || stats.ActiveRecords != 0 || stats.QuarantineRecords != 1 {
		t.Fatalf("reconciled state: dispatched=%d stats=%+v", dispatched, stats)
	}
}

func TestReplayRoutedReconcilesInterruptedReceiptAcknowledgement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.journal")
	payload := []byte("already delivered")
	digestBytes := sha256.Sum256(payload)
	digest := hex.EncodeToString(digestBytes[:])
	active, err := journal.Open(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := active.AppendEnvelope(journal.Envelope{
		Signal: "traces", Tenant: "tenant-a", DeliveryID: "0123456789abcdef0123456789abcdef",
		RequestDigest: digest, Payload: payload, CreatedUnixNano: time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := active.Close(); err != nil {
		t.Fatal(err)
	}
	receipts, err := journal.Open(path+".receipts", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receipts.AppendEnvelope(journal.Envelope{
		Signal: stored.Signal, Tenant: stored.Tenant, DeliveryID: stored.DeliveryID,
		RecordID: stored.RecordID, RequestDigest: digest, CreatedUnixNano: time.Now().UnixNano(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := receipts.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := NewSecureServer("", "", 0, Sink{}, SecurityConfig{JournalPath: path, JournalMaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.CloseJournal()
	dispatched := 0
	if err := s.ReplayRouted(func(journal.Envelope) error {
		dispatched++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	stats := s.DurabilityStats()
	if dispatched != 0 || stats.ActiveRecords != 0 || stats.ReceiptRecords != 1 {
		t.Fatalf("reconciled state: dispatched=%d stats=%+v", dispatched, stats)
	}
	if got := s.dedup.lookup(stored.Tenant, stored.Signal, stored.DeliveryID, payload); got != dedupHit {
		t.Fatalf("restored receipt lookup = %v, want hit", got)
	}
}

func TestDedupWindowDefaultsAreBounded(t *testing.T) {
	d := newDedupWindow(0, 0)
	if d.max != 100000 || d.ttl != 15*time.Minute {
		t.Fatalf("defaults = max %d ttl %s", d.max, d.ttl)
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
