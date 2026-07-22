package journal

import (
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func FuzzDecodeEnvelope(f *testing.F) {
	for _, seed := range [][]byte{{}, {2, 0, 0}, {2, 1, 'x', 0, 0, 0, 0, 0, 0, 0, 1}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		_, _ = DecodeEnvelope(raw)
	})
}

func TestReplayRejectsOversizedRecordBeforeAllocation(t *testing.T) {
	p := filepath.Join(t.TempDir(), "journal")
	j, err := Open(p, 1<<40)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if _, err := j.f.Write([]byte{0, 0, 0, 0x80, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := j.f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := j.Replay(func([]byte) error { t.Fatal("unexpected replay callback"); return nil }); err == nil {
		t.Fatal("oversized record accepted")
	}
}

func TestRecoverRejectsOversizedRecordBeforeAllocation(t *testing.T) {
	p := filepath.Join(t.TempDir(), "journal")
	j, err := Open(p, 1<<40)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[:4], maxJournalRecordBytes+1)
	if _, err := j.f.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	if err := j.f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := j.Recover(func([]byte) error {
		t.Fatal("unexpected recover callback")
		return nil
	}); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("recover error = %v, want ErrRecordTooLarge", err)
	}
}

func TestAppendRejectsOversizedRecord(t *testing.T) {
	j, err := Open(filepath.Join(t.TempDir(), "journal"), 1<<40)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err := j.Append(make([]byte, maxJournalRecordBytes+1)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("append error = %v, want ErrRecordTooLarge", err)
	}
}

func TestJournalProcessCrashRecovery(t *testing.T) {
	if os.Getenv("CORAL_JOURNAL_CRASH_HELPER") == "1" {
		j, err := Open(os.Getenv("CORAL_JOURNAL_CRASH_PATH"), 1024)
		if err != nil {
			os.Exit(2)
		}
		_ = j.Append(EncodeEnvelope(Envelope{Signal: "traces", Tenant: "t", Payload: []byte("crash")}))
		os.Exit(0)
	}
	p := filepath.Join(t.TempDir(), "crash.log")
	cmd := exec.Command(os.Args[0], "-test.run=TestJournalProcessCrashRecovery")
	cmd.Env = append(os.Environ(), "CORAL_JOURNAL_CRASH_HELPER=1", "CORAL_JOURNAL_CRASH_PATH="+p)
	if err := cmd.Run(); err != nil {
		t.Fatalf("helper: %v", err)
	}
	j, err := Open(p, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	var count int
	if err = j.Recover(func(raw []byte) error { count++; _, err := DecodeEnvelope(raw); return err }); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("recovered=%d", count)
	}
}

func TestJournalAppendReplayAndReopen(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.log")
	j, err := Open(p, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Append([]byte("one")); err != nil {
		t.Fatal(err)
	}
	if err = j.Append([]byte("two")); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	j, err = Open(p, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	var got []string
	if err = j.Replay(func(b []byte) error { got = append(got, string(b)); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("got %#v", got)
	}
}

func TestJournalCompactOlderThan(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.log")
	j, err := Open(p, 2048)
	if err != nil {
		t.Fatal(err)
	}
	old := EncodeEnvelope(Envelope{Signal: "logs", CreatedUnixNano: time.Now().Add(-time.Hour).UnixNano(), Payload: []byte("old")})
	fresh := EncodeEnvelope(Envelope{Signal: "logs", CreatedUnixNano: time.Now().UnixNano(), Payload: []byte("fresh")})
	if err = j.Append(old); err != nil {
		t.Fatal(err)
	}
	if err = j.Append(fresh); err != nil {
		t.Fatal(err)
	}
	if err = j.CompactOlderThan(time.Minute); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	j, err = Open(p, 2048)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	var n int
	if err = j.Replay(func(b []byte) error {
		n++
		e, _ := DecodeEnvelope(b)
		if string(e.Payload) != "fresh" {
			t.Fatalf("payload=%q", e.Payload)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("records=%d", n)
	}
}

func TestJournalFsyncFailureIsNotSuccess(t *testing.T) {
	j, err := Open(filepath.Join(t.TempDir(), "j.log"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	j.syncFn = func() error { return os.ErrPermission }
	if err = j.Append([]byte("x")); err == nil {
		t.Fatal("append succeeded despite fsync failure")
	}
	bytes, _ := j.Stats()
	if bytes != 0 {
		t.Fatalf("size=%d after failed fsync", bytes)
	}
	_ = j.Close()
}

func TestJournalCompactFsyncFailure(t *testing.T) {
	j, err := Open(filepath.Join(t.TempDir(), "j.log"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Append([]byte("x")); err != nil {
		t.Fatal(err)
	}
	j.syncFn = func() error { return os.ErrPermission }
	if err = j.Compact(); err == nil {
		t.Fatal("compact succeeded despite fsync failure")
	}
	bytes, _ := j.Stats()
	if bytes == 0 {
		t.Fatal("compact erased records before fsync")
	}
	_ = j.Close()
}

func TestJournalCapacityBoundUnderRepeatedAppend(t *testing.T) {
	const capBytes int64 = 4096
	j, err := Open(filepath.Join(t.TempDir(), "j.log"), capBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	for i := 0; i < 10000; i++ {
		err = j.Append([]byte("record"))
		if err == ErrFull {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	bytes, max := j.Stats()
	if bytes > max || max != capBytes {
		t.Fatalf("bytes=%d max=%d", bytes, max)
	}
	if err != ErrFull {
		t.Fatal("capacity was not enforced")
	}
}

func TestJournalRejectsCorruptionAndFull(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.log")
	j, err := Open(p, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Append([]byte("12345678")); err != nil {
		t.Fatal(err)
	}
	if err = j.Append([]byte("x")); err != ErrFull {
		t.Fatalf("err=%v", err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(p, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt([]byte{0xff}, 8); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	j, err = Open(p, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err = j.Replay(func([]byte) error { return nil }); err == nil {
		t.Fatal("corruption accepted")
	}
}

func TestJournalRecoverTruncatedTail(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.log")
	j, err := Open(p, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Append([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{0, 0, 0, 4, 0, 0, 0, 0, 1})
	_ = f.Close()
	j, err = Open(p, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	var n int
	if err = j.Recover(func([]byte) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("replayed=%d", n)
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	want := Envelope{Signal: "traces", Tenant: "org/project", Payload: []byte("payload")}
	got, err := DecodeEnvelope(EncodeEnvelope(want))
	if err != nil {
		t.Fatal(err)
	}
	if got.Signal != want.Signal || got.Tenant != want.Tenant || string(got.Payload) != string(want.Payload) {
		t.Fatalf("got %#v", got)
	}
}

func TestEnvelopeDeliveryIDRoundTrip(t *testing.T) {
	want := Envelope{
		Signal: "logs", Tenant: "tenant-a",
		DeliveryID:    "0123456789abcdef0123456789abcdef",
		RecordID:      "fedcba9876543210fedcba9876543210",
		RequestDigest: "abc123", FailureReason: "permanent rejection",
		CreatedUnixNano: 42, QuarantinedUnixNano: 84, Payload: []byte("body"),
	}
	got, err := DecodeEnvelope(EncodeEnvelope(want))
	if err != nil {
		t.Fatal(err)
	}
	if got.DeliveryID != want.DeliveryID || got.RecordID != want.RecordID || got.RequestDigest != want.RequestDigest || got.FailureReason != want.FailureReason || got.QuarantinedUnixNano != want.QuarantinedUnixNano || got.Signal != want.Signal || got.Tenant != want.Tenant || string(got.Payload) != string(want.Payload) {
		t.Fatalf("round trip = %+v", got)
	}
}

func TestEncodeEnvelopeRejectsOversizedRoutingFields(t *testing.T) {
	if got := EncodeEnvelope(Envelope{Signal: string(make([]byte, 256))}); got != nil {
		t.Fatal("oversized signal encoded")
	}
	if got := EncodeEnvelope(Envelope{Tenant: string(make([]byte, 256))}); got != nil {
		t.Fatal("oversized tenant encoded")
	}
	if got := EncodeEnvelope(Envelope{RecordID: string(make([]byte, 256))}); got != nil {
		t.Fatal("oversized record id encoded")
	}
	if got := EncodeEnvelope(Envelope{FailureReason: string(make([]byte, 4097))}); got != nil {
		t.Fatal("oversized failure reason encoded")
	}
}

func TestRecordStatsAndLookupEnvelope(t *testing.T) {
	j, err := Open(filepath.Join(t.TempDir(), "j.log"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	old := time.Now().Add(-time.Minute).Truncate(time.Nanosecond)
	first, err := j.AppendEnvelope(Envelope{Signal: "traces", CreatedUnixNano: old.UnixNano(), Payload: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.AppendEnvelope(Envelope{Signal: "logs", Payload: []byte("two")}); err != nil {
		t.Fatal(err)
	}
	records, oldest, err := j.RecordStats()
	if err != nil {
		t.Fatal(err)
	}
	if records != 2 || !oldest.Equal(old) {
		t.Fatalf("record stats = (%d, %s), want (2, %s)", records, oldest, old)
	}
	got, found, err := j.LookupEnvelope(first.RecordID)
	if err != nil || !found || string(got.Payload) != "one" {
		t.Fatalf("lookup = (%+v, %t, %v)", got, found, err)
	}
}

func TestAppendEnvelopeAssignsStableRecordID(t *testing.T) {
	j, err := Open(filepath.Join(t.TempDir(), "j.log"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	stored, err := j.AppendEnvelope(Envelope{Signal: "traces", Payload: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.RecordID) != 32 {
		t.Fatalf("record id = %q", stored.RecordID)
	}
	if stored.CreatedUnixNano == 0 {
		t.Fatal("created timestamp was not assigned")
	}

	var replayed Envelope
	if err := j.Replay(func(raw []byte) error {
		replayed, err = DecodeEnvelope(raw)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if replayed.RecordID != stored.RecordID {
		t.Fatalf("replayed record id = %q, want %q", replayed.RecordID, stored.RecordID)
	}
}

func TestAcknowledgeRetainsOnlyUnconfirmedRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j.log")
	j, err := Open(path, 4096)
	if err != nil {
		t.Fatal(err)
	}
	one, err := j.AppendEnvelope(Envelope{Signal: "traces", Payload: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}
	two, err := j.AppendEnvelope(Envelope{Signal: "logs", Payload: []byte("two")})
	if err != nil {
		t.Fatal(err)
	}
	three, err := j.AppendEnvelope(Envelope{Signal: "metrics", Payload: []byte("three")})
	if err != nil {
		t.Fatal(err)
	}

	removed, err := j.Acknowledge(two.RecordID)
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatal("existing record was not acknowledged")
	}
	if removed, err = j.Acknowledge(two.RecordID); err != nil || removed {
		t.Fatalf("idempotent acknowledge = (%t, %v)", removed, err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	j, err = Open(path, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	var ids []string
	if err := j.Replay(func(raw []byte) error {
		env, err := DecodeEnvelope(raw)
		if err == nil {
			ids = append(ids, env.RecordID)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != one.RecordID || ids[1] != three.RecordID {
		t.Fatalf("retained ids = %#v", ids)
	}
}

func TestAcknowledgeFsyncFailureRetainsRecord(t *testing.T) {
	j, err := Open(filepath.Join(t.TempDir(), "j.log"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	stored, err := j.AppendEnvelope(Envelope{Signal: "traces", Payload: []byte("keep")})
	if err != nil {
		t.Fatal(err)
	}
	j.syncFn = func() error { return os.ErrPermission }
	if removed, err := j.Acknowledge(stored.RecordID); err == nil || removed {
		t.Fatalf("acknowledge despite fsync failure = (%t, %v)", removed, err)
	}
	j.syncFn = j.f.Sync
	var count int
	if err := j.Replay(func([]byte) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("records after failed acknowledge = %d", count)
	}
}

func TestEnsureRecordIDsMigratesLegacyRecordsOnce(t *testing.T) {
	j, err := Open(filepath.Join(t.TempDir(), "j.log"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err := j.Append(EncodeEnvelope(Envelope{Signal: "traces", Payload: []byte("legacy")})); err != nil {
		t.Fatal(err)
	}
	if err := j.EnsureRecordIDs(); err != nil {
		t.Fatal(err)
	}
	var first string
	if err := j.Replay(func(raw []byte) error {
		env, err := DecodeEnvelope(raw)
		first = env.RecordID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(first) != 32 {
		t.Fatalf("migrated record id = %q", first)
	}
	bytesBefore, _ := j.Stats()
	if err := j.EnsureRecordIDs(); err != nil {
		t.Fatal(err)
	}
	bytesAfter, _ := j.Stats()
	if bytesAfter != bytesBefore {
		t.Fatalf("second migration changed size: %d -> %d", bytesBefore, bytesAfter)
	}
	var second string
	if err := j.Replay(func(raw []byte) error {
		env, err := DecodeEnvelope(raw)
		second = env.RecordID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Fatalf("record id changed: %q -> %q", first, second)
	}
}

func TestEnvelopeJournalRecovery(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.log")
	j, err := Open(p, 1024)
	if err != nil {
		t.Fatal(err)
	}
	want := Envelope{Signal: "logs", Tenant: "org/project", Payload: []byte("otlp")}
	if err = j.Append(EncodeEnvelope(want)); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	j, err = Open(p, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	var got Envelope
	if err = j.Recover(func(raw []byte) error { got, err = DecodeEnvelope(raw); return err }); err != nil {
		t.Fatal(err)
	}
	if got.Signal != want.Signal || got.Tenant != want.Tenant || string(got.Payload) != "otlp" {
		t.Fatalf("got %#v", got)
	}
}

func TestJournalCompactClearsRecords(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.log")
	j, err := Open(p, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Append([]byte("done")); err != nil {
		t.Fatal(err)
	}
	if err = j.Compact(); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	j, err = Open(p, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	var count int
	if err = j.Replay(func([]byte) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("replayed %d compacted records", count)
	}
}
