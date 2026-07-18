package journal

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

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
