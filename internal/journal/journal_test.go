package journal

import (
	"os"
	"path/filepath"
	"testing"
)

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
