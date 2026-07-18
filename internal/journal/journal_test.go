package journal

import (
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
