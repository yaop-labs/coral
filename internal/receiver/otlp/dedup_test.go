package otlp

import (
	"testing"
	"time"
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
