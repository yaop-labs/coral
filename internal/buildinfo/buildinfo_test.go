package buildinfo

import (
	"strings"
	"testing"
)

func TestInfoString(t *testing.T) {
	got := (Info{
		Version:   "v0.2.0",
		Revision:  "abc123",
		Modified:  true,
		GoVersion: "go1.26.3",
	}).String()

	for _, want := range []string{
		"coral version=v0.2.0",
		"revision=abc123, modified",
		"go=go1.26.3",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("String() = %q, missing %q", got, want)
		}
	}
}

func TestInfoStringNormalizesEmptyValues(t *testing.T) {
	got := (Info{}).String()
	if got != "coral version=dev revision=unknown go=unknown" {
		t.Fatalf("String() = %q", got)
	}
}

func TestCurrentHasGoVersion(t *testing.T) {
	if got := Current().GoVersion; got == "" {
		t.Fatal("Current().GoVersion is empty")
	}
}
