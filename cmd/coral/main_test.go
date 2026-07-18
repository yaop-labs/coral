package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunVersionDoesNotRequireConfig(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"--version"}, &out); err != nil {
		t.Fatalf("run --version: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "coral version=") {
		t.Fatalf("version output = %q", got)
	}
}

func TestRunRequiresConfig(t *testing.T) {
	if err := run(nil, &bytes.Buffer{}); err == nil {
		t.Fatal("run without --config succeeded")
	}
}

func TestRunHelp(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"--help"}, &out); err != nil {
		t.Fatalf("run --help: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "-config") || !strings.Contains(got, "-version") {
		t.Fatalf("help output = %q", got)
	}
}
