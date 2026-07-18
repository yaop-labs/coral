package otlp

import "testing"

func TestParseWispHeadersOptional(t *testing.T) {
	if _, err := parseWispHeaders("", ""); err != nil {
		t.Fatal(err)
	}
}

func TestParseWispHeadersStrict(t *testing.T) {
	if _, err := parseWispHeaders("bad", "traces"); err == nil {
		t.Fatal("accepted malformed id")
	}
	if _, err := parseWispHeaders("00112233445566778899aabbccddeeff", "unknown"); err == nil {
		t.Fatal("accepted unknown signal")
	}
	id, err := parseWispHeaders("00112233445566778899aabbccddeeff", "TRACES")
	if err != nil || id.SignalKind != "traces" {
		t.Fatalf("identity = %#v, err=%v", id, err)
	}
}
