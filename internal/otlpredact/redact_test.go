package otlpredact

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

func kv(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func TestRedactKeyValues_MatchesKeyOrValue(t *testing.T) {
	r, err := New([]string{`(?i)password`, `secret-\d+`})
	if err != nil {
		t.Fatal(err)
	}
	attrs := []*commonpb.KeyValue{
		kv("password", "hunter2"), // key match
		kv("note", "secret-42"),   // value match
		kv("region", "us-east"),   // benign
		nil,                       // must be skipped, not panic
	}
	if n := r.RedactKeyValues(attrs); n != 2 {
		t.Fatalf("redacted count = %d, want 2", n)
	}
	if attrs[0].GetValue().GetStringValue() != RedactedValue {
		t.Error("key-matched attr not redacted")
	}
	if attrs[1].GetValue().GetStringValue() != RedactedValue {
		t.Error("value-matched attr not redacted")
	}
	if attrs[2].GetValue().GetStringValue() != "us-east" {
		t.Error("benign attr changed")
	}
}

func TestRedactor_InvalidPattern(t *testing.T) {
	if _, err := New([]string{"("}); err == nil {
		t.Fatal("expected error for invalid regexp")
	}
}

func TestRedactor_DisabledWhenNoPatterns(t *testing.T) {
	r, _ := New(nil)
	if r.Enabled() {
		t.Error("Enabled() should be false with no patterns")
	}
}
