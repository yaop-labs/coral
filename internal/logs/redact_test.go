package logs

import (
	"context"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/yaop-labs/coral/internal/otlpredact"
)

func attrVal(attrs []*commonpb.KeyValue, key string) string {
	for _, kv := range attrs {
		if kv.GetKey() == key {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

func TestRedactProcessor_ScrubsAttributesAndBody(t *testing.T) {
	proc, err := NewRedactProcessor([]string{`(?i)authorization|password|token`})
	if err != nil {
		t.Fatal(err)
	}

	b := Batch{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			stringKV("service.name", "checkout"),
			stringKV("token", "abc123"),
		}},
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
			Attributes: []*commonpb.KeyValue{
				stringKV("http.method", "POST"),
				stringKV("authorization", "Bearer xyz"),
			},
			Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "password=hunter2"}},
		}}}},
	}}}

	if _, err := proc.Process(context.Background(), b); err != nil {
		t.Fatalf("Process: %v", err)
	}

	res := b.ResourceLogs[0].Resource.Attributes
	if attrVal(res, "service.name") != "checkout" {
		t.Error("benign resource attr changed")
	}
	if attrVal(res, "token") != otlpredact.RedactedValue {
		t.Error("token resource attr not redacted")
	}

	rec := b.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	if attrVal(rec.Attributes, "http.method") != "POST" {
		t.Error("benign record attr changed")
	}
	if attrVal(rec.Attributes, "authorization") != otlpredact.RedactedValue {
		t.Error("authorization record attr not redacted")
	}
	if got := rec.GetBody().GetStringValue(); got != otlpredact.RedactedValue {
		t.Errorf("secret log body not redacted: %q", got)
	}
}
