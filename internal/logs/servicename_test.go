package logs

import (
	"context"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/yaop-labs/coral/internal/otlpresource"
)

func TestServiceNameProcessor_StampsMissingResources(t *testing.T) {
	proc := NewServiceNameProcessor()
	b := Batch{ResourceLogs: []*logspb.ResourceLogs{
		{Resource: nil},
		{Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringKV("service.name", "checkout")}}},
	}}
	if _, err := proc.Process(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if v := attrVal(b.ResourceLogs[0].Resource.GetAttributes(), "service.name"); v != otlpresource.DefaultServiceName {
		t.Errorf("nil resource: service.name = %q, want %q", v, otlpresource.DefaultServiceName)
	}
	if v := attrVal(b.ResourceLogs[1].Resource.GetAttributes(), "service.name"); v != "checkout" {
		t.Errorf("existing service.name overwritten: %q", v)
	}
}
