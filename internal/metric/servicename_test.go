package metric

import (
	"context"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/yaop-labs/coral/internal/otlpresource"
)

func TestServiceNameProcessor_StampsMissingResources(t *testing.T) {
	proc := NewServiceNameProcessor()
	b := Batch{ResourceMetrics: []*metricspb.ResourceMetrics{
		{Resource: nil},
		{Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringKV("service.name", "app")}}},
	}}
	if _, err := proc.Process(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if v, _ := attrVal(b.ResourceMetrics[0].Resource.GetAttributes(), "service.name"); v != otlpresource.DefaultServiceName {
		t.Errorf("nil resource: service.name = %q, want %q", v, otlpresource.DefaultServiceName)
	}
	if v, _ := attrVal(b.ResourceMetrics[1].Resource.GetAttributes(), "service.name"); v != "app" {
		t.Errorf("existing service.name overwritten: %q", v)
	}
}
