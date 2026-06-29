package metric

import (
	"context"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func attrVal(attrs []*commonpb.KeyValue, key string) (string, bool) {
	for _, kv := range attrs {
		if kv.Key == key {
			return kv.Value.GetStringValue(), true
		}
	}
	return "", false
}

func TestAttributesActions(t *testing.T) {
	rm := &metricspb.ResourceMetrics{Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
		stringKV("keep", "1"),
		stringKV("drop", "x"),
		stringKV("existing", "old"),
	}}}

	p := NewAttributesProcessor([]AttributeAction{
		{Action: "upsert", Key: "existing", Value: "new"},
		{Action: "upsert", Key: "added", Value: "v"},
		{Action: "insert", Key: "keep", Value: "MUST_NOT_OVERWRITE"},
		{Action: "delete", Key: "drop"},
	})

	out, err := p.Process(context.Background(), Batch{ResourceMetrics: []*metricspb.ResourceMetrics{rm}})
	if err != nil {
		t.Fatal(err)
	}
	attrs := out.ResourceMetrics[0].Resource.Attributes

	if v, _ := attrVal(attrs, "existing"); v != "new" {
		t.Errorf("upsert existing = %q, want new", v)
	}
	if v, ok := attrVal(attrs, "added"); !ok || v != "v" {
		t.Errorf("upsert added = %q (ok=%v), want v", v, ok)
	}
	if v, _ := attrVal(attrs, "keep"); v != "1" {
		t.Errorf("insert must not overwrite: keep = %q, want 1", v)
	}
	if _, ok := attrVal(attrs, "drop"); ok {
		t.Error("delete should have removed 'drop'")
	}
}

func TestAttributesNilResource(t *testing.T) {
	rm := &metricspb.ResourceMetrics{} // no Resource
	p := NewAttributesProcessor([]AttributeAction{{Action: "upsert", Key: "k", Value: "v"}})
	out, _ := p.Process(context.Background(), Batch{ResourceMetrics: []*metricspb.ResourceMetrics{rm}})
	if v, ok := attrVal(out.ResourceMetrics[0].Resource.GetAttributes(), "k"); !ok || v != "v" {
		t.Errorf("upsert onto nil resource failed: %q ok=%v", v, ok)
	}
}
