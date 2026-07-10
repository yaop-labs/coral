package otlpresource

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func svcName(res *resourcepb.Resource) string {
	for _, kv := range res.GetAttributes() {
		if kv.GetKey() == "service.name" {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

func TestEnsureServiceName(t *testing.T) {
	t.Run("nil resource", func(t *testing.T) {
		res := EnsureServiceName(nil)
		if svcName(res) != DefaultServiceName {
			t.Errorf("got %q, want %q", svcName(res), DefaultServiceName)
		}
	})
	t.Run("missing attribute", func(t *testing.T) {
		res := EnsureServiceName(&resourcepb.Resource{})
		if svcName(res) != DefaultServiceName {
			t.Errorf("got %q, want %q", svcName(res), DefaultServiceName)
		}
	})
	t.Run("empty value filled", func(t *testing.T) {
		res := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			{Key: "service.name", Value: stringValue("")},
		}}
		res = EnsureServiceName(res)
		if svcName(res) != DefaultServiceName {
			t.Errorf("got %q, want %q", svcName(res), DefaultServiceName)
		}
		if n := len(res.Attributes); n != 1 {
			t.Errorf("attribute count = %d, want 1 (no duplicate)", n)
		}
	})
	t.Run("present value preserved", func(t *testing.T) {
		res := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			{Key: "service.name", Value: stringValue("checkout")},
		}}
		res = EnsureServiceName(res)
		if svcName(res) != "checkout" {
			t.Errorf("got %q, want checkout", svcName(res))
		}
	})
}
