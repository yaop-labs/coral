// Package otlpresource holds shared helpers for normalizing OTLP resources.
package otlpresource

import (
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// DefaultServiceName labels a resource that arrived without a service.name.
const DefaultServiceName = "unknown_service"

// EnsureServiceName guarantees res carries a non-empty service.name (contract
// §6): amber aggregation and fathom matching depend on it, and coral is the
// enforcement point. It returns res, or a freshly allocated resource when res
// is nil, filling in a missing or empty service.name.
func EnsureServiceName(res *resourcepb.Resource) *resourcepb.Resource {
	if res == nil {
		res = &resourcepb.Resource{}
	}
	for _, kv := range res.Attributes {
		if kv.GetKey() != "service.name" {
			continue
		}
		if kv.GetValue().GetStringValue() == "" {
			kv.Value = stringValue(DefaultServiceName)
		}
		return res
	}
	res.Attributes = append(res.Attributes, &commonpb.KeyValue{
		Key:   "service.name",
		Value: stringValue(DefaultServiceName),
	})
	return res
}

func stringValue(v string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}
}
