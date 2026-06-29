package metric

import (
	"context"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// AttributeAction is one enrichment step on a resource attribute.
// Action is "upsert", "insert", or "delete".
type AttributeAction struct {
	Action string
	Key    string
	Value  string
}

// AttributesProcessor enriches each ResourceMetrics' resource attributes — the
// place coral stamps k8s/cloud/owner metadata onto incoming agent metrics.
type AttributesProcessor struct {
	actions []AttributeAction
}

func NewAttributesProcessor(actions []AttributeAction) *AttributesProcessor {
	return &AttributesProcessor{actions: actions}
}

func (p *AttributesProcessor) Process(_ context.Context, b Batch) (Batch, error) {
	for _, rm := range b.ResourceMetrics {
		if rm.Resource == nil {
			rm.Resource = &resourcepb.Resource{}
		}
		for _, a := range p.actions {
			rm.Resource.Attributes = applyAction(rm.Resource.Attributes, a)
		}
	}
	return b, nil
}

func (p *AttributesProcessor) Close() error { return nil }

func applyAction(attrs []*commonpb.KeyValue, a AttributeAction) []*commonpb.KeyValue {
	switch a.Action {
	case "delete":
		out := attrs[:0:0]
		for _, kv := range attrs {
			if kv.Key != a.Key {
				out = append(out, kv)
			}
		}
		return out
	case "insert":
		for _, kv := range attrs {
			if kv.Key == a.Key {
				return attrs // already present
			}
		}
		return append(attrs, stringKV(a.Key, a.Value))
	case "upsert":
		for _, kv := range attrs {
			if kv.Key == a.Key {
				kv.Value = stringValue(a.Value)
				return attrs
			}
		}
		return append(attrs, stringKV(a.Key, a.Value))
	default:
		return attrs
	}
}

func stringKV(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: stringValue(value)}
}

func stringValue(v string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}
}
