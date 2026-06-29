package otlp

import (
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/yaop-labs/coral/internal/model"
)

func spansFromResourceSpans(rs []*tracepb.ResourceSpans) []model.Span {
	var out []model.Span
	for _, r := range rs {
		res := resourceFromProto(r.GetResource())
		for _, ss := range r.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				out = append(out, spanFromProto(sp, res))
			}
		}
	}
	return out
}

func spanFromProto(s *tracepb.Span, res model.Resource) model.Span {
	return model.Span{
		TraceID:      bytesToTraceID(s.GetTraceId()),
		SpanID:       bytesToSpanID(s.GetSpanId()),
		ParentSpanID: bytesToSpanID(s.GetParentSpanId()),
		Resource:     res,
		Name:         s.GetName(),
		Kind:         kindFromProto(s.GetKind()),
		StartTime:    nanosToTime(s.GetStartTimeUnixNano()),
		EndTime:      nanosToTime(s.GetEndTimeUnixNano()),
		Status:       statusFromProto(s.GetStatus()),
		StatusMsg:    s.GetStatus().GetMessage(),
		Attrs:        attrsFromKV(s.GetAttributes()),
	}
}

func resourceFromProto(r *resourcepb.Resource) model.Resource {
	return model.Resource{Attrs: attrsFromKV(r.GetAttributes())}
}

func kindFromProto(k tracepb.Span_SpanKind) model.SpanKind {
	switch k {
	case tracepb.Span_SPAN_KIND_INTERNAL:
		return model.KindInternal
	case tracepb.Span_SPAN_KIND_SERVER:
		return model.KindServer
	case tracepb.Span_SPAN_KIND_CLIENT:
		return model.KindClient
	case tracepb.Span_SPAN_KIND_PRODUCER:
		return model.KindProducer
	case tracepb.Span_SPAN_KIND_CONSUMER:
		return model.KindConsumer
	default:
		return model.KindUnspecified
	}
}

func statusFromProto(st *tracepb.Status) model.SpanStatus {
	switch st.GetCode() {
	case tracepb.Status_STATUS_CODE_OK:
		return model.StatusOK
	case tracepb.Status_STATUS_CODE_ERROR:
		return model.StatusError
	default:
		return model.StatusUnset
	}
}

func bytesToTraceID(b []byte) model.TraceID {
	var id model.TraceID
	copy(id[:], b)
	return id
}

func bytesToSpanID(b []byte) model.SpanID {
	var id model.SpanID
	copy(id[:], b)
	return id
}

func nanosToTime(ns uint64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(ns))
}

func attrsFromKV(kvs []*commonpb.KeyValue) []model.Attribute {
	if len(kvs) == 0 {
		return nil
	}
	out := make([]model.Attribute, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, model.Attribute{
			Key:   kv.GetKey(),
			Value: anyValue(kv.GetValue()),
		})
	}
	return out
}

func anyValue(v *commonpb.AnyValue) model.AttributeValue {
	if v == nil {
		return model.StringValue("")
	}
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return model.StringValue(x.StringValue)
	case *commonpb.AnyValue_BoolValue:
		return model.BoolValue(x.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return model.IntValue(x.IntValue)
	case *commonpb.AnyValue_DoubleValue:
		return model.DoubleValue(x.DoubleValue)
	case *commonpb.AnyValue_BytesValue:
		return model.BytesValue(x.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		items := x.ArrayValue.GetValues()
		out := make([]model.AttributeValue, 0, len(items))
		for _, item := range items {
			out = append(out, anyValue(item))
		}
		return model.ArrayValue(out)
	case *commonpb.AnyValue_KvlistValue:
		items := x.KvlistValue.GetValues()
		out := make([]model.Attribute, 0, len(items))
		for _, item := range items {
			out = append(out, model.Attribute{Key: item.GetKey(), Value: anyValue(item.GetValue())})
		}
		return model.MapValue(out)
	default:
		return model.StringValue("")
	}
}
