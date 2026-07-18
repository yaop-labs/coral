package fathom

import (
	"fmt"
	"sort"
	"strings"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/yaop-labs/coral/internal/model"
)

const scopeName = "coral"

func toTraceRequest(b model.Batch) *coltracepb.ExportTraceServiceRequest {
	groups := make(map[string]*tracepb.ResourceSpans)
	var order []string
	for i := range b.Spans {
		s := &b.Spans[i]
		key := resourceKey(s.Resource) + "\x00" + s.ScopeName + "\x00" + s.ScopeVersion + "\x00" + s.SchemaURL
		rs := groups[key]
		if rs == nil {
			rs = &tracepb.ResourceSpans{
				Resource:   &resourcepb.Resource{Attributes: kvFromAttrs(s.Resource.Attrs)},
				ScopeSpans: []*tracepb.ScopeSpans{{Scope: &commonpb.InstrumentationScope{Name: firstNonEmpty(s.ScopeName, scopeName), Version: s.ScopeVersion}, SchemaUrl: s.SchemaURL}},
			}
			groups[key] = rs
			order = append(order, key)
		}
		rs.ScopeSpans[0].Spans = append(rs.ScopeSpans[0].Spans, spanToProto(s))
	}
	req := &coltracepb.ExportTraceServiceRequest{}
	for _, key := range order {
		req.ResourceSpans = append(req.ResourceSpans, groups[key])
	}
	return req
}

func firstNonEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func spanToProto(s *model.Span) *tracepb.Span {
	sp := &tracepb.Span{
		TraceId:                append([]byte(nil), s.TraceID[:]...),
		SpanId:                 append([]byte(nil), s.SpanID[:]...),
		Name:                   s.Name,
		Kind:                   kindToProto(s.Kind),
		StartTimeUnixNano:      timeToNanos(s.StartTime),
		EndTimeUnixNano:        timeToNanos(s.EndTime),
		Attributes:             kvFromAttrs(s.Attrs),
		Status:                 &tracepb.Status{Code: statusToProto(s.Status), Message: s.StatusMsg},
		Flags:                  s.TraceFlags,
		DroppedAttributesCount: s.DroppedAttributes,
		DroppedEventsCount:     s.DroppedEvents,
		DroppedLinksCount:      s.DroppedLinks,
	}
	if !s.ParentSpanID.IsZero() {
		sp.ParentSpanId = append([]byte(nil), s.ParentSpanID[:]...)
	}
	return sp
}

func kindToProto(kind model.SpanKind) tracepb.Span_SpanKind {
	switch kind {
	case model.KindInternal:
		return tracepb.Span_SPAN_KIND_INTERNAL
	case model.KindServer:
		return tracepb.Span_SPAN_KIND_SERVER
	case model.KindClient:
		return tracepb.Span_SPAN_KIND_CLIENT
	case model.KindProducer:
		return tracepb.Span_SPAN_KIND_PRODUCER
	case model.KindConsumer:
		return tracepb.Span_SPAN_KIND_CONSUMER
	default:
		return tracepb.Span_SPAN_KIND_UNSPECIFIED
	}
}

func statusToProto(status model.SpanStatus) tracepb.Status_StatusCode {
	switch status {
	case model.StatusOK:
		return tracepb.Status_STATUS_CODE_OK
	case model.StatusError:
		return tracepb.Status_STATUS_CODE_ERROR
	default:
		return tracepb.Status_STATUS_CODE_UNSET
	}
}

func timeToNanos(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixNano())
}

func kvFromAttrs(attrs []model.Attribute) []*commonpb.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]*commonpb.KeyValue, len(attrs))
	for i, attr := range attrs {
		out[i] = &commonpb.KeyValue{Key: attr.Key, Value: anyFromGo(attr.Value.Interface())}
	}
	return out
}

func anyFromGo(value any) *commonpb.AnyValue {
	switch typed := value.(type) {
	case nil:
		return &commonpb.AnyValue{}
	case string:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: typed}}
	case bool:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: typed}}
	case int64:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: typed}}
	case float64:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: typed}}
	case []byte:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: typed}}
	case []any:
		values := make([]*commonpb.AnyValue, len(typed))
		for i, item := range typed {
			values[i] = anyFromGo(item)
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: values}}}
	case map[string]any:
		kvs := make([]*commonpb.KeyValue, 0, len(typed))
		for key, item := range typed {
			kvs = append(kvs, &commonpb.KeyValue{Key: key, Value: anyFromGo(item)})
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{Values: kvs}}}
	default:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: fmt.Sprint(typed)}}
	}
}

func resourceKey(resource model.Resource) string {
	attrs := append([]model.Attribute(nil), resource.Attrs...)
	sort.Slice(attrs, func(i, j int) bool { return attrs[i].Key < attrs[j].Key })
	var b strings.Builder
	for _, attr := range attrs {
		b.WriteString(attr.Key)
		b.WriteByte('=')
		b.WriteString(attr.Value.String())
		b.WriteByte('\n')
	}
	return b.String()
}
