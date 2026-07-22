package amber

import (
	"fmt"
	"sort"
	"strings"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/yaop-labs/coral/internal/model"
)

const scopeName = "coral"

// toTraceRequest converts a span batch into an OTLP ExportTraceServiceRequest,
// grouping spans that share a resource into one ResourceSpans.
func toTraceRequest(b model.Batch) *coltracepb.ExportTraceServiceRequest {
	groups := make(map[string]*tracepb.ResourceSpans)
	var order []string
	for i := range b.Spans {
		s := &b.Spans[i]
		scopeSchema := firstNonEmpty(s.ScopeSchemaURL, s.SchemaURL)
		key := resourceKey(s.Resource) + "\x00" + s.ResourceSchemaURL + "\x00" +
			s.ScopeName + "\x00" + s.ScopeVersion + "\x00" + scopeSchema + "\x00" +
			attributesKey(s.ScopeAttributes)
		rs := groups[key]
		if rs == nil {
			rs = &tracepb.ResourceSpans{
				Resource: &resourcepb.Resource{
					Attributes:             kvFromAttrs(s.Resource.Attrs),
					DroppedAttributesCount: s.Resource.DroppedAttributes,
				},
				SchemaUrl: s.ResourceSchemaURL,
				ScopeSpans: []*tracepb.ScopeSpans{{
					Scope: &commonpb.InstrumentationScope{
						Name:                   firstNonEmpty(s.ScopeName, scopeName),
						Version:                s.ScopeVersion,
						Attributes:             kvFromAttrs(s.ScopeAttributes),
						DroppedAttributesCount: s.ScopeDroppedAttrs,
					},
					SchemaUrl: scopeSchema,
				}},
			}
			groups[key] = rs
			order = append(order, key)
		}
		rs.ScopeSpans[0].Spans = append(rs.ScopeSpans[0].Spans, spanToProto(s))
	}
	req := &coltracepb.ExportTraceServiceRequest{}
	for _, k := range order {
		req.ResourceSpans = append(req.ResourceSpans, groups[k])
	}
	return req
}

// TraceRequest returns the canonical OTLP representation of an admitted trace
// batch. Ingress uses the same lossless conversion for durable post-admission
// journal records that the Amber exporter uses on live delivery.
func TraceRequest(b model.Batch) *coltracepb.ExportTraceServiceRequest {
	return toTraceRequest(b)
}

func spanToProto(s *model.Span) *tracepb.Span {
	sp := &tracepb.Span{}
	if len(s.OTLP) > 0 {
		_ = proto.Unmarshal(s.OTLP, sp)
	}
	sp.TraceId = append([]byte(nil), s.TraceID[:]...)
	sp.SpanId = append([]byte(nil), s.SpanID[:]...)
	sp.Name = s.Name
	sp.Kind = kindToProto(s.Kind)
	sp.StartTimeUnixNano = timeToNanos(s.StartTime)
	sp.EndTimeUnixNano = timeToNanos(s.EndTime)
	sp.Attributes = kvFromAttrs(s.Attrs)
	sp.Status = &tracepb.Status{Code: statusToProto(s.Status), Message: s.StatusMsg}
	sp.Flags = s.TraceFlags
	sp.DroppedAttributesCount = s.DroppedAttributes
	sp.DroppedEventsCount = s.DroppedEvents
	sp.DroppedLinksCount = s.DroppedLinks
	if !s.ParentSpanID.IsZero() {
		sp.ParentSpanId = append([]byte(nil), s.ParentSpanID[:]...)
	} else {
		sp.ParentSpanId = nil
	}
	return sp
}

func firstNonEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func kindToProto(k model.SpanKind) tracepb.Span_SpanKind {
	switch k {
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

func statusToProto(s model.SpanStatus) tracepb.Status_StatusCode {
	switch s {
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
	for i, a := range attrs {
		out[i] = &commonpb.KeyValue{Key: a.Key, Value: anyFromGo(a.Value.Interface())}
	}
	return out
}

func anyFromGo(x any) *commonpb.AnyValue {
	switch t := x.(type) {
	case nil:
		return &commonpb.AnyValue{}
	case string:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: t}}
	case bool:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: t}}
	case int64:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: t}}
	case float64:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: t}}
	case []byte:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: t}}
	case []any:
		vals := make([]*commonpb.AnyValue, len(t))
		for i, item := range t {
			vals[i] = anyFromGo(item)
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: vals}}}
	case map[string]any:
		kvs := make([]*commonpb.KeyValue, 0, len(t))
		for k, v := range t {
			kvs = append(kvs, &commonpb.KeyValue{Key: k, Value: anyFromGo(v)})
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{Values: kvs}}}
	default:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: fmt.Sprint(t)}}
	}
}

func resourceKey(r model.Resource) string {
	return attributesKey(r.Attrs) + fmt.Sprintf("\x00%d", r.DroppedAttributes)
}

func attributesKey(input []model.Attribute) string {
	attrs := append([]model.Attribute(nil), input...)
	sort.Slice(attrs, func(i, j int) bool { return attrs[i].Key < attrs[j].Key })
	var b strings.Builder
	for _, a := range attrs {
		b.WriteString(a.Key)
		b.WriteByte('=')
		b.WriteString(a.Value.String())
		b.WriteByte('\n')
	}
	return b.String()
}
