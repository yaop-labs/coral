package otlp

import (
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/yaop-labs/coral/internal/model"
)

func TestSpanFromProto_Basic(t *testing.T) {
	traceID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanID := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	parentID := [8]byte{9, 10, 11, 12, 13, 14, 15, 16}

	start := uint64(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	end := start + uint64(100*time.Millisecond)

	sp := &tracepb.Span{
		TraceId:           traceID[:],
		SpanId:            spanID[:],
		ParentSpanId:      parentID[:],
		Name:              "test.op",
		Kind:              tracepb.Span_SPAN_KIND_SERVER,
		StartTimeUnixNano: start,
		EndTimeUnixNano:   end,
		Status: &tracepb.Status{
			Code:    tracepb.Status_STATUS_CODE_ERROR,
			Message: "something failed",
		},
		Attributes: []*commonpb.KeyValue{
			{Key: "http.method", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "GET"}}},
			{Key: "http.status_code", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 500}}},
		},
	}
	res := &resourcepb.Resource{
		Attributes: []*commonpb.KeyValue{
			{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "payments"}}},
		},
	}

	got := spanFromProto(sp, resourceFromProto(res))

	if got.TraceID != model.TraceID(traceID) {
		t.Errorf("TraceID mismatch")
	}
	if got.SpanID != model.SpanID(spanID) {
		t.Errorf("SpanID mismatch")
	}
	if got.ParentSpanID != model.SpanID(parentID) {
		t.Errorf("ParentSpanID mismatch")
	}
	if got.Name != "test.op" {
		t.Errorf("Name = %q", got.Name)
	}
	if got.Kind != model.KindServer {
		t.Errorf("Kind = %v", got.Kind)
	}
	if got.Status != model.StatusError {
		t.Errorf("Status = %v", got.Status)
	}
	if got.StatusMsg != "something failed" {
		t.Errorf("StatusMsg = %q", got.StatusMsg)
	}
	if got.Resource.ServiceName() != "payments" {
		t.Errorf("ServiceName = %q", got.Resource.ServiceName())
	}
	if d := got.Duration(); d != 100*time.Millisecond {
		t.Errorf("Duration = %v", d)
	}
	if !got.HasError() {
		t.Error("expected HasError()")
	}
	if got.IsRoot() {
		t.Error("expected non-root span")
	}
	if got.AttrValue("http.method") != "GET" {
		t.Errorf("http.method = %q", got.AttrValue("http.method"))
	}
	if got.AttrValue("http.status_code") != "500" {
		t.Errorf("http.status_code = %q", got.AttrValue("http.status_code"))
	}
	for _, attr := range got.Attrs {
		if attr.Key == "http.status_code" && attr.Value.Kind() != model.AttrInt {
			t.Errorf("http.status_code kind = %v, want AttrInt", attr.Value.Kind())
		}
	}
}

func TestSpanFromProto_RootSpan(t *testing.T) {
	sp := &tracepb.Span{
		TraceId:           []byte{1},
		SpanId:            []byte{1},
		Name:              "root",
		StartTimeUnixNano: 1000,
		EndTimeUnixNano:   2000,
	}
	got := spanFromProto(sp, model.Resource{})
	if !got.IsRoot() {
		t.Error("expected root span (no parent)")
	}
	if got.Status != model.StatusUnset {
		t.Errorf("Status = %v, want Unset", got.Status)
	}
}

func TestKindFromProto(t *testing.T) {
	cases := []struct {
		in   tracepb.Span_SpanKind
		want model.SpanKind
	}{
		{tracepb.Span_SPAN_KIND_INTERNAL, model.KindInternal},
		{tracepb.Span_SPAN_KIND_SERVER, model.KindServer},
		{tracepb.Span_SPAN_KIND_CLIENT, model.KindClient},
		{tracepb.Span_SPAN_KIND_PRODUCER, model.KindProducer},
		{tracepb.Span_SPAN_KIND_CONSUMER, model.KindConsumer},
		{tracepb.Span_SPAN_KIND_UNSPECIFIED, model.KindUnspecified},
	}
	for _, tc := range cases {
		if got := kindFromProto(tc.in); got != tc.want {
			t.Errorf("kindFromProto(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestAnyValue_Typed(t *testing.T) {
	cases := []struct {
		v    *commonpb.AnyValue
		kind model.AttributeValueKind
		want string
	}{
		{&commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello"}}, model.AttrString, "hello"},
		{&commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}, model.AttrBool, "true"},
		{&commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 42}}, model.AttrInt, "42"},
		{&commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 3.14}}, model.AttrDouble, "3.14"},
		{nil, model.AttrString, ""},
	}
	for _, tc := range cases {
		got := anyValue(tc.v)
		if got.Kind() != tc.kind {
			t.Errorf("anyValue(%v).Kind() = %v, want %v", tc.v, got.Kind(), tc.kind)
		}
		if got.String() != tc.want {
			t.Errorf("anyValue(%v).String() = %q, want %q", tc.v, got.String(), tc.want)
		}
	}
}

func TestAnyValue_ArrayAndKVList(t *testing.T) {
	array := anyValue(&commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{
		ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{
			{Value: &commonpb.AnyValue_StringValue{StringValue: "a"}},
			{Value: &commonpb.AnyValue_IntValue{IntValue: 7}},
		}},
	}})
	if array.Kind() != model.AttrArray {
		t.Fatalf("array kind = %v, want AttrArray", array.Kind())
	}
	arrayRaw, ok := array.Interface().([]any)
	if !ok || len(arrayRaw) != 2 || arrayRaw[0] != "a" || arrayRaw[1] != int64(7) {
		t.Fatalf("array Interface() = %#v", array.Interface())
	}

	kvlist := anyValue(&commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{
		KvlistValue: &commonpb.KeyValueList{Values: []*commonpb.KeyValue{
			{Key: "enabled", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}},
		}},
	}})
	if kvlist.Kind() != model.AttrMap {
		t.Fatalf("kvlist kind = %v, want AttrMap", kvlist.Kind())
	}
	kvRaw, ok := kvlist.Interface().(map[string]any)
	if !ok || kvRaw["enabled"] != true {
		t.Fatalf("kvlist Interface() = %#v", kvlist.Interface())
	}
}

func TestSpansFromResourceSpans_MultipleResources(t *testing.T) {
	rs := []*tracepb.ResourceSpans{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "svc-a"}}},
				},
			},
			ScopeSpans: []*tracepb.ScopeSpans{
				{Spans: []*tracepb.Span{{TraceId: []byte{1}, SpanId: []byte{1}, Name: "a1"}}},
			},
		},
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "svc-b"}}},
				},
			},
			ScopeSpans: []*tracepb.ScopeSpans{
				{Spans: []*tracepb.Span{
					{TraceId: []byte{2}, SpanId: []byte{2}, Name: "b1"},
					{TraceId: []byte{2}, SpanId: []byte{3}, Name: "b2"},
				}},
			},
		},
	}

	spans := spansFromResourceSpans(rs)
	if len(spans) != 3 {
		t.Fatalf("expected 3 spans, got %d", len(spans))
	}
	if spans[0].Resource.ServiceName() != "svc-a" {
		t.Errorf("spans[0] service = %q", spans[0].Resource.ServiceName())
	}
	if spans[1].Resource.ServiceName() != "svc-b" {
		t.Errorf("spans[1] service = %q", spans[1].Resource.ServiceName())
	}
}

func TestSpansFromResourceSpans_PreservesResourceAndScopeMetadata(t *testing.T) {
	rs := []*tracepb.ResourceSpans{{
		SchemaUrl: "resource-schema",
		Resource: &resourcepb.Resource{
			DroppedAttributesCount: 2,
		},
		ScopeSpans: []*tracepb.ScopeSpans{{
			SchemaUrl: "scope-schema",
			Scope: &commonpb.InstrumentationScope{
				Name:                   "library",
				Version:                "1.0.0",
				DroppedAttributesCount: 3,
				Attributes: []*commonpb.KeyValue{{
					Key: "scope.attr", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "value"}},
				}},
			},
			Spans: []*tracepb.Span{{Name: "operation"}},
		}},
	}}
	spans := spansFromResourceSpans(rs)
	if len(spans) != 1 {
		t.Fatalf("spans = %d", len(spans))
	}
	span := spans[0]
	if span.ResourceSchemaURL != "resource-schema" || span.ScopeSchemaURL != "scope-schema" {
		t.Fatalf("schema URLs = %q/%q", span.ResourceSchemaURL, span.ScopeSchemaURL)
	}
	if span.Resource.DroppedAttributes != 2 || span.ScopeDroppedAttrs != 3 {
		t.Fatalf("dropped metadata = %d/%d", span.Resource.DroppedAttributes, span.ScopeDroppedAttrs)
	}
	if span.ScopeName != "library" || span.ScopeVersion != "1.0.0" || len(span.ScopeAttributes) != 1 {
		t.Fatalf("scope metadata = %+v", span)
	}
}
