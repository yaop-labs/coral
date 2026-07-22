package amber

import (
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/yaop-labs/coral/internal/model"
)

func TestTraceRequestPreservesRawTraceFidelity(t *testing.T) {
	raw, err := proto.Marshal(&tracepb.Span{
		TraceState: "vendor=value",
		Events: []*tracepb.Span_Event{{
			Name:         "exception",
			TimeUnixNano: 42,
			Attributes:   []*commonpb.KeyValue{{Key: "exception.type", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "panic"}}}},
		}},
		Links: []*tracepb.Span_Link{{
			TraceId: []byte{9}, SpanId: []byte{8}, TraceState: "linked=true",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	traceID := model.TraceID{1}
	spanID := model.SpanID{2}
	req := TraceRequest(model.Batch{Spans: []model.Span{{
		TraceID: traceID, SpanID: spanID, Name: "operation", Kind: model.KindServer,
		StartTime: time.Unix(1, 2), EndTime: time.Unix(1, 3),
		Resource:  model.Resource{Attrs: []model.Attribute{model.StringAttr("service.name", "checkout")}},
		ScopeName: "instrumentation", ScopeVersion: "1.2.3", ScopeSchemaURL: "scope://schema",
		ResourceSchemaURL: "resource://schema", TraceFlags: 1, DroppedEvents: 2, DroppedLinks: 3,
		OTLP: raw,
	}}})
	if len(req.ResourceSpans) != 1 || len(req.ResourceSpans[0].ScopeSpans[0].Spans) != 1 {
		t.Fatalf("unexpected request shape: %+v", req)
	}
	got := req.ResourceSpans[0].ScopeSpans[0]
	span := got.Spans[0]
	if span.GetTraceState() != "vendor=value" || len(span.Events) != 1 || len(span.Links) != 1 {
		t.Fatalf("raw trace fields lost: %+v", span)
	}
	if got.SchemaUrl != "scope://schema" || req.ResourceSpans[0].SchemaUrl != "resource://schema" {
		t.Fatalf("schema URLs lost: resource=%q scope=%q", req.ResourceSpans[0].SchemaUrl, got.SchemaUrl)
	}
	if span.GetDroppedEventsCount() != 2 || span.GetDroppedLinksCount() != 3 {
		t.Fatalf("dropped counts lost: events=%d links=%d", span.GetDroppedEventsCount(), span.GetDroppedLinksCount())
	}
}
