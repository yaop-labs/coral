package amber

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"

	"github.com/yaop-labs/coral/internal/model"
	"github.com/yaop-labs/reef/bearer"
	"github.com/yaop-labs/reef/edge"
)

func attr(kvs []*commonpb.KeyValue, key string) *commonpb.AnyValue {
	for _, kv := range kvs {
		if kv.Key == key {
			return kv.Value
		}
	}
	return nil
}

func TestAmberExporter_Export(t *testing.T) {
	var received *coltracepb.ExportTraceServiceRequest
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		received = &coltracepb.ExportTraceServiceRequest{}
		if err := proto.Unmarshal(body, received); err != nil {
			t.Errorf("unmarshal OTLP: %v", err)
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp, err := New(srv.URL, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	s := model.Span{
		TraceID:  model.TraceID{1},
		SpanID:   model.SpanID{1},
		Name:     "test.op",
		Kind:     model.KindServer,
		Status:   model.StatusOK,
		Resource: model.Resource{Attrs: []model.Attribute{model.StringAttr("service.name", "svc")}},
		Attrs: []model.Attribute{
			model.StringAttr("http.method", "GET"),
			{Key: "http.status_code", Value: model.IntValue(200)},
		},
	}
	if err := exp.Export(context.Background(), model.Batch{Spans: []model.Span{s}}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if gotPath != "/v1/traces" {
		t.Errorf("path = %q, want /v1/traces", gotPath)
	}
	if received == nil || len(received.ResourceSpans) != 1 {
		t.Fatalf("expected 1 ResourceSpans, got %+v", received)
	}
	rs := received.ResourceSpans[0]
	if v := attr(rs.Resource.GetAttributes(), "service.name"); v.GetStringValue() != "svc" {
		t.Errorf("resource service.name = %q", v.GetStringValue())
	}
	sp := rs.ScopeSpans[0].Spans[0]
	if sp.Name != "test.op" {
		t.Errorf("name = %q", sp.Name)
	}
	if sp.Status.GetCode() != 1 { // STATUS_CODE_OK
		t.Errorf("status code = %v, want OK", sp.Status.GetCode())
	}
	if v := attr(sp.Attributes, "http.method"); v.GetStringValue() != "GET" {
		t.Errorf("http.method = %q", v.GetStringValue())
	}
	if v := attr(sp.Attributes, "http.status_code"); v.GetIntValue() != 200 {
		t.Errorf("http.status_code = %d, want 200", v.GetIntValue())
	}
}

func TestAmberExporter_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	exp, _ := New(srv.URL, 5*time.Second)
	err := exp.Export(context.Background(), model.Batch{Spans: []model.Span{{Name: "x"}}})
	if err == nil {
		t.Fatal("expected error on 500 response")
	}
}

func TestAmberExporter_EmptyEndpoint(t *testing.T) {
	if _, err := New("", 0); err == nil {
		t.Fatal("expected error for empty endpoint")
	}
}

func TestAmberExporter_SendsBearerToken(t *testing.T) {
	var authorization string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	exp, err := New(srv.URL, time.Second, edge.ClientConfig{
		Auth:                           &bearer.ClientConfig{Token: "secret"},
		DangerAllowBearerOverPlaintext: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := exp.Export(context.Background(), model.Batch{Spans: []model.Span{{Name: "x"}}}); err != nil {
		t.Fatal(err)
	}
	if authorization != "Bearer secret" {
		t.Fatalf("authorization = %q", authorization)
	}
}

func TestToTraceRequest_RootSpanHasNoParent(t *testing.T) {
	s := model.Span{TraceID: model.TraceID{1}, SpanID: model.SpanID{1}}
	req := toTraceRequest(model.Batch{Spans: []model.Span{s}})
	sp := req.ResourceSpans[0].ScopeSpans[0].Spans[0]
	if len(sp.ParentSpanId) != 0 {
		t.Errorf("root span should have empty parent_span_id, got %x", sp.ParentSpanId)
	}
}

func TestEndpointNormalization(t *testing.T) {
	// Both forms must resolve to the same /v1/traces URL.
	for _, ep := range []string{"http://h:8080", "http://h:8080/", "http://h:8080/v1/traces"} {
		e, err := New(ep, 0, edge.ClientConfig{Insecure: true})
		if err != nil {
			t.Fatal(err)
		}
		if e.url != "http://h:8080/v1/traces" {
			t.Errorf("endpoint %q → url %q, want http://h:8080/v1/traces", ep, e.url)
		}
	}
}
