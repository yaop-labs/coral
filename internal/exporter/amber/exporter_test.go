package amber

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hnlbs/collector/internal/model"
)

func TestAmberExporter_Export(t *testing.T) {
	var received payload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/batch" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &received)
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
	b := model.Batch{Spans: []model.Span{s}}

	if err := exp.Export(context.Background(), b); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if len(received.Spans) != 1 {
		t.Fatalf("expected 1 span in payload, got %d", len(received.Spans))
	}
	sp := received.Spans[0]
	if sp.Service != "svc" {
		t.Errorf("service = %q", sp.Service)
	}
	if sp.Name != "test.op" {
		t.Errorf("name = %q", sp.Name)
	}
	if sp.Status != "ok" {
		t.Errorf("status = %q", sp.Status)
	}
	if sp.Attrs["http.method"] != "GET" {
		t.Errorf("http.method = %q", sp.Attrs["http.method"])
	}
	if sp.Attrs["http.status_code"] != float64(200) {
		t.Errorf("http.status_code = %#v", sp.Attrs["http.status_code"])
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
	_, err := New("", 0)
	if err == nil {
		t.Fatal("expected error for empty endpoint")
	}
}

func TestToPayload_NoParent(t *testing.T) {
	s := model.Span{TraceID: model.TraceID{1}, SpanID: model.SpanID{1}}
	p := toPayload(model.Batch{Spans: []model.Span{s}})
	if p.Spans[0].ParentSpanID != "" {
		t.Errorf("root span should have empty parent_span_id")
	}
}
