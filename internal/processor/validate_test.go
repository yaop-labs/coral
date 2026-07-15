package processor

import (
	"context"
	"strings"
	"testing"

	"github.com/yaop-labs/coral/internal/model"
)

func TestValidateProcessor_SizeFilter(t *testing.T) {
	// small span SizeBytes = 64 (fixed) + len("ok") = 66; threshold = 80 keeps it
	// large span = 64 + 100 + 2 = 166 > 80, dropped
	p, err := NewValidate(80, nil)
	if err != nil {
		t.Fatal(err)
	}

	large := model.Span{
		Name:  strings.Repeat("x", 100),
		Attrs: []model.Attribute{model.StringAttr("k", "v")},
	}
	small := model.Span{Name: "ok"}

	got, err := p.Process(context.Background(), model.Batch{Spans: []model.Span{large, small}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Spans) != 1 || got.Spans[0].Name != "ok" {
		t.Errorf("expected 1 small span, got %d", len(got.Spans))
	}
}

func TestValidateProcessor_CredsRedacted(t *testing.T) {
	p, err := NewValidate(0, []string{`(?i)password`})
	if err != nil {
		t.Fatal(err)
	}

	cred := model.Span{
		Name:  "op",
		Attrs: []model.Attribute{model.StringAttr("db.password", "secret"), model.StringAttr("keep", "yes")},
	}
	clean := model.Span{Name: "clean"}

	got, err := p.Process(context.Background(), model.Batch{Spans: []model.Span{cred, clean}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Spans) != 2 {
		t.Fatalf("both spans should be kept (redact, not drop), got %d", len(got.Spans))
	}
	if v := got.Spans[0].AttrValue("db.password"); v != "[REDACTED]" {
		t.Errorf("secret should be redacted, got %q", v)
	}
	if got.Spans[0].AttrValue("keep") != "yes" {
		t.Error("non-secret attribute should be preserved")
	}
}

func TestValidateProcessor_RedactsResourceAndDefaultsService(t *testing.T) {
	p, err := NewValidate(0, []string{`(?i)token`})
	if err != nil {
		t.Fatal(err)
	}
	// Two spans sharing one resource slice, one attr a secret, none carrying
	// service.name.
	shared := []model.Attribute{model.StringAttr("api.token", "abc123")}
	spans := []model.Span{
		{Name: "a", Resource: model.Resource{Attrs: shared}},
		{Name: "b", Resource: model.Resource{Attrs: shared}},
	}
	got, err := p.Process(context.Background(), model.Batch{Spans: spans})
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range got.Spans {
		if s.Resource.AttrValue("api.token") != "[REDACTED]" {
			t.Errorf("span %d: resource secret should be redacted", i)
		}
		if s.Resource.ServiceName() != "unknown_service" {
			t.Errorf("span %d: service.name should default to unknown_service", i)
		}
	}
	if shared[0].Value.String() != "abc123" {
		t.Errorf("shared resource slice was mutated: %q", shared[0].Value.String())
	}
}

func TestValidateProcessor_InvalidPattern(t *testing.T) {
	_, err := NewValidate(0, []string{"[invalid"})
	if err == nil {
		t.Fatal("expected error for invalid regex")
	}
}

func TestValidateProcessor_AllPass(t *testing.T) {
	p, err := NewValidate(0, nil)
	if err != nil {
		t.Fatal(err)
	}
	spans := []model.Span{{Name: "a"}, {Name: "b"}, {Name: "c"}}
	got, err := p.Process(context.Background(), model.Batch{Spans: spans})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Spans) != 3 {
		t.Errorf("expected 3 spans, got %d", len(got.Spans))
	}
}
