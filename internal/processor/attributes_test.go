package processor

import (
	"context"
	"testing"

	"github.com/yaop-labs/coral/internal/model"
)

func span(attrs ...model.Attribute) model.Span {
	return model.Span{Name: "test", Attrs: attrs}
}

func attr(k, v string) model.Attribute { return model.StringAttr(k, v) }

func TestAttributesProcessor_Delete(t *testing.T) {
	p, err := NewAttributes([]AttributeActionConfig{{Action: "delete", Key: "secret"}})
	if err != nil {
		t.Fatal(err)
	}
	s := span(attr("secret", "val"), attr("keep", "yes"))
	got, err := p.Process(context.Background(), model.Batch{Spans: []model.Span{s}})
	if err != nil {
		t.Fatal(err)
	}
	result := got.Spans[0]
	if result.AttrValue("secret") != "" {
		t.Error("secret attr should be deleted")
	}
	if result.AttrValue("keep") != "yes" {
		t.Error("keep attr should remain")
	}
}

func TestAttributesProcessor_Add(t *testing.T) {
	p, err := NewAttributes([]AttributeActionConfig{{Action: "add", Key: "env", Value: "prod"}})
	if err != nil {
		t.Fatal(err)
	}
	s := span()
	got, _ := p.Process(context.Background(), model.Batch{Spans: []model.Span{s}})
	if got.Spans[0].AttrValue("env") != "prod" {
		t.Error("env attr should be added")
	}
}

func TestAttributesProcessor_ResourceScope(t *testing.T) {
	p, err := NewAttributes([]AttributeActionConfig{
		{Action: "add", Scope: "resource", Key: "collector", Value: "coral"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Two spans sharing one resource attribute slice, as produced when they
	// come from the same OTLP ResourceSpans.
	shared := []model.Attribute{attr("service.name", "checkout")}
	spans := []model.Span{
		{Name: "a", Resource: model.Resource{Attrs: shared}},
		{Name: "b", Resource: model.Resource{Attrs: shared}},
	}
	got, err := p.Process(context.Background(), model.Batch{Spans: spans})
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range got.Spans {
		if s.Resource.AttrValue("collector") != "coral" {
			t.Errorf("span %d: resource should carry collector=coral", i)
		}
		if s.AttrValue("collector") != "" {
			t.Errorf("span %d: collector must land on the resource, not span attributes", i)
		}
	}
	if len(shared) != 1 || shared[0].Key != "service.name" {
		t.Errorf("shared resource slice was mutated: %+v", shared)
	}
}

func TestAttributesProcessor_UnknownScope(t *testing.T) {
	if _, err := NewAttributes([]AttributeActionConfig{
		{Action: "add", Scope: "bogus", Key: "k", Value: "v"},
	}); err == nil {
		t.Fatal("expected error for unknown scope")
	}
}

func TestAttributesProcessor_Add_Overwrite(t *testing.T) {
	p, err := NewAttributes([]AttributeActionConfig{{Action: "add", Key: "env", Value: "prod"}})
	if err != nil {
		t.Fatal(err)
	}
	s := span(attr("env", "staging"))
	got, _ := p.Process(context.Background(), model.Batch{Spans: []model.Span{s}})
	if got.Spans[0].AttrValue("env") != "prod" {
		t.Errorf("env should be overwritten, got %q", got.Spans[0].AttrValue("env"))
	}
}

func TestAttributesProcessor_Rename(t *testing.T) {
	p, err := NewAttributes([]AttributeActionConfig{{Action: "rename", Key: "old", NewKey: "new"}})
	if err != nil {
		t.Fatal(err)
	}
	s := span(attr("old", "val"))
	got, _ := p.Process(context.Background(), model.Batch{Spans: []model.Span{s}})
	if got.Spans[0].AttrValue("new") != "val" {
		t.Error("attr should be renamed to 'new'")
	}
	if got.Spans[0].AttrValue("old") != "" {
		t.Error("old attr should not exist after rename")
	}
}

func TestAttributesProcessor_InvalidAction(t *testing.T) {
	_, err := NewAttributes([]AttributeActionConfig{{Action: "explode"}})
	if err == nil {
		t.Fatal("expected error for unknown action")
	}
}

func TestAttributesProcessor_DeleteMissingKey(t *testing.T) {
	_, err := NewAttributes([]AttributeActionConfig{{Action: "delete"}})
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}
