package processor

import (
	"context"
	"testing"

	"github.com/hnlbs/collector/internal/model"
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
