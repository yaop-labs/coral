package processor

import (
	"context"
	"fmt"

	"github.com/yaop-labs/coral/internal/model"
)

type actionKind string

const (
	actionDelete actionKind = "delete"
	actionAdd    actionKind = "add"
	actionRename actionKind = "rename"
)

// attrScope selects whether an action edits span or resource attributes.
type attrScope uint8

const (
	scopeSpan attrScope = iota
	scopeResource
)

type attributeAction struct {
	kind   actionKind
	scope  attrScope
	key    string
	value  model.AttributeValue // for "add"
	newKey string               // for "rename"
}

// AttributeProcessor mutates span or resource attributes: add, delete, or
// rename keys. Resource-scoped actions are how coral stamps enrichment
// (collector=coral, k8s.*/cloud.*) onto traces so it reaches the store.
type AttributeProcessor struct {
	actions []attributeAction
}

type AttributeActionConfig struct {
	Action string
	Scope  string // "span" (default) or "resource"
	Key    string
	Value  string
	NewKey string
}

func NewAttributes(actions []AttributeActionConfig) (*AttributeProcessor, error) {
	out := make([]attributeAction, 0, len(actions))
	for i, a := range actions {
		scope, err := parseScope(a.Scope)
		if err != nil {
			return nil, fmt.Errorf("attributes action[%d]: %w", i, err)
		}
		switch actionKind(a.Action) {
		case actionDelete:
			if a.Key == "" {
				return nil, fmt.Errorf("attributes action[%d]: delete requires key", i)
			}
			out = append(out, attributeAction{kind: actionDelete, scope: scope, key: a.Key})
		case actionAdd:
			if a.Key == "" {
				return nil, fmt.Errorf("attributes action[%d]: add requires key", i)
			}
			out = append(out, attributeAction{kind: actionAdd, scope: scope, key: a.Key, value: model.StringValue(a.Value)})
		case actionRename:
			if a.Key == "" || a.NewKey == "" {
				return nil, fmt.Errorf("attributes action[%d]: rename requires key and new_key", i)
			}
			out = append(out, attributeAction{kind: actionRename, scope: scope, key: a.Key, newKey: a.NewKey})
		default:
			return nil, fmt.Errorf("attributes action[%d]: unknown action %q", i, a.Action)
		}
	}
	return &AttributeProcessor{actions: out}, nil
}

func parseScope(s string) (attrScope, error) {
	switch s {
	case "", "span":
		return scopeSpan, nil
	case "resource":
		return scopeResource, nil
	default:
		return 0, fmt.Errorf("unknown scope %q (want span or resource)", s)
	}
}

func (p *AttributeProcessor) Process(_ context.Context, b model.Batch) (model.Batch, error) {
	for i := range b.Spans {
		p.applyTo(&b.Spans[i])
	}
	return b, nil
}

func (p *AttributeProcessor) Close() error { return nil }

func (p *AttributeProcessor) applyTo(s *model.Span) {
	resourceCloned := false
	for _, a := range p.actions {
		if a.scope == scopeResource {
			// Spans converted from one OTLP ResourceSpans share a resource
			// attribute slice; clone before mutating so siblings aren't
			// corrupted by an in-place set/append.
			if !resourceCloned {
				s.Resource.Attrs = append([]model.Attribute(nil), s.Resource.Attrs...)
				resourceCloned = true
			}
			s.Resource.Attrs = applyAttr(s.Resource.Attrs, a)
		} else {
			s.Attrs = applyAttr(s.Attrs, a)
		}
	}
}

func applyAttr(attrs []model.Attribute, a attributeAction) []model.Attribute {
	switch a.kind {
	case actionDelete:
		return deleteAttr(attrs, a.key)
	case actionAdd:
		return setAttr(attrs, a.key, a.value)
	case actionRename:
		return renameAttr(attrs, a.key, a.newKey)
	}
	return attrs
}

func deleteAttr(attrs []model.Attribute, key string) []model.Attribute {
	out := attrs[:0]
	for _, a := range attrs {
		if a.Key != key {
			out = append(out, a)
		}
	}
	return out
}

func setAttr(attrs []model.Attribute, key string, value model.AttributeValue) []model.Attribute {
	for i, a := range attrs {
		if a.Key == key {
			attrs[i].Value = value
			return attrs
		}
	}
	return append(attrs, model.Attribute{Key: key, Value: value})
}

func renameAttr(attrs []model.Attribute, oldKey, newKey string) []model.Attribute {
	for i, a := range attrs {
		if a.Key == oldKey {
			attrs[i].Key = newKey
			return attrs
		}
	}
	return attrs
}
