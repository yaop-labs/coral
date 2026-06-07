package processor

import (
	"context"
	"fmt"

	"github.com/hnlbs/collector/internal/model"
)

type actionKind string

const (
	actionDelete actionKind = "delete"
	actionAdd    actionKind = "add"
	actionRename actionKind = "rename"
)

type attributeAction struct {
	kind   actionKind
	key    string
	value  model.AttributeValue // for "add"
	newKey string               // for "rename"
}

// AttributeProcessor mutates span attributes: add, delete, or rename keys.
type AttributeProcessor struct {
	actions []attributeAction
}

type AttributeActionConfig struct {
	Action string
	Key    string
	Value  string
	NewKey string
}

func NewAttributes(actions []AttributeActionConfig) (*AttributeProcessor, error) {
	out := make([]attributeAction, 0, len(actions))
	for i, a := range actions {
		switch actionKind(a.Action) {
		case actionDelete:
			if a.Key == "" {
				return nil, fmt.Errorf("attributes action[%d]: delete requires key", i)
			}
			out = append(out, attributeAction{kind: actionDelete, key: a.Key})
		case actionAdd:
			if a.Key == "" {
				return nil, fmt.Errorf("attributes action[%d]: add requires key", i)
			}
			out = append(out, attributeAction{kind: actionAdd, key: a.Key, value: model.StringValue(a.Value)})
		case actionRename:
			if a.Key == "" || a.NewKey == "" {
				return nil, fmt.Errorf("attributes action[%d]: rename requires key and new_key", i)
			}
			out = append(out, attributeAction{kind: actionRename, key: a.Key, newKey: a.NewKey})
		default:
			return nil, fmt.Errorf("attributes action[%d]: unknown action %q", i, a.Action)
		}
	}
	return &AttributeProcessor{actions: out}, nil
}

func (p *AttributeProcessor) Process(_ context.Context, b model.Batch) (model.Batch, error) {
	for i := range b.Spans {
		p.applyTo(&b.Spans[i])
	}
	return b, nil
}

func (p *AttributeProcessor) Close() error { return nil }

func (p *AttributeProcessor) applyTo(s *model.Span) {
	for _, a := range p.actions {
		switch a.kind {
		case actionDelete:
			s.Attrs = deleteAttr(s.Attrs, a.key)
		case actionAdd:
			s.Attrs = setAttr(s.Attrs, a.key, a.value)
		case actionRename:
			s.Attrs = renameAttr(s.Attrs, a.key, a.newKey)
		}
	}
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
