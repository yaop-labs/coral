package processor

import (
	"context"
	"regexp"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/yaop-labs/coral/internal/model"
	"github.com/yaop-labs/coral/internal/otlpredact"
)

const (
	defaultServiceName = "unknown_service"
	redactedValue      = "[REDACTED]"
)

// ValidateProcessor normalizes and sanitizes spans: it drops spans over a size
// limit, guarantees service.name on the resource (contract §6), and redacts —
// rather than drops — attribute values matching credential patterns
// (contract §8), across both span and resource attributes.
type ValidateProcessor struct {
	maxSpanBytes int
	patterns     []*regexp.Regexp
	otlpRedactor *otlpredact.Redactor

	dropped  uint64
	redacted uint64
}

func NewValidate(maxSpanBytes int, patterns []string) (*ValidateProcessor, error) {
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, re)
	}
	if maxSpanBytes <= 0 {
		maxSpanBytes = 64 * 1024
	}
	redactor, err := otlpredact.New(patterns)
	if err != nil {
		return nil, err
	}
	return &ValidateProcessor{maxSpanBytes: maxSpanBytes, patterns: compiled, otlpRedactor: redactor}, nil
}

func (v *ValidateProcessor) Process(_ context.Context, b model.Batch) (model.Batch, error) {
	out := b.Spans[:0]
	for i := range b.Spans {
		s := &b.Spans[i]
		if s.SizeBytes() > v.maxSpanBytes {
			v.dropped++
			continue
		}
		ensureServiceName(s)
		v.redact(s)
		out = append(out, *s)
	}
	return model.Batch{Spans: out}, nil
}

func (v *ValidateProcessor) Close() error { return nil }

// ensureServiceName guarantees the resource carries service.name, which the
// store and fathom rely on (contract §6). Sibling spans may share a resource
// attribute slice, so it is cloned before appending.
func ensureServiceName(s *model.Span) {
	if s.Resource.ServiceName() != "" {
		return
	}
	attrs := append([]model.Attribute(nil), s.Resource.Attrs...)
	s.Resource.Attrs = append(attrs, model.StringAttr("service.name", defaultServiceName))
}

func (v *ValidateProcessor) redact(s *model.Span) {
	if len(v.patterns) == 0 {
		return
	}
	// Span attributes are per-span and safe to edit in place; resource
	// attributes may be shared, so redactAttrs clones them on the first hit.
	s.Attrs = v.redactAttrs(s.Attrs, false)
	s.Resource.Attrs = v.redactAttrs(s.Resource.Attrs, true)
	v.redactNestedOTLP(s)
}

func (v *ValidateProcessor) redactNestedOTLP(s *model.Span) {
	if len(s.OTLP) == 0 || v.otlpRedactor == nil || !v.otlpRedactor.Enabled() {
		return
	}
	var raw tracepb.Span
	if err := proto.Unmarshal(s.OTLP, &raw); err != nil {
		return
	}
	changed := 0
	for _, event := range raw.GetEvents() {
		changed += v.otlpRedactor.RedactKeyValues(event.GetAttributes())
	}
	for _, link := range raw.GetLinks() {
		changed += v.otlpRedactor.RedactKeyValues(link.GetAttributes())
	}
	if changed == 0 {
		return
	}
	encoded, err := proto.Marshal(&raw)
	if err != nil {
		return
	}
	s.OTLP = encoded
	v.redacted += uint64(changed)
}

func (v *ValidateProcessor) redactAttrs(attrs []model.Attribute, shared bool) []model.Attribute {
	cloned := !shared
	for i := range attrs {
		if !v.isCred(attrs[i]) {
			continue
		}
		if !cloned {
			attrs = append([]model.Attribute(nil), attrs...)
			cloned = true
		}
		attrs[i].Value = model.StringValue(redactedValue)
		v.redacted++
	}
	return attrs
}

func (v *ValidateProcessor) isCred(a model.Attribute) bool {
	for _, re := range v.patterns {
		if re.MatchString(a.Key) || re.MatchString(a.Value.String()) {
			return true
		}
	}
	return false
}
