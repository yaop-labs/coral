package processor

import (
	"context"
	"regexp"

	"github.com/yaop-labs/coral/internal/model"
)

// ValidateProcessor drops spans that exceed a size limit or contain
// credential patterns in their attributes.
type ValidateProcessor struct {
	maxSpanBytes int
	patterns     []*regexp.Regexp

	dropped uint64
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
	return &ValidateProcessor{maxSpanBytes: maxSpanBytes, patterns: compiled}, nil
}

func (v *ValidateProcessor) Process(_ context.Context, b model.Batch) (model.Batch, error) {
	out := b.Spans[:0]
	for _, s := range b.Spans {
		if s.SizeBytes() > v.maxSpanBytes {
			continue
		}
		if v.hasCreds(s) {
			continue
		}
		out = append(out, s)
	}
	return model.Batch{Spans: out}, nil
}

func (v *ValidateProcessor) Close() error { return nil }

func (v *ValidateProcessor) hasCreds(s model.Span) bool {
	if len(v.patterns) == 0 {
		return false
	}
	for _, a := range s.Attrs {
		for _, re := range v.patterns {
			if re.MatchString(a.Key) || re.MatchString(a.Value.String()) {
				return true
			}
		}
	}
	return false
}
