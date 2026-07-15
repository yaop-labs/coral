package logs

import (
	"context"

	"github.com/yaop-labs/coral/internal/otlpredact"
)

// RedactProcessor scrubs credential-bearing values from log attributes
// (resource, scope, and each record) and from a matching string log body — logs
// are the most frequent secret carrier, so both are covered (contract §8).
type RedactProcessor struct {
	r *otlpredact.Redactor
}

func NewRedactProcessor(patterns []string) (*RedactProcessor, error) {
	red, err := otlpredact.New(patterns)
	if err != nil {
		return nil, err
	}
	return &RedactProcessor{r: red}, nil
}

func (p *RedactProcessor) Process(_ context.Context, b Batch) (Batch, error) {
	if !p.r.Enabled() {
		return b, nil
	}
	for _, rl := range b.ResourceLogs {
		if rl.GetResource() != nil {
			p.r.RedactKeyValues(rl.Resource.Attributes)
		}
		for _, sl := range rl.GetScopeLogs() {
			if sl.GetScope() != nil {
				p.r.RedactKeyValues(sl.Scope.Attributes)
			}
			for _, rec := range sl.GetLogRecords() {
				p.r.RedactKeyValues(rec.Attributes)
				p.r.RedactValue(rec.GetBody())
			}
		}
	}
	return b, nil
}

func (p *RedactProcessor) Close() error { return nil }
