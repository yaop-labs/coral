// Package otlpredact redacts credential-bearing values from OTLP attributes.
// It is shared by the metric and log pipelines so every signal scrubs secrets
// with the same key/value pattern matching the trace validate processor uses
// (contract §8).
package otlpredact

import (
	"regexp"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// RedactedValue replaces any attribute value identified as a credential.
const RedactedValue = "[REDACTED]"

// Redactor matches attribute keys and string values against credential patterns.
type Redactor struct {
	patterns []*regexp.Regexp
}

// New compiles the credential patterns.
func New(patterns []string) (*Redactor, error) {
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, re)
	}
	return &Redactor{patterns: compiled}, nil
}

// Enabled reports whether any pattern is configured.
func (r *Redactor) Enabled() bool { return len(r.patterns) > 0 }

// MatchString reports whether s matches any credential pattern.
func (r *Redactor) MatchString(s string) bool {
	for _, re := range r.patterns {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

// RedactKeyValues replaces, in place, the value of every attribute whose key or
// string value matches a pattern, and returns how many were redacted. Redacting
// the value (rather than dropping the record) preserves the telemetry while
// removing the secret.
func (r *Redactor) RedactKeyValues(attrs []*commonpb.KeyValue) int {
	n := 0
	for _, kv := range attrs {
		if kv == nil {
			continue
		}
		if r.MatchString(kv.GetKey()) || r.MatchString(kv.GetValue().GetStringValue()) {
			kv.Value = RedactedAny()
			n++
		}
	}
	return n
}

// RedactedAny returns a fresh AnyValue holding the redaction placeholder.
func RedactedAny() *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: RedactedValue}}
}
