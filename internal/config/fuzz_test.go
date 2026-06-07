package config

import "testing"

// FuzzConfigParse verifies that arbitrary YAML input never panics.
// Validation errors are expected.
func FuzzConfigParse(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("assembly:\n  decision_wait: 30s\n"))
	f.Add([]byte("assembly:\n  decision_wait: INVALID\n"))
	f.Add([]byte("{{{not yaml}}}"))
	f.Add([]byte("\x00\x01\x02"))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = Parse(data)
	})
}
