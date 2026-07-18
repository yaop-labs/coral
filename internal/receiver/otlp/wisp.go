package otlp

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// WispIdentity is optional delivery metadata. It is never an authentication
// credential and callers without these headers remain valid OTLP clients.
type WispIdentity struct {
	EnvelopeID [16]byte
	SignalKind string
}

func parseWispHeaders(envelope, signal string) (WispIdentity, error) {
	var id WispIdentity
	if envelope == "" && signal == "" {
		return id, nil
	}
	if envelope == "" {
		return id, fmt.Errorf("wisp envelope id is required with signal kind")
	}
	if len(envelope) != 32 {
		return id, fmt.Errorf("wisp envelope id must be 32 hex characters")
	}
	b, err := hex.DecodeString(envelope)
	if err != nil {
		return id, fmt.Errorf("wisp envelope id: %w", err)
	}
	copy(id.EnvelopeID[:], b)
	if signal != "" {
		signal = strings.ToLower(strings.TrimSpace(signal))
		if signal != "traces" && signal != "metrics" && signal != "logs" && signal != "profiles" {
			return id, fmt.Errorf("unsupported wisp signal kind %q", signal)
		}
		id.SignalKind = signal
	}
	return id, nil
}
