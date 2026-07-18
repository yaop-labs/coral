package otlp

import (
	"context"
	"github.com/yaop-labs/coral/internal/journal"
	"github.com/yaop-labs/coral/internal/model"
	"testing"
)

func TestReplayEnvelopeRejectsMissingSink(t *testing.T) {
	err := ReplayEnvelope(context.Background(), journal.Envelope{Signal: "traces", Payload: []byte{1}}, ReplaySinks{})
	if err == nil {
		t.Fatal("accepted replay without sink")
	}
}

func TestReplayEnvelopeUnsupportedSignal(t *testing.T) {
	err := ReplayEnvelope(context.Background(), journal.Envelope{Signal: "profiles"}, ReplaySinks{Traces: func(context.Context, model.Batch) error { return nil }})
	if err == nil {
		t.Fatal("accepted unsupported signal")
	}
}
