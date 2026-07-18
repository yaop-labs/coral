package app

import (
	"bytes"
	"strings"
	"testing"

	"github.com/yaop-labs/reef/credential"
)

func TestCredentialMetricsAreBoundedAndGenerationIsMonotonic(t *testing.T) {
	var metrics credentialMetrics
	metrics.ObserveCredential(credential.Event{
		Status:  credential.Status{Kind: credential.KindClientToken, Generation: 2},
		Success: true,
		Changed: true,
	})
	metrics.ObserveCredential(credential.Event{
		Status:  credential.Status{Kind: credential.KindClientToken, Generation: 1},
		Success: false,
	})

	var output bytes.Buffer
	metrics.writePrometheus(&output)
	for _, want := range []string{
		`coral_credential_events_total{kind="client_bearer",outcome="success"} 1`,
		`coral_credential_events_total{kind="client_bearer",outcome="failure"} 1`,
		`coral_credential_events_total{kind="client_bearer",outcome="changed"} 1`,
		`coral_credential_generation_max{kind="client_bearer"} 2`,
	} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("metrics missing %q:\n%s", want, output.String())
		}
	}
}

func TestCredentialMetricsIgnoreUnknownKinds(t *testing.T) {
	var metrics credentialMetrics
	metrics.ObserveCredential(credential.Event{
		Status:  credential.Status{Kind: credential.Kind("future_kind"), Generation: 1},
		Success: true,
	})
	var output bytes.Buffer
	metrics.writePrometheus(&output)
	if strings.Contains(output.String(), "future_kind") {
		t.Fatalf("unknown kind became a metric label:\n%s", output.String())
	}
}
