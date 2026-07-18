package app

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/yaop-labs/reef/credential"
)

var credentialKinds = [...]credential.Kind{
	credential.KindServerLeaf,
	credential.KindClientLeaf,
	credential.KindServerCA,
	credential.KindClientCA,
	credential.KindServerToken,
	credential.KindClientToken,
}

type credentialMetrics struct {
	success    [len(credentialKinds)]atomic.Uint64
	failure    [len(credentialKinds)]atomic.Uint64
	change     [len(credentialKinds)]atomic.Uint64
	generation [len(credentialKinds)]atomic.Uint64
}

func (m *credentialMetrics) ObserveCredential(event credential.Event) {
	index := credentialKindIndex(event.Status.Kind)
	if index < 0 {
		return
	}
	for current := m.generation[index].Load(); event.Status.Generation > current; current = m.generation[index].Load() {
		if m.generation[index].CompareAndSwap(current, event.Status.Generation) {
			break
		}
	}
	if event.Success {
		m.success[index].Add(1)
	} else {
		m.failure[index].Add(1)
	}
	if event.Changed {
		m.change[index].Add(1)
	}
}

func (m *credentialMetrics) writePrometheus(w io.Writer) {
	_, _ = fmt.Fprintln(w, "# TYPE coral_credential_events_total counter")
	_, _ = fmt.Fprintln(w, "# TYPE coral_credential_generation_max gauge")
	for index, kind := range credentialKinds {
		_, _ = fmt.Fprintf(
			w,
			"coral_credential_events_total{kind=%q,outcome=\"success\"} %d\n",
			kind,
			m.success[index].Load(),
		)
		_, _ = fmt.Fprintf(
			w,
			"coral_credential_events_total{kind=%q,outcome=\"failure\"} %d\n",
			kind,
			m.failure[index].Load(),
		)
		_, _ = fmt.Fprintf(
			w,
			"coral_credential_events_total{kind=%q,outcome=\"changed\"} %d\n",
			kind,
			m.change[index].Load(),
		)
		_, _ = fmt.Fprintf(
			w,
			"coral_credential_generation_max{kind=%q} %d\n",
			kind,
			m.generation[index].Load(),
		)
	}
}

func credentialKindIndex(kind credential.Kind) int {
	for index, candidate := range credentialKinds {
		if kind == candidate {
			return index
		}
	}
	return -1
}
