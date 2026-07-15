package metric

import (
	"context"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/yaop-labs/coral/internal/otlpredact"
)

func TestRedactProcessor_ScrubsCredentialsAcrossScopes(t *testing.T) {
	proc, err := NewRedactProcessor([]string{`(?i)authorization|password|api_key`, `[A-Za-z0-9_-]{40,}`})
	if err != nil {
		t.Fatal(err)
	}

	longToken := "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH" // > 40 chars
	b := Batch{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			stringKV("service.name", "app"),
			stringKV("api_key", "should-be-scrubbed"),
		}},
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{
			Name: "cpu_seconds_total",
			Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{
				Attributes: []*commonpb.KeyValue{
					stringKV("region", "us"),
					stringKV("session", longToken),
				},
				TimeUnixNano: 1, Value: &metricspb.NumberDataPoint_AsInt{AsInt: 7},
			}}}},
		}}}},
	}}}

	if _, err := proc.Process(context.Background(), b); err != nil {
		t.Fatalf("Process: %v", err)
	}

	res := b.ResourceMetrics[0].Resource.Attributes
	if v, _ := attrVal(res, "service.name"); v != "app" {
		t.Errorf("benign resource attr changed: %q", v)
	}
	if v, _ := attrVal(res, "api_key"); v != otlpredact.RedactedValue {
		t.Errorf("api_key not redacted: %q", v)
	}

	dp := b.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].GetGauge().DataPoints[0].Attributes
	if v, _ := attrVal(dp, "region"); v != "us" {
		t.Errorf("benign datapoint attr changed: %q", v)
	}
	if v, _ := attrVal(dp, "session"); v != otlpredact.RedactedValue {
		t.Errorf("long-token datapoint attr not redacted: %q", v)
	}
}

func TestRedactProcessor_NoPatternsIsPassthrough(t *testing.T) {
	proc, err := NewRedactProcessor(nil)
	if err != nil {
		t.Fatal(err)
	}
	b := Batch{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringKV("api_key", "kept")}},
	}}}
	if _, err := proc.Process(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if v, _ := attrVal(b.ResourceMetrics[0].Resource.Attributes, "api_key"); v != "kept" {
		t.Errorf("value changed with no patterns: %q", v)
	}
}
