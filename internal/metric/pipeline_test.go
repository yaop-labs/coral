package metric

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// TestMetricPipelineEndToEnd wires receiver → attributes(enrich) → amber
// exporter, then pushes OTLP metrics over gRPC and asserts the fake amber
// received the enriched, intact metrics over HTTP /v1/metrics.
func TestMetricPipelineEndToEnd(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Fake amber: capture the OTLP request posted to /v1/metrics.
	var mu sync.Mutex
	var got *colmetricspb.ExportMetricsServiceRequest
	var gotPath string
	amber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		req := &colmetricspb.ExportMetricsServiceRequest{}
		_ = proto.Unmarshal(body, req)
		mu.Lock()
		got, gotPath = req, r.URL.Path
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer amber.Close()

	recv := NewOTLPReceiver("127.0.0.1:0", "", logger)
	attrs := NewAttributesProcessor([]AttributeAction{{Action: "upsert", Key: "collector", Value: "coral"}})
	exp, err := NewAmberExporter(amber.URL, 2*time.Second, RetryPolicy{})
	if err != nil {
		t.Fatal(err)
	}

	p := NewPipeline(2, 100, logger)
	p.AddReceiver(recv)
	p.AddProcessor(attrs)
	p.AddExporter(exp)

	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		_ = p.Shutdown(context.Background())
	}()

	// gRPC client pushes one gauge point.
	conn, err := grpc.NewClient(recv.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := colmetricspb.NewMetricsServiceClient(conn)

	req := &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringKV("service.name", "app")}},
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{
			Name: "cpu_seconds_total",
			Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{
				TimeUnixNano: 1, Value: &metricspb.NumberDataPoint_AsInt{AsInt: 7},
			}}}},
		}}}},
	}}}
	if _, err := client.Export(context.Background(), req); err != nil {
		t.Fatalf("grpc export: %v", err)
	}

	// Wait for the batch to traverse the pipeline and reach fake amber.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := got != nil
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if got == nil {
		t.Fatal("fake amber received nothing")
	}
	if gotPath != "/v1/metrics" {
		t.Errorf("path = %q, want /v1/metrics", gotPath)
	}
	if len(got.ResourceMetrics) != 1 {
		t.Fatalf("ResourceMetrics = %d, want 1", len(got.ResourceMetrics))
	}
	res := got.ResourceMetrics[0].Resource.GetAttributes()
	if v, _ := attrVal(res, "service.name"); v != "app" {
		t.Errorf("service.name lost: %q", v)
	}
	if v, ok := attrVal(res, "collector"); !ok || v != "coral" {
		t.Errorf("enrichment missing: collector = %q ok=%v", v, ok)
	}
	m := got.ResourceMetrics[0].ScopeMetrics[0].Metrics[0]
	if m.Name != "cpu_seconds_total" || m.GetGauge().DataPoints[0].GetAsInt() != 7 {
		t.Errorf("metric not preserved: %+v", m)
	}
}

func TestCROSMetricExporterExport(t *testing.T) {
	var gotPath string
	var got *colmetricspb.ExportMetricsServiceRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		req := &colmetricspb.ExportMetricsServiceRequest{}
		_ = proto.Unmarshal(body, req)
		gotPath = r.URL.Path
		got = req
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	exp, err := NewCROSExporter(server.URL, time.Second, RetryPolicy{})
	if err != nil {
		t.Fatalf("new cros exporter: %v", err)
	}
	err = exp.Export(context.Background(), Batch{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringKV("service.name", "app")}},
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{
			Name: "cpu_seconds_total",
			Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{
				TimeUnixNano: 1, Value: &metricspb.NumberDataPoint_AsInt{AsInt: 7},
			}}}},
		}}}},
	}}})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if gotPath != "/v1/metrics" {
		t.Fatalf("path = %q, want /v1/metrics", gotPath)
	}
	if got == nil || len(got.ResourceMetrics) != 1 {
		t.Fatalf("unexpected metric request: %+v", got)
	}
}
