package otlp

import (
	"context"
	"fmt"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/yaop-labs/coral/internal/journal"
	"github.com/yaop-labs/coral/internal/logs"
	"github.com/yaop-labs/coral/internal/metric"
	"github.com/yaop-labs/coral/internal/model"
)

type ReplaySinks struct {
	Traces  func(context.Context, model.Batch) error
	Metrics func(context.Context, metric.Batch) error
	Logs    func(context.Context, logs.Batch) error
}

func ReplayEnvelope(ctx context.Context, env journal.Envelope, sinks ReplaySinks) error {
	switch env.Signal {
	case "traces":
		var req coltracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(env.Payload, &req); err != nil {
			return err
		}
		if sinks.Traces == nil {
			return fmt.Errorf("trace replay sink unavailable")
		}
		return sinks.Traces(ctx, model.Batch{Spans: spansFromResourceSpans(req.GetResourceSpans())})
	case "metrics":
		var req colmetricspb.ExportMetricsServiceRequest
		if err := proto.Unmarshal(env.Payload, &req); err != nil {
			return err
		}
		if sinks.Metrics == nil {
			return fmt.Errorf("metric replay sink unavailable")
		}
		return sinks.Metrics(ctx, metric.Batch{ResourceMetrics: req.GetResourceMetrics()})
	case "logs":
		var req collogspb.ExportLogsServiceRequest
		if err := proto.Unmarshal(env.Payload, &req); err != nil {
			return err
		}
		if sinks.Logs == nil {
			return fmt.Errorf("log replay sink unavailable")
		}
		return sinks.Logs(ctx, logs.Batch{ResourceLogs: req.GetResourceLogs()})
	default:
		return fmt.Errorf("unsupported replay signal %q", env.Signal)
	}
}
