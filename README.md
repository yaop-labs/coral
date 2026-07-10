# coral

Trace + metric collector for the yaop stack: receives OTLP/Jaeger/Zipkin, processes, and exports to amber.

## Transport

A single OTLP ingress serves every signal — traces, metrics, and logs — on the
platform-standard `4317` (gRPC) / `4318` (HTTP) ports (contract §2). Legacy
trace-only protocols (Jaeger Thrift, Zipkin) keep their own ports. Self-obs
(`/metrics`, `/healthz`, `/readyz`) is on `4888`.

## Delivery semantics

Delivery is **at-most-once within coral**: there is no spool, so batches are
dropped on backpressure or shutdown rather than persisted. End-to-end
durability rests on the wisp spool and the amber WAL at the edges (contract §1).
Partially-invalid payloads are answered `200 + partial_success` so senders do
not retry rejected records (contract §4).
