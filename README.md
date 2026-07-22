# coral

Telemetry admission and routing gateway for the YAOP stack. Coral receives
standard OTLP traces, metrics, and logs plus legacy Jaeger/Zipkin traces,
applies explicitly configured processing, and independently routes signals to
downstream systems.

## Transport

A single OTLP ingress serves every signal — traces, metrics, and logs — on the
platform-standard `4317` (gRPC) / `4318` (HTTP) ports (contract §2). Legacy
trace-only protocols (Jaeger Thrift, Zipkin) keep their own ports. Self-obs
(`/metrics`, `/healthz`, `/readyz`, `/status`) is on `4888`. Lifecycle,
readiness, typed operational errors, and status implement the Gyre v0.5.0
component contract; Reef remains the transport-security owner.

## Delivery semantics

Delivery without `journal_path` is **at-most-once within Coral**: batches can be
dropped on backpressure, exporter failure, process failure, or forced shutdown.
The optional `journal_path` now fsyncs a canonical post-admission envelope
before enqueue and removes it only after required Amber delivery succeeds.
Transient required-delivery failures are redispatched from the journal without
a restart. Permanent failures, including non-zero Amber OTLP partial success,
move to `<journal_path>.quarantine`; bounded Wisp response-loss receipts live in
`<journal_path>.receipts`. Crashes between either sidecar append and active
record removal are reconciled on startup. Journal pressure, pending retry/ack,
and quarantine degrade readiness and are exposed by `coral_journal_*` metrics.
This closes Coral's single-node delivery-ownership gate; the full stable release
still depends on the remaining bounds/routing, Amber compatibility, and
operability gates.
Supported item-level trace rejections are answered with
`partial_success`; complete item-level log/metric aggregation remains a release
protocol gate.

Fan-out destinations have independent bounded queues. Amber is required;
Fathom, S3, and devnull are best-effort. A required-lane failure retains its
journal record, while an optional destination cannot block Amber completion.
When `journal_path` is enabled, every active signal pipeline must configure an
Amber exporter; Coral rejects a best-effort-only durable configuration.
Stateful trace batching also accepts an explicit `max_bytes` budget. Mapped
tenant identity is propagated to OTLP exporters through `X-Coral-Tenant` and
to S3 through tenant-scoped object prefixes.

### Tenant admission limits

`tenant_limits` are keyed by the authenticated Reef principal→tenant mapping.
Supported bounded controls are `max_items`, `max_bytes`, `max_concurrent`,
`max_requests_per_second`, `max_log_record_bytes`, `max_log_attributes`, and
`max_log_attribute_keys`. Defaults are unlimited within global safety bounds;
setting a value is an operator-controlled tightening policy. Log limit
violations are permanent (`InvalidArgument`/`400`) and are never appended to the
durable journal.

Example:

```yaml
tenant_map:
  wisp-project-a: project-a
tenant_limits:
  project-a:
    max_items: 10000
    max_bytes: 67108864
    max_concurrent: 32
    max_requests_per_second: 200
    max_log_record_bytes: 1048576
    max_log_attributes: 128
    max_log_attribute_keys: 4096
    max_metric_attributes: 4096
    max_metric_attribute_keys: 4096
    max_metric_series: 10000
```

## Security

Reef v0.3 protects OTLP gRPC/HTTP, self-observability, and HTTP exporter edges
with TLS, optional client-certificate verification, bearer authentication, and
managed last-known-good rotation. External plaintext is fail-closed unless
`insecure: true` is explicit; bearer over plaintext additionally requires
`danger_allow_bearer_over_plaintext: true`. Prefer `token_file` over embedding
a token in YAML. Example:

```yaml
receivers:
  otlp_grpc:
    endpoint: "0.0.0.0:4317"
    tls:
      enabled: true
      cert_file: "/run/coral/tls.crt"
      key_file: "/run/coral/tls.key"
      client_ca_file: "/run/coral/client-ca.crt" # enables mTLS
    auth:
      bearer:
        - name: "wisp"
          token_file: "/run/coral/ingress-token"

exporters:
  - type: amber
    endpoint: "https://amber:5318"
    tls:
      enabled: true
      ca_file: "/run/coral/amber-ca.crt"
      cert_file: "/run/coral/client.crt"
      key_file: "/run/coral/client.key"
      server_name: "amber"
    auth:
      token_file: "/run/coral/amber-token"
```

The removed `metric_pipeline.receivers` and `log_pipeline.receivers` keys are
rejected at startup. Move those endpoints to top-level `receivers`; all signals
then share the standard OTLP ports.

## Operations and development

`coral --version` prints the release version, Git revision, modified state, and
Go toolchain. `/metrics` exposes the same process-constant identity plus
readiness state and per-signal input queue depth/capacity.

Run the local source gate with:

```sh
make verify
make fuzz
```

Release packages are deterministic `.tar.gz` archives built by
`scripts/package.sh`; tag builds publish archives and `SHA256SUMS` only after
the release gate passes. The current beta release is `v0.1.0-beta.1`.
