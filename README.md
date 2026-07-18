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

Fan-out destinations have independent bounded queues. A slow or retrying
destination can drop from its own queue, but cannot delay delivery to the other
exporters.

## Security

OTLP gRPC and HTTP listeners support TLS, optional client-certificate
verification, and bearer authentication. HTTP exporters support custom CAs,
client certificates, server-name verification, and bearer authentication.
Prefer `token_file` over embedding a token in YAML. Example:

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

Run the complete local gate with:

```sh
make verify
make fuzz
```

Release packages are deterministic `.tar.gz` archives built by
`scripts/package.sh`; tag builds publish archives and `SHA256SUMS` only after
the release gate passes. The current architecture review, responsibility
boundaries, and capability plan are in
[`docs/REVIEW.md`](docs/REVIEW.md),
[`ADR 0001`](docs/adr/0001-coral-role-and-boundaries.md), and
[`docs/ROADMAP.md`](docs/ROADMAP.md).
