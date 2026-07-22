# Coral platform compatibility

Updated: 2026-07-22

Coral baseline: `f0868e1` (unreleased)

This matrix records the versions Coral builds against and the behavior
verified in the current workspace. It is not a promise about unreviewed future
commits in sibling repositories.

| Component | Version/baseline | Current Coral contract |
| --- | --- | --- |
| Gyre | module `v0.5.0` | `App` implements `gyre.Component`; `/healthz`, `/readyz`, and `/status` use the Gyre lifecycle contract. Coral does not implement Runtime, Admin, or Reloadable. |
| Reef | module `v0.3.0` | OTLP gRPC/HTTP, self-observation, and HTTP exporters use high-level fail-closed edges with managed credentials. Legacy Jaeger/Zipkin listeners are not Reef-protected. |
| Wisp | released `v0.11.0` | Standard OTLP works. Optional `x-wisp-envelope-id` and `x-wisp-signal-kind` use canonical mapped-tenant dedup plus bounded durable response-loss receipts. Coral's delivery ownership is independent of Wisp's version cadence. |
| Amber | local `v0.3.0-39-g06107b8`; no Coral pin | HTTP OTLP endpoints are the required source-of-truth destination. Coral preserves metric/log protobuf structures and maximal trace metadata, rejects non-zero OTLP partial success as permanent incomplete delivery, and quarantines the required journal record. Real-pair fidelity remains a stabilization gate. |
| Fathom | local untagged `0233fb5`; no Coral pin | Optional isolated derived fan-out over OTLP/HTTP. The three-signal replay/readiness gate passes at the current workspace baselines. Fathom failure must not block Amber, but lane drops remain observable data loss for that derived destination. |

## Stable wire contracts

- Coral owns the platform-standard OTLP ports 4317 (gRPC) and 4318 (HTTP) for
  traces, metrics, and logs.
- Clients without Wisp headers remain valid standard OTLP clients.
- Wisp headers are delivery metadata, not authentication or tenant selection.
- Reef authentication derives the principal. Payload attributes never select
  a tenant.
- External plaintext on Reef-managed edges is rejected unless `insecure: true`
  is explicit. Bearer over plaintext additionally requires
  `danger_allow_bearer_over_plaintext: true`.
- `service.name` is guaranteed by configured Coral processing before supported
  downstream paths.
- Amber is the required durable source of truth in the production profile;
  Fathom is a derived fan-out.

## Current compatibility boundaries

### Wisp delivery identity

Coral accepts a 32-hex-character `x-wisp-envelope-id` and an optional signal
kind matching the actual OTLP endpoint. Same identity and payload can be
acknowledged as a duplicate; a different payload is a permanent conflict.
Dedup is bounded by 15 minutes and 100,000 in-memory entries. Completed Wisp
identities additionally use a bounded `<journal_path>.receipts` ledger for the
same TTL. Live lookup, active journal, receipt replay, and restart recovery use
the mapped tenant, signal, normalized delivery ID, and canonical request digest.

### Tenant identity

`tenant_map` is currently an ingress allowlist and routing key. It is not a
versioned organisation/project control plane. The mapped value is carried
through asynchronous pipeline ownership, sampling, journal, dedup, OTLP
export headers (`X-Coral-Tenant`), and S3 object prefixes. External Amber/Fathom
enforcement of that header is part of the Gate 3 real-pair contract.

### Durability

Wisp has a durable edge spool and Amber has its own WAL. With `journal_path`,
Coral fsyncs admitted OTLP before success, keeps per-record ownership through
processing, and removes active work only after every required Amber
contribution completes or an atomic move to permanent-failure quarantine.
Transient failure is redispatched live with bounded state. Receipt/quarantine
sidecars are recovered before active replay, including interrupted transitions.
Readiness fails on journal pressure, retry/ack backlog, or non-empty quarantine.

### Legacy receivers

Jaeger Thrift and Zipkin remain trace-only compatibility receivers. Their
listeners do not use Reef edge policy. They are outside the production profile;
if enabled for compatibility they must be bound to loopback behind an
explicitly managed local edge. The S3 `jsonl` exporter is similarly a lossy
derived format and is never a replacement for Amber's OTLP source of truth.

## Required release matrix

Before Coral's next stable release, the exact release commit must pass:

- Wisp v0.11 → Coral → Amber for traces, metrics, and logs, including outage,
  retry, response loss, restart, and duplicate delivery;
- Coral → Fathom three-signal fan-out and failure isolation;
- Gyre lifecycle conformance and Reef credential rotation;
- maximal OTLP fidelity/privacy fixtures against Amber;
- current and previous supported journal format recovery and rollback tests.

Any sibling version used by those gates must be recorded in the release
notes and artifact provenance.
