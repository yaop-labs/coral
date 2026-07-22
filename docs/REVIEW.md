# Coral current engineering review

Review date: 2026-07-22

Reviewed baseline: `f0868e1994c818e098afd0161401856f970c38a0` on `main`.
Gate 1 and Gate 2 closure below cover the current working tree.

Target: stable single-node Coral with a version chosen from the completed
capability and compatibility impact, not assigned in advance

## Executive result

Coral has a strong collector core: standard three-signal OTLP ingress, legacy
trace receivers, bounded generic pipelines, isolated fan-out, Reef security,
Gyre lifecycle, admission controls, Wisp delivery metadata, and broad race and
failure-injection coverage. A rewrite is not justified.

Coral is not ready to tag as stable yet, but Gate 1 is closed. The journal now
owns the single-node handoff from an acknowledged OTLP admission through
required Amber acceptance or durable quarantine. It performs bounded live
redispatch, persists response-loss receipts, reconciles interrupted state
transitions, and reports pressure/failure through readiness and metrics. The
remaining release blockers are real Amber fidelity proof, legacy listener
policy, and the operational release gate.

The current stabilization scope is single-node production operation. Horizontal
scale, a control-plane API, and a full organisation/project model remain later
work and are not required to make the single-node contract honest.

## Verified capabilities

- One OTLP gRPC and one OTLP HTTP ingress serve traces, metrics, and logs.
- OTLP/HTTP supports protobuf, JSON, gzip, compressed/decompressed limits, and
  protocol-specific responses.
- Reef v0.3 protects OTLP, self-observation, and HTTP exporter edges with
  fail-closed plaintext policy, TLS/mTLS, bearer auth, managed credential
  reload, and redirect/origin containment.
- The app implements Gyre v0.5 component lifecycle, typed readiness/status,
  startup rollback, and context-bounded idempotent shutdown.
- Input queues and exporter lanes are bounded by item count and configured
  byte estimates; slow destinations have independent lanes.
- Admission supports configured principal-to-tenant allowlists and bounded
  item, byte, concurrency, request-rate, log, and metric limits.
- Optional Wisp headers are strictly parsed. Process-local dedup detects
  identical replays and payload conflicts with bounded TTL and capacity.
- The journal format is length-delimited, checksummed, fsynced, capacity
  bounded, versioned, and tested for interrupted tails and injected fsync
  failures.
- Logs and metrics remain in their OTLP protobuf representations through their
  pipelines. Redaction covers log bodies/attributes and metric datapoints and
  exemplars.
- Tail sampling is bounded by trace count and a byte estimate and exposes
  pending, eviction, and late-span metrics.
- CI/release workflows, build metadata, deterministic packages, and a broad
  race-tested suite exist.
- The current Coral-to-Fathom three-signal integration gate passes.

## Closed stabilization work

### Gate 1 — delivery-owned journal lifecycle

Ingress fsyncs a canonical post-admission envelope with an internal record ID
before enqueue. The identity survives queueing, batching, tail-sampling splits,
redispatch generations, and exporter fan-out. Amber is required; Fathom/S3 are
best-effort. Successful required delivery atomically reclaims only completed
records. Transient failures are redispatched without restart; permanent and
partial-success rejection is moved to a durable quarantine with a bounded log
reason. A bounded receipt ledger prevents Wisp response-loss duplicates across
restart. Startup reconciles crashes after receipt/quarantine fsync but before
active-record removal. Pressure, retry, pending acknowledgement, and quarantine
degrade readiness and are visible through bounded metrics.

The race-tested matrix covers append/ack fsync failure, capacity exhaustion,
truncated/corrupt tails, process crash, compaction retention, interrupted
sidecar transitions, stale dispatch completion, downstream recovery, permanent
rejection, response loss, and graceful shutdown ordering.

### Gate 2 — exact bounds and routing

The trace batch processor now bounds retained spans by count and bytes,
including direct pass-through for an individual item larger than the buffer
budget. Nested processor/exporter configuration is type-aware and rejects
unknown fields, including actions, sampling rules, retry, TLS, and auth blocks.
Mapped tenant identity survives queueing, batching, sampling, metrics, logs,
trace fan-out, and journal replay. HTTP exporters emit it as the bounded
`X-Coral-Tenant` routing header; S3 partitions objects by escaped tenant. Queue
and exporter-lane metrics expose item/byte depth and stable destination labels.

### Gate 3 — source-of-truth fidelity (closed)

The first slice adds a raw trace golden fixture covering trace state, events,
links, schema URLs, and dropped counts; an all-OTLP-metric-types round-trip
fixture covering temporality, monotonicity, and exemplars; and representative
log resource/record preservation. A reproducible local Amber process-pair
smoke now accepts representative traces, metrics, and logs through Coral
(`configs/examples/gate3-amber.yaml` plus the adjacent JSON fixtures). Fathom,
maximal-field assertions through the live pair, the partial-success matrix,
and release-profile enforcement are carried into Gate 4 hardening.

## Release blockers

## High-priority correctness work

### P1 — legacy listeners bypass the Reef edge policy

Jaeger UDP/TCP/HTTP and Zipkin HTTP use endpoint-only configuration. They have
no TLS/auth or explicit insecure policy and can bind externally. For stable
operation they must either adopt Reef-compatible protection, be restricted to
loopback by default with explicit risk opt-in, or be declared unsupported in
the production profile.

## Lower-priority and later work

- Complete item-level partial-success aggregation for logs and metrics.
- Define versioned organisation/project identity; the current map is a routing
  contract, not a full control-plane model.
- Add fair scheduling across tenants rather than request-local quotas only.
- Persist or explicitly reset tail-sampling decisions across restart and
  expose keep/drop/incomplete reasons.
- Version or explicitly label the S3 trace format as lossy.
- Add Gyre Reloadable only after transactional last-known-good config
  replacement is designed.
- Add admin/RBAC/audit APIs only when the platform needs them.
- Design affinity/shared state before claiming horizontal durability.

## Corrections landed during this re-baseline

- `Journal.Recover` now applies the configured and hard record limits before
  allocating an on-disk length, matching the safe replay path.
- Trace memory accounting now includes retained raw OTLP and scope/schema
  strings, with a regression test using a multi-kilobyte raw span.
- The Amber and Fathom trace converters now preserve separate resource/scope
  schemas, scope attributes, trace state, flags, events, links, and dropped
  counts while still applying normalized processor output.
- Trace redaction now scrubs nested event and link attributes in the retained
  raw OTLP before either downstream converter can emit them.
- Amber and Fathom exporters now decode OTLP success responses for all signals;
  a non-zero downstream partial-success rejection is a permanent exporter
  failure rather than a falsely confirmed delivery. Required Amber work is
  conservatively moved to durable quarantine.

## Verification status

At the reviewed commit:

- vet: pass;
- full race suite: pass;
- config fuzz smoke: pass with writable temporary cache;
- golangci-lint v2.12.2: zero issues with writable temporary cache;
- deterministic Linux/amd64 package comparison: pass;
- Coral-to-Fathom three-signal integration gate: pass;
- live release tag: absent;
- Gate 1 component/app crash, retry, response-loss, partial-success
  classification, pressure, and quarantine matrix: pass;
- Gate 2 byte-boundary, nested-config, routing-header, lane-metrics, and
  multi-signal integration matrix: pass;
- real Wisp-to-Coral-to-Amber sustained soak: remains Gate 4.

## Release verdict

Do not tag the current commit as stable. Keep it as an unreleased development
baseline. Gates 1 and 2 are closed; continue with Gate 3 fidelity and then the
operational release gate. No version is assigned in advance.
