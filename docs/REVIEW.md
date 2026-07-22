# Coral current engineering review

Review date: 2026-07-22

Reviewed baseline: `f0868e1994c818e098afd0161401856f970c38a0` on `main`.
Gate 1 closure below also covers the current uncommitted working tree.

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
remaining release blockers are exact stateful bounds/routing, real Amber
fidelity proof, legacy listener policy, and the operational release gate.

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

## Release blockers

### P0 — stateful processing is not universally byte-bounded

The re-baseline corrected `model.Span.SizeBytes` to conservatively include raw
OTLP and scope/schema strings, so input queues, exporter lanes, and the tail
sampler no longer ignore nested events and links. The trace batch processor,
however, still buffers by span count and timeout without its own byte budget.
Moving a batch out of the input queue releases that queue's reservation while
the processor continues retaining it.

The stable gate requires byte accounting for every stateful buffer and
boundary tests using maximal events, links, nested values, and shared
resource/scope structures.

### P0 — finish tenant routing beyond the pipeline queue

Admission now stamps immutable tenant metadata on trace spans and metric/log
batches before queueing. The real asynchronous tail sampler keys traces by that
metadata, including after batching and replay. Dedup and durable receipts now
use the mapped tenant consistently. Exporters still do not propagate the
admitted tenant downstream.

For the next stable release, immutable routing metadata must survive every
queue, processor, split, and fan-out lane, or multi-tenant mode must fail
closed and remain explicitly experimental. The release must not claim tenant
isolation that ends before downstream propagation.

## High-priority correctness work

### P1 — legacy listeners bypass the Reef edge policy

Jaeger UDP/TCP/HTTP and Zipkin HTTP use endpoint-only configuration. They have
no TLS/auth or explicit insecure policy and can bind externally. For stable
operation they must either adopt Reef-compatible protection, be restricted to
loopback by default with explicit risk opt-in, or be declared unsupported in
the production profile.

### P1 — nested typed configuration is not uniformly strict

Top-level YAML uses `KnownFields(true)`, but custom `yaml.Node` decoding for
processors/exporters can ignore unknown nested keys. A typo in a timeout,
limit, redaction, or TLS-adjacent field must fail startup rather than silently
change behavior.

### P1 — per-destination lane metrics remain incomplete

Gate 1 added active journal bytes/records/oldest age, receipts, quarantine,
retry/ack state, and redispatch outcomes, and connected unhealthy durable state
to readiness. Exporter lane item/byte depth still lacks stable per-destination
labels and remains Gate 2 work.

## Lower-priority and later work

- Complete item-level partial-success aggregation for logs and metrics.
- Define versioned organisation/project identity and downstream propagation;
  the current map is an admission policy, not a full control-plane model.
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
- real Wisp-to-Coral-to-Amber sustained soak: remains Gate 4.

## Release verdict

Do not tag the current commit as stable. Keep it as an unreleased development
baseline. Gate 1 is closed; continue with Gate 2, then repeat the integration
and release review after the remaining gates close. No version is assigned in
advance.
