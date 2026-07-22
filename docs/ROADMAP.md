# Coral roadmap

Updated: 2026-07-22

Baseline: `f0868e1` on `main`

Direction: stable single-node Coral; the release version is chosen only after
the capability gates close.

This roadmap replaces the stale completion labels in the original
thirteen-increment plan. Completed historical work is recorded in
`docs/HISTORY.md`; evidence and current gaps are in `docs/REVIEW.md`.

## Current stabilization objective

Coral is being made into a secure, bounded, restart-safe single-node telemetry
gateway between Wisp/standard OTLP clients and Amber, with an isolated optional
Fathom fan-out. It does not need to be a horizontally scalable control plane or
a query service. It does need to tell the truth about every acknowledgement,
drop, retry, transformation, and durability boundary.

The release version is an outcome of the completed increment, not a deadline
or a requirement to match Wisp feature-for-feature.

## Current capability ledger

| Area | Status | Release interpretation |
| --- | --- | --- |
| Unified OTLP ingress | Complete | gRPC/HTTP, three signals, protobuf/JSON/gzip |
| Operational identity and CI | Complete | version metadata, CI, race/lint/fuzz, deterministic packaging |
| Gyre v0.5 lifecycle | Complete | component status/readiness and bounded idempotent close |
| Reef v0.3 OTLP/export edges | Complete | production policy on OTLP, self-observation, and HTTP exporters |
| Legacy Jaeger/Zipkin security | Open | exclude from production profile or protect explicitly |
| Pipeline drain and fan-out isolation | Complete | bounded lanes and truthful aggregate outcomes |
| Exact memory bounds | Complete for Gate 2 | queues, lanes, sampler, and trace batch processor account retained bytes |
| Trace fidelity | Partial | Coral converters preserve maximal spans; a real Amber round-trip gate remains |
| Tenant admission quotas | Complete for Gate 2 | mapped tenant identity crosses async state, dedup, journal, downstream HTTP headers, and S3 partitioning |
| Wisp delivery identity | Complete for Gate 1 | canonical live dedup plus bounded durable response-loss receipts use the mapped tenant/signal identity |
| Durable handoff journal | Gate 1 complete | append-before-ack, required-Amber completion, live redispatch, receipts, quarantine, atomic reclaim, and pressure readiness are implemented |
| Logs admission | Partial | bounds/redaction work; item partial success and Amber contract remain |
| Metrics admission | Partial | bounds/fidelity tests work; full series semantics and partial success remain |
| Tail sampling | Partial | bounded state and real queued tenant identity work; durable decision checkpoints do not |
| Admin API/horizontal scale | Deferred | explicitly outside the current stabilization cycle |

## Stabilization gates

### Gate 1 — make durability ownership real

Status: **closed on 2026-07-22**. This closes the durability-ownership gate;
it does not assign a release version or close the independent bounds, routing,
Amber fidelity, and operability gates below.

Goal: no batch acknowledged as durable can disappear before the configured
required destination durably accepts it.

Required work:

1. **Completed:** give every admitted journal record
   a stable internal identity independent of optional Wisp headers.
2. **Completed:** append and fsync the accepted
   post-admission envelope before returning OTLP success.
3. **Completed:** carry that identity through
   queueing, processing, batching/sampling, and fan-out without losing or
   prematurely completing parent records.
4. **Completed:** Amber is required; Fathom and other derived
   exporters are best-effort. Add explicit configuration only if a later use
   case needs another required destination.
5. **Completed:** acknowledge journal work only after
   required durable admission succeeds.
6. **Completed:** bounded jittered live redispatch replays the journal without
   restart. Permanent failures, including non-zero OTLP partial success, move
   to durable quarantine with a bounded reason and an operator log event.
7. **Completed:** reclaim acknowledged records with
   fsync plus atomic rename while retaining every unacknowledged record. Normal
   operation must reclaim delivered records and must not fill an append-only
   file forever.
8. **Completed:** journal pressure, retry, pending acknowledgement, and
   quarantine affect readiness and `/status`; bounded Prometheus metrics expose
   active records/age/bytes, receipts, quarantine, and redispatch outcomes.

Verification:

- crash injection before/after append, fsync, OTLP response, dispatch,
  downstream response, acknowledgement, and compaction;
- response-loss duplicate tests using Wisp delivery IDs;
- downstream outage, retry exhaustion, permanent rejection, partial success,
  corrupt tail, disk full, permission loss, and restart loops;
- invariant: every acknowledged fixture is either durably delivered or still
  present/quarantined after restart.

Closure evidence:

- full `go test ./... -race -count=1` passes;
- required exporter success removes the record, required failure retains it,
  and optional exporter failure does not block required completion;
- append precedes enqueue, post-admission trace payloads exclude rejected
  spans, and multi-child records are not acknowledged after the first child;
- legacy envelopes receive stable IDs before replay and acknowledgement uses
  an fsynced same-directory atomic replacement;
- graceful shutdown drains required delivery and acknowledgement before closing
  the journal.
- transient required failure is redispatched successfully without restart;
  permanent failure is atomically quarantined and degrades readiness;
- fsynced Wisp receipts suppress response-loss duplicates after restart, and
  startup reconciles crashes between receipt/quarantine append and active
  record removal;
- stale completions from an older dispatch generation cannot acknowledge a
  newer attempt;
- journal pressure returns `/readyz` 503 and exposes `coral_journal_healthy 0`;
- corrupt tails, record/disk capacity, append/ack fsync permission failures,
  compaction retention, process-crash recovery, and legacy envelope migration
  are covered by race-tested failure injection.

### Gate 2 — make bounds and routing exact

Status: **closed on 2026-07-22**. Mapped tenant metadata is preserved through
async state and emitted to OTLP downstreams as `X-Coral-Tenant`; S3 uses a
tenant-scoped object prefix. External Amber/Fathom enforcement of that routing
contract remains part of Gate 3 real-pair verification.

Goal: configured memory/disk/tenant limits describe retained state rather than
only a convenient subset of it.

Required work:

1. **Completed in the re-baseline:** include raw OTLP and scope/schema in
   conservative trace accounting used by queues, lanes, and the sampler.
2. **Completed in the re-baseline:** apply pre-allocation record limits to
   both journal Replay and Recover.
3. **Completed:** add byte budgets to stateful processor buffers so dequeueing
   does not make retained memory disappear from accounting.
4. **Completed:** preserve immutable routing metadata across the input queue,
   stateful splits/flushes, and exporter lanes.
5. **Completed:** use the same mapped tenant/routing key for quota, dedup,
   journal, sampling, metrics, and downstream propagation.
6. **Decided:** mapped multi-tenant routing is an explicit Coral contract;
   external downstream enforcement is verified in Gate 3; single-tenant
   deployment remains the recommended fail-closed production profile until
   that real-pair verification is complete.
7. **Completed:** reject unknown nested processor/exporter configuration fields.
8. **Completed:** expose queue and exporter-lane item/byte depth and capacity
   with stable destination identifiers.

Verification:

- maximal nested OTLP fixtures at every byte boundary;
- same TraceID and delivery ID across two authenticated tenants through the
  real async app, not only direct component tests;
- config typo/fuzz matrix and decompression/record-allocation attacks;
- bounded high-load tests proving no queue, sampler, dedup map, rate window, or
  journal grows beyond its declared limit.

Closure evidence:

- trace batch buffering has a configured `max_bytes` budget and passes a
  boundary test including oversized individual spans;
- processor/exporter nested typo fixtures fail configuration parsing;
- downstream OTLP requests carry `X-Coral-Tenant`, while S3 objects are
  partitioned under `tenant/<mapped-tenant>/`;
- queue and exporter-lane Prometheus metrics expose stable `destination` labels;
- the full race suite, integration suite, and config fuzz matrix pass.

### Gate 3 — make the source-of-truth path lossless and protocol-correct

Status: **in progress**. The first fidelity slice now has raw trace golden
coverage, all OTLP metric-type preservation coverage, representative log
resource/record coverage, and explicit downstream tenant-header assertions.
The real Amber/Fathom pair and release policy gates remain open.

Goal: Amber receives every supported field not intentionally transformed by a
configured processor, and Coral understands Amber's response.

Required work:

1. **Completed in the re-baseline:** preserve separate resource/scope schema,
   scope metadata, trace state, flags, events, links, and dropped counts on
   Coral-to-Amber and Coral-to-Fathom traces.
2. **Completed in the re-baseline:** redact nested trace event/link attributes
   in retained raw OTLP before downstream conversion.
3. **Completed across Gate 1 and the re-baseline:** decode OTLP success bodies from
   Amber and Fathom for all signals and classify non-zero partial rejection as
   permanent failure; required Amber rejection is conservatively quarantined.
4. **Slice covered:** verify metric types, temporality, monotonicity, exemplars,
   logs, resource attributes, and service identity in Coral's OTLP contract;
   real Amber compatibility remains open.
5. **Policy selected:** legacy Jaeger/Zipkin listeners are excluded from the
   production profile unless bound to loopback behind an explicitly managed
   edge; S3 `jsonl` is an optional lossy derived export, never the source of
   truth. Gate 4 must encode this policy in the production example/runbook.

Verification:

- maximal trace round-trip through a real Coral-to-Amber process pair;
- golden fixtures for every OTLP metric type and representative log bodies;
- permanent/transient/partial downstream response matrix;
- privacy fixture proving configured secrets do not survive in top-level or
  nested output fields.

### Gate 4 — prove operability and cut the release

Goal: turn component correctness into a supportable release.

Required work:

1. Add a production example with TLS/mTLS, bearer rotation, required Amber,
   bounded journal/queues, self-observation protection, and explicit legacy
   listener policy.
2. Publish a runbook for startup, readiness, journal pressure, quarantine,
   downstream outage, credential rotation, graceful shutdown, backup/move of
   journal state, and rollback.
3. Add real current-Wisp → Coral → Amber and Coral → Fathom gates. Preserve the
   existing successful three-signal Coral-to-Fathom gate.
4. Run race, lint, fuzz, deterministic package, crash matrix, and a sustained
   outage/recovery soak on the exact release commit.
5. Reconcile README, compatibility matrix, example configs, changelog, and
   build metadata. `Unreleased` must describe the actual increment.
6. Choose the version from the delivered compatibility and migration impact,
   build all release targets, verify checksums, tag the reviewed commit, and
   verify the tag workflow and published artifacts.

Release decision:

- no P0/P1 issue in `docs/REVIEW.md` remains unresolved or explicitly scoped
  out by a fail-closed production profile;
- no acknowledged fixture is lost in the failure matrix;
- the source-of-truth path is lossless under the stated transformations;
- current `main` and release tag CI are green;
- upgrade/rollback boundaries are documented and tested.

## After the current stabilization cycle

These capabilities are valuable but do not block an honest single-node
release:

- versioned organisation/project identity and policy reload;
- tenant-fair scheduling and downstream tenant-aware query contracts;
- durable tail-sampling checkpoints and richer keep/drop reason metrics;
- Gyre Reloadable and a Reef-protected audited admin API;
- horizontal routing/affinity, shared dedup/durability state, rolling
  migration, backup/restore certification, and multi-node chaos tests;
- platform SLOs, capacity certification, and provenance beyond the existing
  deterministic archives.

Each later increment must state its public contract, migration, failure
semantics, observability, and verification before implementation begins.
