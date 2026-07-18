# Coral capability roadmap

Updated: 2026-07-18

This roadmap follows the architecture decision in
`docs/adr/0001-coral-role-and-boundaries.md`. Stages are capability increments,
not a countdown to v1.0. A version or tag is chosen only when an increment is
complete, compatible, documented, and green.

## Current baseline

The completed baseline through increment 2.1 provides unified OTLP gRPC/HTTP
for traces/metrics/logs, Reef v0.3 managed security edges, count-bounded queues,
isolated exporter lanes, classified HTTP retry, trace oversize partial success,
legacy Jaeger/Zipkin ingestion, build identity, readiness/queue/credential
metrics, CI, deterministic release archives, and a Gyre v0.5.0-conformant
component lifecycle.

The baseline is not durable or multi-tenant. It has no Wisp envelope support,
storage/query API, or complete OTLP trace fidelity. Bounded drain semantics and
truthful byte/capacity accounting are the next feature increment. See
`docs/REVIEW.md` for evidence and `docs/PLATFORM_COMPATIBILITY.md` for
cross-repository boundaries.

## Increment 1 — operational identity and continuous verification

Status: completed and merged to `main`; final commit and CI are recorded above.

**Goal.** Make every binary identifiable and every proposed change subject to a
reproducible, automated minimum gate.

**Boundaries.** Add build metadata, `--version`, readiness reason/build metrics,
CI, release archive/checksum automation, and operator documentation. Do not
change OTLP, Gyre, Reef, Wisp, Amber, or Fathom contracts.

**Public contracts.** Additive CLI `--version`; additive bounded-cardinality
Prometheus metrics. Existing health status codes and config remain compatible.

**Storage/migrations.** None.

**Security model.** CI uses least-privilege read permissions. Release jobs use
the platform-provided token only for an explicitly pushed tag. No secrets enter
artifacts or build metadata.

**Failure semantics.** Build/test failure blocks artifacts. Readiness reports a
bounded lifecycle reason (`starting`, `ready`, `stopping`, `failed`) and never
leaks an error string as a label.

**Observability.** Version/revision/build status, readiness state, and current
queue depths; metadata labels are process-constant.

**Test strategy.** Unit tests for metadata formatting/escaping and readiness;
CI runs formatting check, `go vet`, `go test ./... -race -count=1`, config fuzz
smoke, and deterministic build twice with checksum comparison.

**Done when.** Local gate and CI are green, archives contain binary/license/
README, SHA-256 checksums are produced, docs/changelog are updated, and the
exact main commit's CI is verified.

**Compatibility.** Additive. Development builds report `dev`/`unknown`.

## Increment 2 — Gyre v0.5.0 lifecycle conformance

Status: completed and merged to `main` at
`c8e44f4435927958b5dacb2cfcd81007ef809e2c`; GitHub Actions run `29626105651`
passed.

**Goal.** Make Coral a real Gyre component and remove lifecycle leaks before
adding tenant or durability state.

**Boundaries.** Implement `gyre.Component` directly on the Coral app; standardize
state, readiness, status, typed errors, partial-start rollback, and close
semantics. Mount Gyre operational endpoints. Do not mount Gyre Admin, implement
reload, change Reef policy, or alter OTLP.

**Public contracts.** Additive `/status` JSON using `gyre.Snapshot`. Existing
`/healthz`, `/readyz`, `Start`, and `Shutdown` remain; `Shutdown` is an alias for
Gyre `Close`. Go consumers gain the `gyre.Component` interface.

**Storage/migrations.** None. Static configuration uses generation zero and
does not claim `gyre.Reloadable`.

**Security model.** Status uses static bounded reasons and contains no config,
endpoint, token, certificate, or arbitrary error text. Gyre Admin remains
disabled until Reef protection and audit are designed.

**Failure semantics.** Failed startup rolls back completed stages in reverse
order. `Close` is safe before `Start`, idempotent, and returns on caller
deadline while the single cleanup operation continues. Errors use Gyre codes.

**Observability.** Standard state/since/condition status plus existing
`coral_ready` and bounded readiness-state metrics.

**Test strategy.** `gyre.ConformanceCheck`, lifecycle ordering, repeated close,
typed errors, status endpoint, failed-listener rollback, full race suite.

**Done when.** No listener survives injected late startup failure; close before
start and repeated close are safe; Gyre endpoints and typed readiness are
tested; docs and platform matrix match code; CI is green on feature and main.

**Compatibility.** Additive except the Go toolchain floor moves from 1.26.3 to
Gyre v0.5.0's required 1.26.5. Reef v0.1.0 and all OTLP contracts are unchanged.

## Increment 2.1 — Reef v0.3 production security edges

Status: completed (implementation commit `17b007b`; pending merge).

Adopt Reef v0.3.0 before tenant identity: fail-closed external plaintext,
explicit bearer-over-plaintext risk acceptance, managed last-known-good
certificate/CA/token rotation, authenticated principal propagation,
origin-bound exporters, bounded credential metrics, and lifecycle cleanup.
This is a self-contained compatibility increment and does not make Reef
principals into tenants. See ADR 0003 and the Reef migration guide.

## Increment 3a — deadline-bounded drain and truthful delivery telemetry

Status: completed.

**Goal.** Make processing and draining observable and deadline-bounded before
adding durability or byte admission.

**Boundaries.** Separate receiver/run and processing/drain contexts; make the
public pipeline shutdown deadline effective; cancel blocked processors and
exporters on forced drain; report processor, exporter, and exporter-lane loss;
and distinguish processed, dispatched, and successfully returned delivery
counters. Cleanup continues in the background after a deadline.

**Public contracts.** Additive `coral_pipeline_*` metrics with the static
`signal` label. Existing `coral_spans_out`, `coral_metric_points_out`, and
`coral_log_records_out` remain for compatibility for at least the next
capability increment, but are explicitly defined as processed—not delivered.
`Shutdown(ctx)` remains idempotent and may now return a data-loss summary or
the caller deadline.

**Storage/migrations.** None.

**Security model.** Metrics use only the static traces/metrics/logs signal
values. Errors and metrics do not include payload attributes, credentials, or
raw destination endpoints.

**Failure semantics.** Shutdown stops admission and drains with the shutdown
context. Run-context cancellation no longer pre-cancels delivery. A forced
deadline cancels in-flight work and means delivery is indeterminate; it does
not claim rollback.

**Observability.** Processed, dispatched, delivered, processor failure, exporter
failure, exporter queue-drop, drain duration, forced state, and bounded drain
outcome by signal.

**Test strategy.** Race tests for enqueue/shutdown, deterministic blocked
exporter failure injection, run-cancellation drain tests, timeout tests,
idempotency, and application metrics integration.

**Done when.** A cancelled run context still permits graceful delivery, the
public shutdown call returns within its deadline when an exporter blocks,
repeated shutdown returns the same terminal result, and counters distinguish
processing from confirmed exporter success.

**Compatibility.** No config, OTLP, Gyre, Reef, Wisp, or storage contract
changes. Callers that ignored shutdown errors continue to compile; operators
should treat newly reported failures as actionable. See ADR 0004 and the
pipeline drain migration guide.

## Increment 3b — bounded admission and per-destination capacity

Status: in progress. The first slice validates pipeline worker and queue bounds
before application construction; byte admission and lane accounting follow.

**Goal.** Bound all in-memory telemetry retention in bytes as well as item
counts and make each fan-out destination diagnosable.

**Boundaries.** Add byte accounting/admission for input queues, exporter lanes,
batch processors, and tail sampling; expose per-destination lane
depth/capacity; validate numeric config bounds; and classify overload
consistently without changing OTLP payloads.

**Public contracts.** Additive capacity configuration and metrics. Zero values
retain documented defaults; negative and extreme values fail startup. Static
destination identifiers are configuration identities, never raw endpoints.

**Storage/migrations.** None.

**Security model.** Capacity labels use only static signal/destination
identifiers. Tenant quotas remain out of scope until authenticated
organisation/project identity exists.

**Failure semantics.** Byte-budget exhaustion is retryable OTLP
`Unavailable`/`503`. A permanently oversized individual item uses partial
success where the protocol permits; rejected elements are never counted as
accepted. Tail-sampler eviction and late-item behavior remain explicit.

**Observability.** Queue/lane bytes and items, capacity, admission rejection,
sampler bytes/traces/evictions/late items, and per-destination dispatch,
delivery, failure, and drop counts.

**Test strategy.** Byte-budget boundary tests for all signals, fan-out reference
accounting, deterministic overload tests over gRPC/HTTP, tail-sampler pressure
tests, config-bound tests, and race tests.

**Done when.** Every queue/sampler has count and byte bounds, overload behavior
is consistent over gRPC/HTTP, and a slow destination can be identified without
unbounded metric cardinality.

**Compatibility.** Existing safe configs remain valid. Newly rejected values
were previously unsafe or ambiguous and receive a migration note. No
Gyre/Reef/Wisp wire changes.

## Increment 4 — lossless standard OTLP trace path

The trace fidelity slice now preserves raw OTLP events, links, scope/schema,
flags, and dropped-field counts through the Fathom exporter. Full trace
assembly and sampling semantics remain in progress.

**Goal.** Preserve all standard OTLP trace fields before Wisp enables durable
traces.

**Boundaries.** Carry OTLP `ResourceSpans` losslessly through the default OTLP
path or extend the internal model to retain scope/schema, trace state, flags,
events, links, and dropped counts. Keep legacy Jaeger/Zipkin conversion
explicit. Version the S3 format or label it lossy.

**Public contracts.** Standard OTLP remains unchanged. Configured enrichment,
redaction, and sampling remain explicit processors. No implicit scope rewrite.

**Storage/migrations.** No Coral state. Amber/Fathom compatibility must be
verified against their currently supported OTLP fields before merge. S3 format
changes require a version marker and reader migration story.

**Security model.** Redaction covers newly preserved nested fields according to
documented policy. No field is silently dropped as “sanitization”.

**Failure semantics.** Unsupported permanent content is rejected with partial
success when item-scoped; otherwise a permanent protocol error. No
accept-and-drop.

**Observability.** Explicit counters for configured redaction, normalization,
sampling, legacy conversion loss, and rejected fields/items, using bounded
reasons.

**Test strategy.** Golden round-trip tests for every OTLP span field, fuzzing of
OTLP conversion/redaction, and cross-signal transport tests.

**Done when.** A maximal OTLP trace fixture is equivalent after
ingest/process/export except for configured transformations.

**Compatibility.** Wire-compatible and fidelity-increasing. Downstream contract
checks are mandatory; no Gyre/Reef/Wisp changes.

## Increment 5 — authenticated organisation/project identity

**Goal.** Establish a tenant context on every admitted request.

**Boundaries.** Add Coral-owned credential-to-organisation/project mapping or a
compatible identity adapter; propagate immutable tenant context through
admission, queues, journal keys, exporters, metrics, and audit events. Preserve
an explicitly configured single-tenant mode for migration.

**Public contracts.** New versioned config for organisations/projects and
credentials. Standard OTLP payloads stay unchanged. Tenant propagation to
Amber/Fathom requires prior compatibility analysis and versioned headers or
transport identity.

**Storage/migrations.** No telemetry state yet. Credential mapping reload and
rotation semantics are documented; Reef v0.1.0 bearer tokens currently require
restart.

**Security model.** Default-deny multi-tenant mode; constant-time secret checks;
token/mTLS identity cannot select another tenant through payload attributes;
per-project allowlists and audit-safe identifiers.

**Failure semantics.** Missing/invalid identity is permanent unauthenticated
(`Unauthenticated`/`401`). Unknown or disabled tenant is permission denied
(`PermissionDenied`/`403`). Authentication outages are fail-closed and
readiness-visible.

**Observability.** Accepted/rejected items by bounded tenant ID, signal, and
reason; authentication failures without tokens or unbounded remote addresses.

**Test strategy.** Cross-tenant isolation matrix over gRPC/HTTP, rotation tests,
fuzzing of identity config, and negative tests preventing payload-based tenant
spoofing.

**Done when.** Every admitted item has exactly one immutable organisation and
project, and no test can cross tenant boundaries.

**Compatibility.** Single-tenant compatibility mode accepts existing clients.
Multi-tenant activation is opt-in until migration is complete.

## Increment 6 — Wisp delivery identity and bounded deduplication

Implementation progress: optional header validation and a bounded two-phase
tenant/signal-aware dedup window are implemented for OTLP gRPC. HTTP lookup /
commit and dedup observability remain open.

**Goal.** Safely consume optional Wisp delivery metadata without excluding
standard OTLP clients.

**Boundaries.** Parse both headers on HTTP and gRPC metadata. Require
`x-wisp-envelope-id` to be exactly 32 hexadecimal characters when present.
Validate an optional signal kind against the actual endpoint. Compute a
canonical payload digest. Key dedup by organisation, project, signal, and
envelope ID. Use a time- and capacity-bounded window.

**Public contracts.** Headers remain optional and are not authentication. The
proposed default TTL is 24 hours, configurable within safe limits. Same key and
digest is an idempotent success; same key and another digest is a permanent
conflict. Clients without headers use normal admission with no dedup guarantee.

**Storage/migrations.** Start with a versioned dedup record schema suitable for
the following durable-journal increment. TTL/capacity changes require no
unbounded migration; expired records are garbage-collected.

**Security model.** Tenant and signal are server-derived; envelope IDs cannot
cross tenants or prove authenticity. Hashing resists payload disclosure and
collision misuse.

**Failure semantics.** Malformed/mismatched headers are
`InvalidArgument`/`400`. Conflicts are permanent and audited. Capacity or dedup
store unavailability is retryable and never bypasses duplicate safety in
strict mode. Duplicates can recur after TTL expiry, capacity eviction, or
state loss, and those boundaries are documented.

**Observability.** Dedup hits, misses, conflicts, evictions, store failures, and
record age by tenant/signal/reason without envelope-ID labels.

**Test strategy.** Parser fuzzing; HTTP/gRPC parity; standard clients without
headers; tenant/signal separation; same/different payload replay; TTL/capacity
tests; concurrent replay race tests.

**Done when.** All stated replay/conflict cases are deterministic and bounded,
and Wisp v0.8.x compatibility is verified without changing Wisp.

**Compatibility.** Additive for clients without headers. Present invalid
headers begin failing deliberately; release notes call this out.

## Increment 7 — durable admission journal

Implementation progress: bounded CRC-protected fsync journal, gRPC/HTTP append
before acknowledgement, explicit replay API, lifecycle close, and pressure
snapshot are implemented. Replay worker, compaction/age TTL, and crash-injected
end-to-end recovery remain open before marking this increment complete.

Replay worker and post-success compaction are now wired into App startup; the
remaining completion gates are process-level crash injection and age-based TTL
compaction.

Records written before routed envelopes are replayable only through the legacy
opaque `ReplayAdmission` callback. `ReplayRouted` rejects those records rather
than guessing a signal or tenant; migration must drain legacy records first.

**Goal.** Close the Wisp → Coral → Amber acknowledgement gap.

**Boundaries.** Persist accepted signal envelopes before OTLP success; replay
after restart; isolate destination delivery; delete/compact only after the
configured durable-admission policy is met. Bound disk by bytes/age and reserve
space for metadata/compaction.

**Public contracts.** Admission success means journal durability, not downstream
query visibility. At-least-once delivery permits duplicates at documented
crash/response-loss/TTL boundaries. The Wisp envelope contract from increment 6
remains optional.

**Storage/migrations.** Versioned records with tenant, signal, envelope identity
when present, payload hash, raw OTLP bytes, destination state, timestamps, and
checksum. Online forward migrations; rollback reads the previous schema during
one compatibility window. Corrupt records are quarantined, never silently
dropped.

**Security model.** Tenant-scoped files/keys, restrictive permissions,
optional encryption-at-rest design review, no secrets in filenames/logs, and
disk-exhaustion fail-closed behavior.

**Failure semantics.** Fsync/admission failure is retryable and not acknowledged.
Disk pressure rejects before exhaustion. Replay is ordered only where the
signal contract requires it. Partial downstream success retains only rejected
work when safely identifiable; otherwise it retains the envelope and documents
duplicate risk.

**Observability.** Journal bytes/records/oldest age, fsync/append/replay latency,
retries, corrupt/quarantined records, disk pressure, compaction, migration, and
per-destination pending state.

**Test strategy.** Crash at every append/fsync/ack/dispatch/compact boundary,
restart recovery, torn/corrupt record injection, disk-full/permission failures,
duplicate response loss, race tests, and parser fuzzing.

**Done when.** No acknowledged fixture is lost across injected process crashes,
disk is bounded, and recovery/migration/rollback tests pass.

**Compatibility.** Enabling durability requires a data directory and migration
guide. A temporary memory mode may remain for development but is never reported
durable or production-ready.

## Increment 8 — quotas, admission fairness, and complete OTLP partial success

**Goal.** Prevent one tenant or signal from exhausting shared capacity and make
every rejection protocol-correct.

**Boundaries.** Per-tenant/signal byte/rate/concurrency/cardinality quotas,
fair scheduling, request/header/decompression limits, and item-level validation
for logs/metrics/traces. Decode downstream OTLP partial success and integrate it
with journal state.

**Public contracts.** Versioned quota config/API. Retryable overload is
`ResourceExhausted`/`429` or `Unavailable`/`503` with consistent retry hints;
permanent item failures use OTLP partial success. Rejected items are never
acknowledged as admitted.

**Storage/migrations.** Quota state may use bounded checkpoints; journal schema
changes only if rejected sub-item identity is persisted.

**Security model.** Quotas are keyed by authenticated tenant, not attacker
labels. Admin overrides are audited and bounded.

**Failure semantics.** Atomic admission decides accepted/rejected work before
success. HTTP/gRPC classifications are equivalent. Unknown downstream partial
success is retained conservatively rather than dropped.

**Observability.** Admission latency, quota utilization/rejections, partial
success, request size, decompression ratio, and fairness lag with bounded labels.

**Test strategy.** Multi-tenant load/fairness tests, decompression bombs,
cardinality attacks, downstream partial-success fixtures, and protocol parity.

**Done when.** Capacity tests demonstrate isolation and every response matches
the documented admission state.

**Compatibility.** Default quotas are permissive within global safety bounds;
tightening is an operator-controlled policy change.

## Increment 9 — logs capability contract

**Goal.** Provide safe log admission and explicit downstream storage/query
integration without turning Coral into the log store.

**Boundaries.** Preserve OTLP logs; enforce attribute/body/record limits,
cardinality policy, configured redaction/privacy, tenant routing, and Amber
storage contract. Define stable platform query expectations outside Coral.

**Public contracts.** Standard OTLP plus versioned redaction/normalization
policy. No hidden field removal. Pagination/query limits belong to the platform
API/Amber contract and are compatibility-reviewed.

**Storage/migrations.** Coral journal only. Amber owns indexes, retention, and
backup migrations.

**Security model.** Tenant isolation, field allow/deny policy, audit of policy
changes, and no secret-bearing samples in self-observation.

**Failure semantics.** Permanent per-record violations use partial success;
privacy policy is fail-closed; configured redaction is counted.

**Observability.** Records/bytes accepted/rejected/redacted, field/cardinality
violations, downstream lag/failures, and retention contract health.

**Test strategy.** Golden fidelity/redaction tests, high-cardinality attacks,
large bodies, Unicode/binary values, restart recovery, and cross-tenant queries
at the downstream boundary.

**Done when.** Logs survive durable handoff with explicit privacy/cardinality
semantics and verified Amber retrieval/retention behavior.

**Compatibility.** Policy defaults preserve fields; enabling loss/redaction is
explicit and documented.

## Increment 10 — metrics capability contract

**Goal.** Admit metrics safely with bounded label cardinality and verified Amber
time-series semantics.

**Boundaries.** Preserve all OTLP metric types/temporality/exemplars; enforce
label/name/point limits and tenant quotas; define downstream query,
downsampling, and retention contracts.

**Public contracts.** Standard OTLP. Any normalization or temporality conversion
is versioned, configured, and observable. Query APIs remain outside Coral.

**Storage/migrations.** Coral journal only; Amber owns series/index/downsampling
schemas and migrations.

**Security model.** Tenant-derived routing, label-based tenant spoof prevention,
and bounded series creation.

**Failure semantics.** Invalid points use partial success; overload is
retryable; no silent temporality conversion or exemplar loss.

**Observability.** Points/bytes/series admitted, label-limit rejection,
temporality/type mix, downstream lag, and journal pressure.

**Test strategy.** Golden fixtures for every metric type, cardinality attacks,
out-of-order/duplicate points, exemplars, restart, and downstream query
contract tests.

**Done when.** Fidelity and bounded-series tests pass and Amber
downsampling/retention behavior is verified.

**Compatibility.** Fidelity-preserving defaults; limits roll out with observe
then enforce modes.

## Increment 11 — traces, assembly, correlation, and sampling

**Goal.** Add production trace semantics after lossless transport and durable
admission are established.

**Boundaries.** Tenant-aware trace assembly, correlation, explicit/tail sampling,
incomplete-trace policy, late spans, and retention integration. Sampling state
is byte-bounded and restart behavior is explicit.

**Public contracts.** Standard OTLP; sampling/redaction are explicit policy.
Trace completeness and decision horizons are documented. Wisp durable traces
require compatibility verification.

**Storage/migrations.** Journal plus bounded sampler state/checkpoints as needed;
Amber owns durable trace storage/index/retention.

**Security model.** Trace IDs do not cross tenant scope; sampling rules cannot
exfiltrate attribute values through metrics/logs.

**Failure semantics.** Incomplete/late traces and eviction are counted; restart
behavior does not silently force-keep unless policy says so.

**Observability.** Pending bytes/traces, keep/drop reason, forced/early decision,
late/incomplete spans, assembly latency, and downstream lag.

**Test strategy.** Deterministic clocks/RNG, late/out-of-order spans, eviction,
restart recovery, multi-tenant same-trace-ID tests, and high-volume bounds.

**Done when.** Sampling and assembly have documented completeness/duplicate
boundaries and all state is bounded/recoverable.

**Compatibility.** Existing tail-sampling config receives a migration path;
behavior changes are not silently applied.

## Increment 12 — platform API and administration support

**Goal.** Expose only the stable Coral administration surface needed by the
platform.

**Boundaries.** Versioned status/admission/migration APIs, pagination, query
cost limits, audit events, and RBAC integration. Telemetry search remains in the
platform API backed by Amber/Fathom.

**Public contracts.** Versioned API with cursors, bounded pages, explicit error
model, and deprecation policy.

**Storage/migrations.** Audit and admin state only; schema-versioned and
retention-bounded.

**Security model.** Authenticated service identity, least-privilege RBAC,
tenant-scoped reads, tamper-evident audit trail.

**Failure semantics.** No partial unscoped results; stale cursors and expensive
queries fail explicitly.

**Observability.** API latency/errors/cost, authorization decisions, audit
write failures, and pagination limits.

**Test strategy.** Contract/golden tests, RBAC matrix, cursor fuzzing,
cross-tenant isolation, and audit failure injection.

**Done when.** The API is versioned, bounded, audited, and integrated without
duplicating telemetry query ownership.

**Compatibility.** Additive v1 administration namespace; no Gyre v0.5.0 change
without separate compatibility work.

## Increment 13 — production depth and horizontal scale

**Goal.** Operate Coral predictably across upgrades, failures, and capacity
growth.

**Boundaries.** Horizontal routing/affinity, rolling migrations, backup/restore
for journal/admin state, disaster recovery, SLOs, capacity model,
upgrade/rollback, and operational runbooks.

**Public contracts.** Deployment topology, health/readiness, SLO, and supported
upgrade matrix become release artifacts.

**Storage/migrations.** Online forward migration, tested rollback window,
backup verification, restore drills, and corrupt-node replacement.

**Security model.** Node/service identity, encrypted transport/state as required,
key rotation, supply-chain provenance, and least-privilege runtime.

**Failure semantics.** Node loss, partition, clock skew, and split-brain
duplicate boundaries are explicit. No acknowledged data is lost within the
declared durability model.

**Observability.** SLI/SLO dashboards, capacity headroom, migration progress,
replication/routing state, recovery point/time, and version skew.

**Test strategy.** Multi-node chaos, upgrade/rollback matrices, backup restore,
partition/disk/clock faults, long soak, and capacity certification.

**Done when.** SLO and disaster-recovery exercises pass at a documented scale,
and main-commit artifacts have verified provenance.

**Compatibility.** N-1 rolling upgrade support is defined and tested; migration
and rollback limits are published per release.
