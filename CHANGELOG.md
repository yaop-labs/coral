# Changelog

All notable Coral capability increments are documented here. Coral does not use
version numbers as a countdown to v1.0; a release is cut only for a complete,
tested, documented increment.

## Unreleased

### Added

- Bounded per-tenant request-rate quotas (`max_requests_per_second`) with
  tenant-isolated rejection counters; concurrency quota accounting preserves
  existing accepted/rejected totals.
- Classified tenant admission overload consistently as gRPC `ResourceExhausted`
  and HTTP `429`, without changing pipeline/storage failure responses.
- Preserved distinct concurrency and request-rate rejection reasons through
  the shared admission path for diagnostics and protocol responses.
- Journal admission now rejects oversized tenant/signal routing fields instead
  of allowing one-byte length truncation and replay corruption.
- Journal replay rejects oversized individual records before allocation and
  includes fuzz coverage for untrusted envelope bytes.
- Journal append enforces the same per-record bound, preventing records that
  would be accepted but unreplayable after restart.
- Fixed dedup window lock ownership so Wisp envelope check/lookup cannot
  self-deadlock; tenant/signal scoping remains covered by race-tested tests.
- Added deterministic TTL-expiry and bounded-eviction coverage for the dedup
  window.
- Corrected roadmap status to reflect the implemented HTTP dedup lookup/commit
  path and bounded hit/conflict observability.
- Added backward-compatible journal v3 delivery identity fields and replay
  restoration into the bounded Wisp dedup window for gRPC and HTTP admissions.
- Added an end-to-end replay regression test proving restored delivery IDs
  produce dedup hits before new admission.
- Added fuzz coverage for malformed Wisp envelope and signal headers.
- Documented the implemented 15-minute/100,000-entry dedup boundary instead of
  the stale 24-hour proposal.
- Added bounded dedup miss and capacity-eviction counters to the server stats.
- Added configurable per-tenant `max_log_record_bytes`; oversized records are
  rejected before sink/journal with permanent `InvalidArgument`/`400` semantics.
- Added configurable per-tenant `max_log_attributes` for resource, scope, and
  record attributes, with the same fail-before-acknowledgement semantics.
- Added configurable per-tenant `max_log_attribute_keys` to bound distinct
  attribute-key cardinality across each log request.
- Added `coral_otlp_log_limit_rejected` self-observability counter, separate
  from partial-success record rejects and quota overload.
- Exported bounded configured-tenant accepted/rejected/quota-rejected counters
  from `/metrics` with deterministic ordering and escaped labels.
- Exported bounded Wisp dedup hits, conflicts, misses, and evictions from
  `/metrics`, without envelope-ID labels.
- Reconciled Increment 9 progress: bounded log admission is implemented;
  per-record partial-success aggregation and downstream retrieval remain open.
- Added tenant `max_metric_attributes` admission bound using lossless OTLP
  reflection; violations are rejected before sink/journal.
- Added tenant `max_metric_attribute_keys` to bound distinct metric label-key
  cardinality per request.
- Added tenant `max_metric_series` to bound metric descriptor admission per
  request before downstream handoff.
- Reconciled Increment 10 progress with the implemented metric bounds and
  observability; series/temporality/Amber work remains explicitly open.
- Metric redaction now also scrubs exemplar filtered attributes, preserving the
  configured privacy policy across all supported metric datapoint types.
- Added end-to-end Amber exporter coverage proving metric exemplar fields are
  preserved through Coral processing.
- Fixed tail sampler byte accounting when max-trace eviction occurs; added a
  regression test for bounded pending state.
- Fixed tail sampler byte accounting when traces age out at the decision
  horizon, preventing stale pending-byte pressure after normal flush.
- Added Amber exporter coverage proving cumulative Sum temporality and
  monotonicity are preserved without implicit conversion.
- Marked the Wisp delivery identity/dedup capability complete; no release tag
  is created until a user-visible release increment is bundled.

- Architecture review, responsibility-boundary ADR, and capability roadmap.
- Process build identity via `--version`, startup logs, and
  `coral_build_info`.
- Readiness state/reason and per-signal input queue depth/capacity metrics.
- CI gates for formatting, vet, lint, race tests, fuzz smoke, and reproducible
  packages.
- Deterministic cross-platform release archives with SHA-256 checksums.
- Gyre v0.5.0 component lifecycle, typed readiness, and secret-free
  `/status`.
- Lifecycle conformance tests covering close-before-start, repeated close, and
  rollback of listeners after a failed startup.
- A versioned platform compatibility matrix for Gyre, Reef, Wisp, Amber, and
  Fathom boundaries.
- Reef v0.3 production edges for OTLP, self-observability, and all HTTP
  exporters.
- Managed last-known-good certificate, CA, and bearer-token reload with
  bounded credential event/generation metrics.
- Authenticated Reef principal propagation and exporter origin/redirect
  containment.
- Bounded pipeline admission by item count and bytes across traces, metrics,
  logs, and exporter lanes.
- Lossless OTLP trace metadata preservation through Fathom, including raw
  events, links, scope/schema, flags, and dropped-field counts.
- Tenant principal allowlists, per-tenant item/byte request limits, and
  explicit tenant context propagation.
- Optional Wisp envelope validation and bounded tenant/signal-aware two-phase
  deduplication with hit/conflict counters.
- Bounded CRC-protected fsync admission journal with replay and pressure
  snapshots.
- Routed journal envelopes with startup replay, post-replay compaction, age
  TTL compaction, and process-crash recovery coverage.

### Changed

- HTTP operational and OTLP servers now set explicit header/read/write/idle
  timeouts and header limits.
- Partial startup now rolls back completed stages in reverse order; shutdown is
  idempotent and bounded at the public Gyre boundary.
- The minimum Go toolchain is 1.26.5, matching Gyre v0.5.0.
- External plaintext now requires `insecure: true`; bearer over plaintext also
  requires `danger_allow_bearer_over_plaintext: true`.
