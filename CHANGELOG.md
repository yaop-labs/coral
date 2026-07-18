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
