# Changelog

All notable Coral capability increments are documented here. Coral does not use
version numbers as a countdown to v1.0; a release is cut only for a complete,
tested, documented increment.

## Unreleased

### Added

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

### Changed

- HTTP operational and OTLP servers now set explicit header/read/write/idle
  timeouts and header limits.
- Partial startup now rolls back completed stages in reverse order; shutdown is
  idempotent and bounded at the public Gyre boundary.
- The minimum Go toolchain is 1.26.5, matching Gyre v0.5.0.
