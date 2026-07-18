# ADR 0002: adopt Gyre lifecycle without merging security responsibilities

- Status: accepted
- Date: 2026-07-18
- Decision owners: Coral maintainers

## Context

Gyre v0.5.0 defines the platform operational contract: component lifecycle,
readiness/status, typed errors, reload generations, and resource identity. Reef
owns TLS, mTLS, bearer credentials, rotation, and edge policy. Coral previously
implemented local readiness and shutdown behavior that resembled Gyre but was
not conformant:

- closing an unstarted OTLP ingress could block forever;
- a late startup failure did not roll back resources started earlier;
- readiness had no standard status snapshot or typed error;
- operational endpoint behavior was duplicated locally.

Treating Gyre as an organisation/project authority would also be incorrect.
Neither Gyre's component API nor its resource merge contract resolves an
authenticated telemetry tenant.

## Decision

`internal/app.App` directly implements `gyre.Component` at Gyre v0.5.0.
Direct implementation keeps lifecycle ownership in Coral and avoids a second
adapter state machine.

Coral:

- exposes the stable identity `coral` and its build version;
- uses Gyre lifecycle states and one bounded `Ready` condition;
- rolls back completed startup stages in reverse order;
- makes `Close` safe before start, idempotent, and bounded by the caller's
  context;
- retains `Shutdown` as a compatibility alias;
- mounts `gyre.HTTPHandler` beside Coral metrics;
- returns Gyre typed errors from lifecycle/readiness boundaries.

Coral does not adopt Gyre Runtime, Admin, or Reloadable in this increment.
There is only one top-level component, so Runtime adds no useful dependency
ordering. Admin requires a Reef-protected authorization and audit design.
Reloadable requires transactional replacement and monotonic last-known-good
configuration generations.

Coral continues using Reef v0.1.0 for existing transport behavior. A Reef
v0.3.0 upgrade is a separate security capability with explicit config and
policy compatibility tests.

## Failure semantics

If startup fails, already-started stages are stopped in reverse order using an
independent five-second cleanup deadline. The original failure and cleanup
failure are preserved in a typed retryable `unavailable` error.

`Close` starts cleanup once. If its context expires, it returns a retryable
`shutting_down` error while that same cleanup continues; repeated calls wait
on the same completion. Successful cleanup transitions to `stopped`; cleanup
errors transition to `failed`.

Liveness remains cheap. Readiness is true only in the Gyre `ready` state.
Status messages are static and secret-free rather than propagating arbitrary
dependency errors.

## Consequences

- Gyre v0.5.0 becomes a direct dependency and the module requires Go 1.26.5.
- Existing `/healthz` and `/readyz` status codes remain compatible; `/status`
  is additive.
- Existing callers of `Shutdown` continue to compile.
- Static Coral config reports generation zero and does not claim reload
  support.
- Tenant identity, Wisp deduplication, and Reef credential rotation remain
  separate roadmap capabilities.

## Verification

The product suite runs `gyre.ConformanceCheck` and adds Coral-specific tests
for close-before-start, repeated close, typed lifecycle failures, status
serialization, and listener cleanup after failed startup. Full race testing is
required before merge.
