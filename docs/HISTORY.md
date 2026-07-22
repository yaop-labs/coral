# Coral engineering history

This document consolidates the work that led to the current Coral baseline.
It is a capability ledger, not a claim that every historical roadmap item is
complete. The current release assessment lives in `docs/REVIEW.md`; the work
remaining for a stable release lives in `docs/ROADMAP.md`.

## Sources

The history can be reconstructed from four independent records:

- `../../platform-review/coral.md` and `../../platform-review/contract.md`
  contain
  the cross-product review and contract decisions from 2026-07-07;
- the repository-root `REVIEW.md` is the detailed 2026-07-09/10 code review
  performed with the same angles-first method used for Wisp;
- Git contains the implementation sequence, review branches, pull-request
  merges, and per-capability documentation commits;
- `CHANGELOG.md` records the user-visible result of the capability run.

The old review documents are historical snapshots. Statements such as “no
journal”, “no Wisp delivery identity”, or “branch not merged” were true at the
reviewed commit and must not be read as current status.

## Timeline

### 2026-05-18 through 2026-06-29 — repository bootstrap

The initial collector, trace pipeline, and metrics path were imported and the
module was renamed to `github.com/yaop-labs/coral`.

### 2026-07-07 — platform review and contract

The first cross-product pass compared Coral with Wisp, Amber, and Fathom. It
identified the original contract failures: six OTLP ports, logs not reaching
Amber, unclassified retry, no TLS/auth, duplicated signal pipelines, lossy
trace handling, weak self-observation, and the old `cros` name.

The same pass established the platform decisions that shaped the fixes:

- Coral owns the standard OTLP ingress on 4317/4318 for all signals;
- Amber is the durable source of truth and Fathom is an isolated derived
  fan-out;
- HTTP/protobuf, HTTP/JSON, and gzip have explicit protocol behavior;
- retryable overload, permanent rejection, and OTLP partial success are
  distinct outcomes;
- Reef owns transport security and Gyre owns component lifecycle;
- Wisp delivery identity is optional metadata, never authentication.

### 2026-07-09/10 — full code review and contract-conformance fixes

The production code was reviewed package by package using the Wisp review
method. Fifteen focused commits built the first coherent Coral baseline:

- logs gained an Amber destination;
- retry classification and jittered backoff were shared;
- `cros` was renamed to Fathom;
- OTLP/HTTP gained JSON, gzip, request limits, and protocol status handling;
- trace enrichment and credential redaction were corrected;
- self-observation moved to `coral_*` metrics and port 4888;
- three duplicated pipeline cores became one generic pipeline;
- traces, metrics, and logs moved to one OTLP gRPC/HTTP ingress;
- accept-time trace rejection gained OTLP partial success;
- legacy Jaeger/Zipkin parsing and service-name normalization were hardened.

The work was merged to `main` through pull requests on 2026-07-15. The remote
branch `fix/contract-conformance` remains only as merged history.

### 2026-07-15 — secure edges and isolated fan-out

Coral integrated Reef on OTLP edges, moved each exporter to an independent
bounded lane, applied shared retry defaults, and tightened payload limits and
redaction. These changes were also merged to `main`.

### 2026-07-18 — capability roadmap run

The architecture review and the original thirteen-increment roadmap were
created at commit `0263ed2`. The same day produced the bulk of the current
implementation: 145 commits on the repository history date, including the
following milestones.

| Milestone | Result in current `main` |
| --- | --- |
| Operational baseline | build identity, CI, race/lint/fuzz gates, deterministic packages, release workflow |
| Gyre v0.5 | component lifecycle, status/readiness, bounded and idempotent close |
| Reef v0.3 | fail-closed OTLP and self-observation edges, managed credentials, origin-bound HTTP exporters |
| Bounded pipeline drain | separate receive/drain lifetime and truthful aggregate delivery counters |
| Bounded admission | count and byte budgets on input queues and exporter lanes |
| Trace fidelity work | raw OTLP retained in the internal span model; Fathom path preserves scope, events, links, flags, and dropped counts |
| Tenant admission | principal allowlist, configured tenant mapping, item/byte/concurrency/rate limits, bounded counters |
| Wisp delivery metadata | strict headers, tenant/signal-keyed process-local dedup, conflict detection, bounded TTL/capacity |
| Admission journal | bounded checksummed fsync log, routed envelopes, recovery and corruption/failure tests |
| Signal admission limits | log record/attribute bounds and metric attribute/key/descriptor bounds |
| Tail sampling | trace/byte bounds, lifecycle fixes, tenant-key data structure, and process-local metrics |

These commits are all ancestors of `main`; the old feature branches do not
need to be merged again.

## Current baseline

The current baseline is commit `f0868e1` from 2026-07-18. It has no release
tag. Coral pins Gyre v0.5.0 and Reef v0.3.0. Wisp has since reached v0.11.0,
so older compatibility text referring to Wisp v0.7/v0.8 is obsolete.

The capability run substantially improved Coral, but several roadmap status
updates got ahead of end-to-end behavior. The 2026-07-22 re-baseline corrected
the Amber trace converter to preserve the raw OTLP fields retained by the model
and identified delivery ownership and tenant routing as the next stabilization
work. The remaining gaps are current release work, not missing history.

### 2026-07-22 — Gate 1 delivery ownership

The first Gate 1 implementation gave every durable envelope an internal record
ID, changed ingress to fsync canonical post-admission OTLP before enqueue, and
carried item-level ownership and tenant metadata through batching,
tail-sampling, and fan-out. Amber became the required destination; Fathom, S3,
and devnull remained best-effort. A record is now reclaimed with an fsynced
same-directory atomic replacement only after all of its child units complete
at every required exporter. Required failures and capacity evictions retain
the record. Graceful shutdown now closes the journal only after pipeline and
required-exporter drain, so terminal acknowledgements are not stranded.

The closing slice added bounded jittered redispatch directly from the active
journal, generation-safe completion, durable receipt and quarantine sidecars,
and startup reconciliation for crashes between a sidecar fsync and active
record removal. Permanent downstream rejection, including non-zero Amber OTLP
partial success, is quarantined with a bounded reason; transient failure stays
live until delivery succeeds. Wisp response-loss retries are suppressed across
restart by a bounded receipt ledger keyed by mapped tenant, signal, delivery
ID, and canonical request digest. Pressure, retries, pending acknowledgements,
and quarantine now affect readiness and expose bounded metrics. Race-tested
failure injection covers corrupt/truncated tails, capacity exhaustion, fsync
permission failures, crash recovery, compaction retention, and interrupted
receipt/quarantine transitions. Gate 1 is closed; no release version is implied.

## Verification record for this re-baseline

On 2026-07-22 at `f0868e1`:

- `go vet ./...` passed;
- `go test ./... -race -count=1` passed;
- the config fuzz target passed a 10-second run when its cache was placed in a
  writable temporary directory;
- golangci-lint v2.12.2 reported zero issues when its caches were placed in a
  writable temporary directory;
- two Linux/amd64 packages built from identical version/revision inputs were
  byte-for-byte identical;
- the Coral-to-Fathom three-signal gate passed with 2,000 traces, 16,000 spans,
  2,000 metric points, 345 logs, and all indexed-readiness criteria true.

These are strong regression signals. They do not close the durability and
contract gaps listed in the current review.
