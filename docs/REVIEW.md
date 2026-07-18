# Coral architecture and engineering review

Review date: 2026-07-18  
Reviewed commit: `35f920f` on `fix/contract-conformance`  
Working baseline: `feat/operational-baseline`

## Executive result

Coral is a small Go collector (about 12.4k lines including tests) with a useful
OTLP/legacy ingestion and fan-out core. The current branch materially improves
the repository baseline: one OTLP endpoint serves all three signals, Reef
provides TLS/bearer protection, HTTP payloads are bounded, exporter retries are
classified, fan-out queues are isolated, trace oversize rejection is returned
as OTLP partial success, and the race test suite passes.

It is not production-ready. The largest blockers are the non-durable
acknowledgement gap, absence of tenant identity/isolation, count-only memory
bounds, lossy OTLP trace conversion, missing Wisp envelope semantics, incomplete
partial-success propagation, shutdown data loss, and the absence of CI/release
metadata.

Facts below are statements verified in code or repository state. Proposed
directions are explicitly labelled as decisions or assumptions.

## Verified capabilities

- One optional gRPC and one optional HTTP OTLP ingress serve traces, metrics,
  and logs (`internal/receiver/otlp/ingress.go:139-227`).
- OTLP/HTTP supports protobuf, JSON, and gzip with compressed/decompressed
  request bounds (`internal/otlphttp/read.go`).
- gRPC has a 16 MiB default receive limit
  (`internal/receiver/otlp/ingress.go:34,111-168`).
- TLS, optional client certificates, and bearer authentication are wired through
  Reef for OTLP receivers and HTTP exporters
  (`internal/config/config.go:68-73,167-174,206-214,354-362`).
- Traces, metrics, and logs have independent worker pipelines and each exporter
  has an independent bounded-by-count lane
  (`internal/pipeline/pipeline.go:35-69,109-140,223-249`).
- Retry classification distinguishes permanent and transient HTTP failures,
  uses jitter, and honours retry hints (`internal/exporter/backoff`).
- An oversized trace span can be rejected before enqueue and reported through
  OTLP partial success (`internal/app/app.go:170-210`;
  `internal/receiver/otlp/ingress.go:304-423`).
- Metrics and logs remain in their OTLP protobuf representation through their
  pipelines; configured service-name and redaction processors mutate them
  explicitly (`internal/metric`, `internal/logs`).
- Legacy Jaeger Thrift and Zipkin trace receivers exist. The Thrift decoder has
  extensive malformed-input tests and UDP panic containment.
- `/healthz`, `/readyz`, and Prometheus text metrics exist when the self-observe
  endpoint is configured (`internal/app/app.go:416-484`).
- Config parsing rejects unknown top-level typed fields using
  `yaml.Decoder.KnownFields(true)` (`internal/config/config.go:224-234`).
- Baseline verification on 2026-07-18 passed `gofmt`, `go vet ./...`,
  `go build ./...`, and `go test ./... -race -count=1`.

## Production blockers and risks

### P0 — acknowledged data can be lost

`admit*` returns success after `Pipeline.Enqueue` places a batch in a memory
channel (`internal/receiver/otlp/ingress.go:306-357`). Export happens later.
Wisp can therefore retire a durable envelope before Amber has admitted it. A
Coral crash loses the acknowledged queue. The README's edge-durability wording
does not close this protocol gap.

Decision: add a bounded durable handoff journal and acknowledge only after the
journal admission contract is satisfied. Until then, Coral is at-most-once
after its ingress acknowledgement boundary.

### P0 — authenticated principal is not yet tenant identity

At the reviewed baseline, Reef v0.1.0 exposed neither the matched key name nor a
tenant identity. The Reef v0.3 security increment now propagates the configured
bearer key name as an authenticated principal over HTTP and gRPC. Coral's
`Sink` and all signal batches still have no immutable organisation/project
context, mapping policy, per-tenant quotas, queues, storage keys, or metrics.

Authentication can now answer “which configured credential matched?”, but not
yet “which organisation/project owns this request?”. This still blocks safe
tenant-aware deduplication, quotas, auditing, and multi-tenant deployment.

### P0 — OTLP traces are silently lossy

OTLP traces are converted to `model.Span` on ingress and reconstructed on
export. The model does not retain instrumentation scope/schema URL, trace state,
flags, events, links, dropped counts, or the original scope grouping
(`internal/receiver/otlp/convert.go:13-39`;
`internal/exporter/amber/convert.go:20-60`). Export sets scope name to `coral`.
This violates the requirement that field loss or normalization be explicit and
observable, and will affect Wisp's future durable trace path.

S3 is intentionally even narrower: its JSONL format retains a subset of span
fields and only `service.name` from resource data
(`internal/exporter/s3/exporter.go:119-153`). That format has no version marker.

### P0 — memory is not bounded in bytes

The pipeline queue and every exporter lane are sized only in batch count and
default to 10,000 (`internal/pipeline/pipeline.go:19-25,63-69,109-113`).
An OTLP request may be up to 16 MiB. Multiple signal pipelines and fan-out lanes
multiply retained payload references. Tail sampling separately retains up to
100,000 traces by default, also without a byte or span limit
(`internal/processor/sampling/tail.go:61-105`).

This is bounded in object count but not by a safe capacity model. Queue length,
fan-out count, payload size, and tail-sampling retention can combine into
unacceptable memory use.

### P0 — Wisp delivery identity is ignored

There is no reference to `x-wisp-envelope-id` or `x-wisp-signal-kind`. Requests
without the headers work, which is required, but present headers are neither
validated nor used for tenant/signal-aware bounded deduplication. Same-ID,
different-payload conflicts cannot be detected.

### P1 — downstream partial success is discarded

HTTP exporters treat every status below 300 as complete success and drain the
body without decoding an OTLP response (`internal/exporter/amber/exporter.go`;
`internal/metric/amber.go`; `internal/logs/fathom.go:62-79`). If Amber returns
OTLP partial success, Coral silently treats rejected items as accepted. There is
no way to retry only rejected elements because the journal/item identity model
does not exist yet.

### P1 — shutdown can discard queued data

The runtime context passed to `App.Start` is cancelled by the signal before
`App.Shutdown` creates its 15-second context (`cmd/coral/main.go:46-59`).
Workers and exporter lanes keep using the cancelled runtime context
(`internal/pipeline/pipeline.go:109-140,197-249`). Queue draining can therefore
invoke exporters with an already-cancelled context. `Shutdown` also waits on
worker/exporter wait groups without selecting on its own deadline
(`internal/pipeline/pipeline.go:145-183`).

Stateful processor `Close` methods ignore emit errors and use
`context.Background` (`internal/processor/batch.go:63-80`;
`internal/processor/sampling/tail.go:212-230`), so shutdown is neither bounded
nor reliably reported.

### P1 — observability can overstate delivery

`itemsOut` increments after dispatch attempts even if every non-blocking
exporter lane was full (`internal/pipeline/pipeline.go:223-250`). It measures
items processed to fan-out, not items exported. The public name and comments say
“out/exported”. Exporter drops are aggregated across destinations, preventing
source-of-truth versus derived-destination diagnosis.

Missing required signals include request/export latency, queue depth, retry
attempts, per-reason rejection/drop counters, partial success, storage
failures, disk pressure, active migrations, readiness reason, and build
metadata. Existing counters have no tenant dimension because identity is absent.

### P1 — admission validation is incomplete

- Trace size partial success exists only when a `validate` processor is
  configured; metrics and logs have no item admission rules.
- OTLP semantic validity (ID lengths/zero IDs, timestamp relationships, metric
  shape) is largely delegated to protobuf decoding.
- `x-wisp-*` headers are not validated.
- Config validation checks broad component types but does not consistently
  validate nested processor/exporter fields at parse time. Custom `yaml.Node`
  decoding bypasses the top-level `KnownFields` guarantee.
- Worker, queue, sampling, batch, timeout, endpoint, retry, and regex counts have
  no coherent upper/lower bound policy.
- Zipkin HTTP limits the reader to 16 MiB but does not distinguish exact
  overflow with `413` (`internal/receiver/zipkin/http.go`).

### P1 — tail sampling has explicit but under-observed loss boundaries

Tail sampling is bounded by trace count, not bytes/spans. Eviction forces an
early decision. Late spans use a finite decided LRU; after eviction the same
trace can receive a new, different decision (`internal/processor/sampling/tail.go`).
Close force-keeps all pending traces, so shutdown changes sampling semantics.
There are no counters for forced decisions, late spans, evictions, or kept/dropped
items.

### Resolved baseline finding — lifecycle metadata, CI, and release artifacts

The reviewed commit had no GitHub workflow, release script, artifact checksum,
changelog, or build/version injection. Increment 1 added build identity,
readiness/queue metrics, CI gates, and deterministic cross-platform archives.
The final `main` commit
`d99a4dbe21fd9c3562936a763e66bb9ae1dec1ee` passed GitHub Actions run
`29625160872`.

Gyre lifecycle conformance was still absent at that baseline. Increment 2 now
tracks the direct Gyre v0.5.0 component adoption; the cross-platform contract
split is documented in `docs/PLATFORM_COMPATIBILITY.md`.

The module declares Go 1.26.3. Offline dependency enumeration succeeds, but the
network-based available-update/retraction check could not run in the restricted
audit environment.

### P2 — operational API has weak server hardening

The self-observation server has no read/header/idle timeouts and no explicit
access-control model (`internal/app/app.go:416-436`). Readiness is one boolean,
with no reason. Ingress HTTP has `ReadTimeout` but not explicit
`ReadHeaderTimeout`, `WriteTimeout`, `IdleTimeout`, or maximum header bytes.
Serve-loop failures are only logged and do not transition readiness.

## Tests and engineering depth

There are roughly 167 test/fuzz/benchmark functions across 29 test files.
Unit and integration coverage is strongest around config, Thrift parsing,
pipeline lifecycle, tail sampling, and unified OTLP transport. Race tests pass.

Gaps:

- only config has a fuzz target; externally supplied OTLP/JSON/gzip, Zipkin,
  Jaeger Thrift, header parsing, and future journal records need fuzzing;
- no crash/restart recovery tests because no journal exists;
- no disk-full, corrupt-record, short-write, downstream partial-success, or
  retry exhaustion failure injection;
- no byte-capacity tests across request, queue, sampler, and fan-out;
- no multi-tenant isolation tests;
- several timing tests use sleeps and may be flaky under load;
- no CI currently runs any gate.

## Architecture conclusion

The reusable core is worth evolving; a rewrite is not justified. The generic
pipeline, unified ingress, protocol helpers, Reef integration, and existing test
base provide useful seams. The next work should harden those seams in small
capability increments rather than introduce storage/query functionality into
Coral.

The accepted role and boundaries are recorded in
`docs/adr/0001-coral-role-and-boundaries.md`. The implementation sequence and
compatibility story are in `docs/ROADMAP.md`.

## Assumptions requiring later validation

- Amber can provide an unambiguous durable-admission response for each signal.
  Its current contract and version must be checked before durable ACK semantics
  are implemented.
- A future platform identity source can supply stable organisation/project IDs
  and credential mappings. Gyre v0.5.0 is explicitly not that authority; it
  owns operational lifecycle/resource contracts, while Reef owns transport
  security.
- A 24-hour default Wisp deduplication TTL is likely adequate for Wisp retry and
  restart horizons. It is a roadmap proposal, not a current contract.
- A single-node journal is a useful first durable increment; horizontal scaling
  may require affinity or a shared consistency layer.
