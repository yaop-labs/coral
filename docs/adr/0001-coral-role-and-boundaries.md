# ADR 0001: Coral role and responsibility boundaries

- Status: accepted
- Date: 2026-07-18
- Decision owners: Coral maintainers

## Context

Coral currently accepts OTLP traces, metrics, and logs, plus legacy
Jaeger/Zipkin traces. It processes data in count- and estimate-bounded queues and
fans out to Amber, Fathom, S3, or devnull. With `journal_path`, Coral now fsyncs
the accepted post-admission envelope before enqueue and retains it until the
required Amber destination accepts the resulting work.

Wisp has a durable, at-least-once spool and Amber is intended to own durable
telemetry storage. Those edge guarantees alone did not close the handoff gap.
Coral's journal now owns that interval with live redispatch, permanent-failure
quarantine, bounded durable Wisp receipts, and failure-state observability.

Coral also has no query API, retention engine, storage schema, organisation or
project model, tenant propagation, or UI API. Adding all of those here would
duplicate Amber and platform API responsibilities.

## Decision

Coral is the platform's telemetry admission and routing boundary:

1. terminate OTLP transport security and authenticate the caller;
2. resolve an authenticated organisation/project identity;
3. enforce request, tenant, signal, cardinality, and resource limits;
4. validate OTLP and return protocol-correct retryable, permanent, and partial
   success outcomes;
5. provide a minimal durable admission journal so an acknowledged batch
   survives restart until downstream durable admission;
6. perform only configured, observable processing such as redaction,
   enrichment, and sampling;
7. route each signal to independently isolated downstream destinations;
8. use an optional Wisp envelope ID as delivery identity, never as
   authentication.

The durable journal is a handoff mechanism, not a telemetry database. It is
bounded by bytes and age, tenant-aware, signal-aware, restart-safe, and
garbage-collected after downstream admission according to documented delivery
semantics.

Coral does not own:

- host collection, eBPF, or profiles production (Wisp);
- long-term telemetry storage, indexing, retention, backup, or query (Amber);
- incident analysis and derived indexes (Fathom);
- platform control-plane APIs, UI aggregation, or global RBAC policy;
- Reef, Gyre, Wisp, Amber, or Fathom public contracts.

Coral may expose a narrow administration API for its own admission state,
migrations, and audit data. Any broader API requires a separate ADR.

## Consequences

- Individual journal records are removed only after every required Amber
  contribution completes, or after an atomic move to permanent-failure
  quarantine. Safe compaction retains all unrelated active records.
- Tenant identity must precede tenant-aware deduplication.
- Standard OTLP clients without Wisp headers remain supported.
- `x-wisp-envelope-id` is scoped by tenant and signal and must be backed by a
  bounded TTL/capacity policy. A repeated ID with another payload is a conflict.
- Storage and query roadmap items describe downstream contracts and integration,
  not a new Coral telemetry database.
- Horizontal scaling requires shared routing or a journal/dedup design whose
  consistency boundaries are explicit.

## Compatibility

This ADR does not change a wire or configuration contract. Later increments
must document compatibility and migration individually. No Gyre, Reef, or Wisp
contract may be changed implicitly.
