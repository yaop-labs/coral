# Coral platform compatibility

Updated: 2026-07-18

This matrix separates implemented compatibility from planned integration. A
platform repository being locally available does not make an untagged contract
part of Coral.

| Component | Contract/version | Coral relationship | Current status |
|---|---|---|---|
| Gyre | v0.5.0 | Operational lifecycle, readiness, status, typed errors, and resource identity | `App` implements `gyre.Component`; standard health/readiness/status endpoints are mounted. Runtime, reload, admin, and resource layering are not adopted yet. |
| Reef | v0.1.0 | TLS, mTLS, bearer verification, and client transport construction | Used by OTLP ingress and HTTP exporters. Authentication proves only that a configured token matched; it does not produce an organisation/project principal. |
| Wisp | v0.7.0 stable; v0.8.x delivery contract under development | Durable edge sender of standard OTLP | Standard OTLP works without Wisp headers. `x-wisp-envelope-id` and `x-wisp-signal-kind` are not consumed yet; deduplication is therefore not claimed. |
| Amber | Contract requires separate verification | Durable telemetry destination | Coral exports to Amber, but acknowledgement after Coral ingress is not proof of Amber durable admission. |
| Fathom | Contract requires separate verification | Derived/analysis destination | Coral exports independently from the Amber lane; it does not assign Fathom storage or query responsibilities to itself. |

## Gyre boundary

Gyre is the shared operational contract, not the tenant control plane and not a
telemetry transport. Coral vNext adopts these v0.5.0 semantics:

- stable component identity `coral` and release version;
- lifecycle states and a bounded, secret-free status snapshot;
- `Close` is safe before `Start`, repeatable, and returns on caller
  cancellation/deadline while cleanup continues;
- partial startup is rolled back in reverse order;
- readiness failures use Gyre typed errors;
- `/healthz`, `/readyz`, and `/status` use `gyre.HTTPHandler`.

Coral deliberately does not mount Gyre Admin endpoints. They would expose
configuration operations and must first be protected through a reviewed Reef
edge and audit model. Coral also does not implement `gyre.Reloadable` yet:
accepted generations need transactional pipeline replacement, last-known-good
preservation, and tests before that contract can be claimed.

Gyre `Resource` merging is reserved for a versioned Coral resource/config
increment. It must reject conflicting `service.name` values and must not
silently rewrite incoming OTLP resource identity.

## Reef boundary

Reef owns transport security. Gyre status may report bounded security
conditions, but cannot validate certificates, tokens, or rotation itself.

Coral remains on Reef v0.1.0 in this increment. Reef v0.3.0 adds unified edge
policy and observable last-known-good credential rotation. Adopting it is not a
dependency bump only: its fail-closed plaintext policy, health-path exemptions,
credential lifecycle, and principal/audit hooks require a Coral config
compatibility review and negative transport tests.

Reef v0.1 bearer validation does not expose the matched credential as a
principal. Consequently Coral is authenticated but not multi-tenant. Tenant
identity must be server-derived from a reviewed Reef/Coral identity adapter;
payload attributes and Wisp envelope IDs cannot select a tenant.

## Wisp boundary

Wisp remains the collection agent. Coral remains compatible with any standard
OTLP client and never requires Wisp-specific headers.

When implemented, Wisp delivery identity will have these boundaries:

- exactly 32 hexadecimal characters when present;
- optional and never authentication;
- scoped by server-derived organisation, project, actual signal, and envelope
  ID;
- same identity plus the same payload digest is an idempotent hit;
- same identity plus another digest is a permanent, observable conflict;
- dedup state is bounded by both TTL and capacity;
- duplicates remain possible after TTL/capacity eviction, state loss, or a
  response-loss boundary.

This cannot be implemented safely before tenant identity because a global
dedup key would let one caller interfere with another.

## Compatibility gates

Every cross-repository increment must:

1. depend only on a tagged version;
2. record the exact contract and compatibility/migration story;
3. preserve standard OTLP behavior;
4. test gRPC and HTTP parity where applicable;
5. avoid changing Gyre, Reef, or Wisp public contracts implicitly;
6. verify CI on the feature commit and the final `main` commit.
