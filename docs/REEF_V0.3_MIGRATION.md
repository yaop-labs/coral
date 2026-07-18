# Reef v0.3 migration

Coral adopts tagged Reef v0.3.0 as its production security-edge API. This is a
deliberate fail-closed migration, not only a module upgrade.

## Configuration changes

Every OTLP receiver, self-observability listener, and HTTP exporter accepts:

```yaml
insecure: false
danger_allow_bearer_over_plaintext: false
credential_reload_interval: 5s
```

The zero reload interval selects Reef's five-second default. Explicit values
must be between one second and 24 hours.

Only literal loopback IPs (`127.0.0.1` and `::1`) may use plaintext without an
opt-in. `localhost`, wildcard binds, DNS names, container service names, and
external IPs are not treated as loopback.

An external plaintext listener or exporter now requires:

```yaml
insecure: true
```

This is intended for explicitly accepted development or protected-network
risk. TLS is the production migration.

Bearer credentials over plaintext additionally require:

```yaml
danger_allow_bearer_over_plaintext: true
```

Setting either opt-in while TLS is enabled is rejected, as is setting the
bearer-danger opt-in without bearer authentication.

## Credential lifecycle

File-backed server certificates, client certificates, CA pools, and bearer
tokens are loaded fail-stop at startup and then reloaded in the background.
A failed or half-written update preserves the last-known-good generation.
Successful new handshakes/requests use the new generation without restarting
Coral.

Inline and environment bearer sources remain startup-only. `token_file` is
recommended for rotation.

Coral closes every managed edge during pipeline/app shutdown and also closes
materialized exporters when later application construction fails.

## HTTP boundary behavior

Exporter transports are bound to the configured scheme and host. A transport
cannot be reused for another origin, and Coral does not follow downstream HTTP
redirects. This prevents bearer credentials from escaping the configured
Amber/Fathom edge and avoids changing an OTLP POST into a redirected GET.

When bearer auth protects the self-observability edge, `/healthz` and
`/readyz` remain public while `/metrics` and `/status` are protected.
OTLP ingress leaves only its `/healthz` route exempt.

## Principal and observability

Reef propagates the configured bearer key name as a secret-free authenticated
principal in HTTP and gRPC request contexts. Coral does not yet interpret that
name as an organisation/project; the tenant mapping remains a separate,
versioned capability.

Coral exports bounded metrics by Reef credential kind and outcome:

- `coral_credential_events_total{kind,outcome}`;
- `coral_credential_generation_max{kind}`.

Token values, certificate contents, file paths, remote addresses, and
unbounded principal names are never metric labels.

## Legacy receivers

Jaeger and Zipkin legacy listeners do not yet expose Reef TLS/auth config.
They should remain loopback-only or be protected by a reviewed perimeter.
Their security migration is separate from the standard OTLP edge and must not
be mistaken for Reef v0.3 coverage.
