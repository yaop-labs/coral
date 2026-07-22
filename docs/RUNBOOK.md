# Coral production runbook

The release profile is `configs/examples/production.yaml`. It uses Reef for
TLS/mTLS and bearer authentication; credentials are referenced by file and are
never embedded in OTLP payloads or the YAML file.

## Start and readiness

1. Verify certificate/key and token-file ownership, permissions, and expiry.
2. Verify the journal filesystem has free space and matches `journal_max_bytes`.
3. Start Coral with `coral -config /etc/coral/production.yaml`.
4. Probe the protected metrics endpoint with the operations bearer token. A
   successful response confirms configuration, TLS, and auth are active.

## Pressure and quarantine

Monitor queue item/byte depth, exporter-lane depth, journal bytes, and
quarantine counters. If journal pressure reaches the configured bound, stop
admission or scale the downstream before deleting state. Quarantined records
are evidence of required-Amber rejection and must be inspected before replay.

## Downstream outage and recovery

Keep Coral running during a bounded Amber outage: the journal and retry policy
provide the durable handoff. Alert on retry growth and journal age. Restore
Amber, confirm its health endpoint, then verify queue depth and journal bytes
decrease. Do not remove the journal to force recovery.

## Credential rotation

Write the new token to a new file, restrict it to the Coral service account,
atomically replace the configured token-file path, then perform a graceful
reload/restart. Confirm the next authenticated request succeeds before
revoking the old token at the downstream. Rotate certificates with the same
overlap strategy and validate the full chain before replacement.

## Graceful shutdown

Send SIGTERM and wait for the process to report drained pipelines. If the
deadline expires, preserve the journal directory and record the unresolved
queue/journal metrics for the next startup.

## Backup, restore, and rollback

Stop Coral before copying the journal directory. Copy it atomically together
with its metadata, preserve ownership, and restore to the exact configured
`journal_path`. Roll back by restoring the previous binary and configuration;
never reuse a journal with a different tenant mapping or incompatible schema
without an explicit migration check.
