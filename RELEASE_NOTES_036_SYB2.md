# Snowyard Go Relay 0.3.6 — SYB2 / 44Hz (experimental source)

Based on Relay 0.3.5 CHECKPOINT RACE HOTFIX. Pair only with Snowyard 0.14a SYB2.

- Added WebSocket opcode-2 binary frame input and output for Brawl command batches; control and snapshot messages remain JSON.
- New optional hello field `binary:true` required for roster-v3 Brawl clients (older clients receive a clear upgrade error).
- Go validates binary batch envelope and monotonic tick/sequence via the **existing** `handle()` path; keeps authoritative roster and anti-abuse checks.
- Converts binary payloads into the existing event journal as `{\"b\":\"...\"}` so checkpoint replay/catch-up retains its proven delivery semantics.
- Sends binary command batches to SYB2-capable peers; legacy protocol classes keep their old transport.
- Hash check now defaults to 88 ticks (2 seconds at 44Hz); environment variable `SNOWYARD_HASH_INTERVAL_TICKS` still overrides it.
- Added binary message/bridge tests; all Go tests and Go race tests pass.

Caution: not a verified live deployment. Do not deploy alone: older 0.13a roster-v3 Brawl clients can no longer join. Back up persisted 60Hz world before the paired experimental upgrade.
