# Go Relay 0.3.13 — public aggregate connection health

The Render dashboard can fail to load while the relay itself remains available. `/health` now includes:

- `uptimeSeconds`: process uptime (resets after a restart/redeploy)
- `disconnects`: process-local aggregate counts grouped into `read_eof`, `write_error`, `pong_timeout`, `queue_full`, `read_error`, and `other`
- `lastDisconnectUnix`: Unix timestamp of the last recorded disconnect

Counters reset whenever the relay process restarts, are not persisted, and cannot prove which network hop caused an EOF. No nicknames, IPs, room names, or game payloads are exposed. Existing `status`, `version`, `serverVersion`, and `players` fields remain.

Gameplay, SYB2, 44Hz, timeout thresholds, and graceful offline fallback are unchanged. The Java client must be updated to 0.14i because its hardcoded Relay version check now expects 0.3.13. Existing 0.14h will reject the new welcome version.
