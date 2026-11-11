# Snowyard Relay 0.3.14 — Royale Handoff Reliability

- Log `royale_handoff_queued` when a survivor's fallback is enqueued.
- Log `royale_handoff_sent` after the WebSocket write completes successfully.
- Accept `handoff_ack` only when a live Royale room has exactly one participant; log `royale_handoff_applied` on acceptance.
- Confirmation is informational; never pause the solo client's offline transition waiting for an ACK.
- Relay protocol version string and `/health` serverVersion updated to `0.3.14`.
- New regression test for ACK validation and unchanged room membership.

Go test suite, race detector and vet: PASS. Live Render tests not performed.
