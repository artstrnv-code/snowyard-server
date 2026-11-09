# Snowyard Relay 0.3.4a — deployment identification and roster abort recovery

Base: Relay 0.3.4. No change to physics or packet format for ordinary commands.

## Changes

- `/health` now returns the correct `serverVersion: "0.3.4a"`. 0.3.4 incorrectly returned `0.3.3`, making it impossible to verify deployment by health alone.
- The `welcome` JSON now contains a dedicated `serverVersion` property (`0.3.4a`), so clients can refuse an incompatible running backend before pausing the room.
- When a roster-v2 transaction aborts, established players receive **both** `roster_aborted` and explicit `session_resumed` if the room had been running. Previously the internal `paused` flag was reset without notifying Java clients, freezing their UI indefinitely.
- Failed and queued joiners remain outside the active roster. They receive `roster_aborted` with a reconnect instruction, rather than immediately re-downloading a checkpoint and triggering repeated global freezes.
- Added tests for abort/resume and version-bearing welcome.

## Known limits

- Admission still temporarily freezes the established lockstep cohort. This is a synchronous membership transaction, not seamless join-in-progress.
- Actual online Render + Windows testing is required to verify the full snapshot/replay round-trip.
- If live `/health` does not return `serverVersion: "0.3.4a"`, the deployment is stale. Do not debug client roster while on a stale server.
- No automatic repair for genuine divergent hashes among established clients.

## Validation

- `go test ./...`: PASS
- `go test -race ./...`: PASS
- `go vet ./...`: PASS
- `go build ./...`: PASS
