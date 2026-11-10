# Snowyard Relay 0.3.5 — live membership, reduced network load

## New
- Added **opt-in rosterVersion=3** (persistent global Brawl only) and `roster_v3.go`.
- Separated snapshot transfer/replay from actual membership. Late joiners subscribe to the live command stream through atomic `sync_live` and `roster_plan` / `join_plan_ready` / future `roster_commit`.
- No global freeze at replay tail or during ordinary new-player joins in v3. Third and later joiners cannot overwrite a pending join transaction.
- Aborts unacknowledged plans before the active input stream reaches their future effective tick. Ignores obsolete join/progress acknowledgements.
- Restores the first player from a saved world without requiring an active voting cohort.
- Indexed `sync_poll` journal lookups; replay batches have bounded wire size and explicit `more` metadata for v3. v1/v2 retain original end-of-replay semantics.
- Refreshes old checkpoints when a bounded journal can no longer replay their histories. Rebinds post-checkpoint future input commands to fresh event cursors.
- Enlarged bounded Go outbound queue (128 -> 256).
- The WebSocket welcome and `/health` identify `serverVersion=0.3.5`.

## Validation
- `go test ./...`: PASS.
- `go test -race ./...`: PASS.
- `go vet ./...`: PASS.
- `go build ./...`: PASS.
- New Go tests include P1/P2 snapshot and live handoff, concurrent P3 download, old/new generation tick boundaries, cancelling a bad plan, solo world restoration. Existing Go tests retained.

## Not in scope / known limitations
- No complete unexpected-active-disconnect recovery; the relay pauses rather than synthesizing missing commands.
- No CPU/latency benchmarks from Render; scaling to 32 or more real players is not established.
- Go still runs local snapshot-store saves under a room lock. Java still captures snapshots on the render thread.
- Existing per-player lockstep still waits for everyone's tick input.
- Java 0.13a is required to activate v3; deploying Relay alone does not migrate old Java clients.

See `PROTOCOL_035.md`. Source files reside at the ZIP root (not in a nested folder).
