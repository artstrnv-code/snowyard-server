# Snowyard Relay Server 0.3 — Multiplayer Sessions & World Transport

## Added

- Global Brawl room with coordinator/backup election, pause-on-handoff and double-check hash mismatch detection.
- Brawl join-in-progress infrastructure: opaque validated binary snapshots, block-wise download, bounded command replay and loading state.
- Base `SnapshotStore` API and atomic, checksum-verified temporary disk store. Can later be replaced with a Google Sheets / Apps Script adapter.
- Periodic snapshot requests (default 300 s); best-effort save request on graceful departure of the last client.
- Backup-peer snapshot notifications and optional chunk downloads to enable client-held replicas.
- Battle Royale automatic grouping into 10-slot rooms, automatic start when full or after 120 s, deterministic bot slots controlled only by the coordinator.
- Up-to-32 command batching for future clients, with per-command validation and journal replay.
- Configurable hash checkpoint period for new rooms (default 120 ticks).

## Preserved

- WebSocket `/ws`, RFC6455 handling, legacy two-player client hello/version 1 and 8-tick legacy hash interval.
- No server-side physics, map or bot AI; the relay only forwards input and handles metadata/state transfer.
- Room isolation, bounded send queues, max 100 simultaneous socket policy.

## Tested

`go test ./...`, `go test -race ./...`, `go vet ./...`, `go build ./...`, including protocol, multi-room routing, 10-slot BR matching, coordinator handoff, SHA-256 and corrupt-file handling, resynchronization and replay, command batching and repeated hash mismatch.

## Not implemented yet

- Google Sheets macro and corresponding storage adapter.
- Java 0.12+ multiplayer client using these new messages.
- End-to-end game tests on Render; Render Free capacity tests with 32, 64 or 100 real players.
- True replay persistence through Render process restarts when Google Sheets is unavailable.

**Deployment**: replace prior Go source files in the existing repository, add `session.go`, `snapshot.go`, `session_test.go`, `PROTOCOL_03.md` and this file, run tests, commit and push. The existing 0.11.1 client should continue to use the legacy room; new Brawl/BR rooms require an updated Java client.
