# Snowyard Relay Server 0.3.2c — Live replay catch-up cutover

## Fix

When a late-joining Brawl client approaches the end of the command journal, the relay pauses new gameplay command ingestion **before** it sends the final `sync_batch` (a batch of fewer than 64 events). The latest journal cursor then stays stable until `sync_finish`, preventing the `Relay: replay not caught up` race under a continuous 60 TPS command stream. On successful synchronization, the existing deterministic roster barrier owns the pause; normal play resumes after the agreed roster commit. An interrupted join releases its temporary pause.

No physics or world simulation is run on the Go relay. No changes to Google Sheets storage.

**Requires client 0.12g** for a late joiner, because the client must preserve its `REPLAYING` sync stage upon `session_paused`.

## Verification

- `go test ./...` (including live replay cutover regression)
- `go test -race ./...`
- `go vet ./...`
- `go build`
- Live Render and Windows multiplayer not run.
