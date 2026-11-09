# Snowyard Relay 0.3.1 — Google Sheets World Storage

## Implemented

- `sheets_store.go`: server-only authenticated JSON API client for Apps Script Macro 0.1 (`begin`, `put`, `commit`, `latest`, `read`).
- Binary world snapshots preserved byte-for-byte. Batching of 16 × 8192-byte chunks; each downloaded snapshot checked against declared byte length and SHA-256.
- `hybridSnapshotStore`: atomic local cache written first; a single background worker uploads the newest checkpoint and retries failures. No Google HTTP calls inside game-room locks during save.
- On joining Brawl, compare a validated local snapshot to the cloud snapshot by tick; use the newer one and prevent silently initializing an empty world when Google is unavailable and no local copy exists.
- Snapshot load on Brawl room creation is moved outside the global relay mutex; it no longer blocks joins in other rooms.
- Settings `SNOWYARD_SHEETS_URL` and `SNOWYARD_SHEETS_TOKEN`, both optional together. Without them, existing temporary storage behavior is preserved.
- Existing `/ws` and legacy 0.11.1 compatibility retained. No Go-server physics or bots.

## Tests

`go test ./...`, `go test -race ./...`, `go vet ./...`, and Go build passed locally. Integration tests use an HTTPS fake of the Apps Script API for multi-batch upload/download (up to 2 MiB), checksum tampering, cloud outage, local fallback, and retry/recovery.

## Not yet tested / implemented

- Actual Google Apps Script deployment end-to-end (requires user configuration and valid token).
- Java client binary snapshot generation/restore, full N-player Brawl and BR.
- Load tests on Render Free / actual 32-100 players.
- Strong durability guarantee for unsent pending checkpoints if Render terminates before cloud upload completes.
- Automatic cloud reconciliation when the Google side receives outside-of-process concurrent writers; stale writes are rejected.

## Deployment

Copy the updated Go source files into the existing Git repository and add `sheets_store.go`, `sheets_store_test.go`, `RELEASE_NOTES_031.md`. Do not copy Go executables or secrets. Set the two environment variables in Render and deploy after backing up the previous revision. See README.
