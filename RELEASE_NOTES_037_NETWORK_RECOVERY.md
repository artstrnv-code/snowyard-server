# Go Relay 0.3.7 — bounded replay recovery guard

Base: 0.3.6 SYB2. Client requirement: Snowyard 0.14b.

## Changes
- Introduced `sync_restart` for *unadmitted* roster-v3 peers; replaces a failed live/replay catch-up with a request for a fresh coordinator checkpoint. Existing active players may not use it.
- Active roster-v3 departure marks `recoveryRequired`; a bare `coordinator_resume` cannot silently resume a world with a departed actor still in its deterministic membership.
- When the last active peer disappears, verified solo restoration clears the inherited recovery pause and sends a deterministic roster commit at checkpoint tick+1.
- Server build announcement updated to 0.3.7, with a matching client guard.
- Added regression coverage for live sync restart, prohibited unsafe resume and solo recovery.

## Verification
- `go test ./...` PASS.
- `go test -race ./...` PASS.
- `go vet ./...` PASS.

## Known limitation (critical)
- Recovery for 2+ *remaining active* players after unexpected roster-v3 departure is still not automated. This release prefers a visible paused room to an unverified resume/desynchronization. Do not label it as full session-recovery support.
- Full real-world multi-client test NOT performed.
