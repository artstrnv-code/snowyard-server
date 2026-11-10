# Snowyard Go Relay 0.3.8 — Deterministic Departure (candidate)

Base: Relay 0.3.7. Works with Snowyard client 0.14c.

## Changes

- For healthy roster-v3 sessions, publish an ordered `roster_commit` with a new generation at tick `departed.lastCommandTick + 1`.
- Re-elect coordinator before committing new roster, while keeping the session unpaused for the safe departure case.
- Preserve ordinary lockstep commands from the previous generation before the effective tick.
- Ignore old-generation messages that were in flight past the departure boundary, including hashes; trim crossing batches.
- Reset speculative tick monotonicity for surviving players at the cutover.
- If already paused, a join transition is pending, or the departing player is 48+ ticks ahead of a survivor, fall back to checkpoint recovery rather than guessing a safe roster.
- Server identity updated to 0.3.8.
- Added departure tests for tick zero, normal 2 → 1, stale commands and unsafe fallback.

## Verification

- `go test ./...` PASS
- `go test -race ./...` PASS
- `go vet ./...` PASS
- Java core compilation and 60 executable core tests PASS (including new departure test)
- Desktop Java/libGDX and real WebSocket gameplay NOT exercised in this environment.

## Known limitations

Full checkpoint recovery for a damaged multi-player roster is not completed. This build addresses **normal healthy departures**, not every failure mode. Do not deploy as a verified stable build without testing on real clients.
