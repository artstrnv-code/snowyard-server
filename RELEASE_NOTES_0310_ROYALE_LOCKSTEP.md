# Snowyard Go Relay 0.3.10 — Online Royale lockstep routing

- `match_started` includes the authoritative ordered player roster; ten fighter slots are shared between humans and bots.
- Standard SYB2 `command_batch` messages now carry real human inputs in isolated Royale rooms after the countdown; the Relay does **not** calculate physics or AI.
- Elected coordinator sends `bot_command` target plans; backup host can take over after disconnect without stale monotonic-tick rejections.
- Royale commands are forbidden during matchmaking/countdown.
- When someone disconnects and >=2 humans remain connected, `royale_departure` announces a future removal tick and the departing participant's last accepted command tick, allowing deterministic neutral gap fill.
- With exactly one connection remaining, the Relay triggers `offline_fallback`; the surviving client can continue the current match offline.
- Room membership closes at matchmaking completion; no late join, ratings, rewards or persistent world snapshots.
- Server build advertised as 0.3.10; updated Brawl and Royale clients require this build.

Validation: `go test ./...`, `go test -race ./...`, and `go vet ./...` passed. Live multiplayer/Render load tests were not performed.
