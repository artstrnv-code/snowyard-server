# Snowyard Relay 0.2 — Go multiplayer room core

A small, dependency-free WebSocket relay for Snowyard. Server coordinates connections and broadcasts ordered commands; **all world simulation remains in desktop clients**.

## Run

```sh
go test ./...
go run .
```

`PORT` defaults to `10000`. `GET /health` returns the previous wire format (`status`, `version`, `players`). WebSocket endpoint: `/ws`.

## Protocol (JSON-over-WebSocket, version 1)

### Existing clients (unchanged)

```json
{"type":"hello","version":1,"nickname":"Player"}
```

Joins `legacy/global`, exactly like relay 0.1. `welcome`, `player_joined`, `player_left`, `command` and `hash` keep the existing format. Additional `mode` and `room` fields in `welcome` can be ignored.

### Opt-in room namespace for future clients

```json
{"type":"hello","version":1,"nickname":"Player","mode":"brawl","room":"global"}
```

A different `mode` and/or `room` isolates membership and gameplay messages. Example `mode` labels `brawl` or `battle_royale` are **transport namespaces**, not implemented game modes. New rooms have no world snapshot or matchmaking logic. The original desktop 0.11.1 adapter accepts only two peers, so use its unmodified hello for compatibility until a new client protocol is implemented.

### Send a tick command

```json
{"type":"command","tick":1,"sequence":1,"payload":{"move":"left"}}
```

The relay validates tick/sequence ordering per player and sends it to all other peers **in that room**. It never processes physics. Hash checkpoints (`tick` divisible by 8, 64 hex chars) are also forwarded only within the room.

## Resource/security properties

- Independent room locks keep different rooms from blocking each other during command processing.
- Per-peer bounded outbound queue (`128`); slow receivers are disconnected rather than retaining unbounded memory.
- WebSocket RFC 6455 frame size limits and client masking requirements remain enforced.
- `hello` deadline 15 seconds; normal read deadline 75 seconds; heartbeat ping every 25 seconds.
- Total connection cap 100; max room registry entries 256. **Neither number is a production performance guarantee** on Render Free.
- No database, account authentication, persistent worlds or snapshot resynchronization yet.

See [RELEASE_NOTES_02.md](RELEASE_NOTES_02.md) for changes, compatibility and test details.
