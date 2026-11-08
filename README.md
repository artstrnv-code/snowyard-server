# Snowyard Relay Server v0.1

Small Go WebSocket relay/coordinator for the **Snowyard** game. No physics, combat, AI, snow simulation, or persistent world saves run on Render.

**Status:** first infrastructure milestone, **not yet compatible with the existing Java LAN client**. The Java client currently speaks a binary TCP lockstep protocol; this service uses WebSocket + JSON v1. Client integration, late-join snapshot recovery, distributed consensus, cloud saves, login and matchmaking remain separate milestones.

## Run locally

Requires Go 1.23+. No external modules or libraries.

```sh
go test ./...
go run .
```

The service listens at `0.0.0.0:${PORT}`, defaulting to `10000`.

- `GET /health` — status, protocol version and online peer count.
- `GET /ws` — WebSocket endpoint for the shared Brawl relay.

Render settings (native Go environment):

- Root Directory: blank
- Build Command: `go build -tags netgo -ldflags '-s -w' -o app .`
- Start Command: `./app`
- `PORT`: supplied by Render; no need to configure manually.

## Protocol v1

All messages are UTF-8 JSON WebSocket text frames. A client MUST mask its frames according to RFC 6455 and reply to server ping frames with pong frames. On connection, send:

```json
{"type":"hello","version":1,"nickname":"SnowKid"}
```

The server returns a `welcome` event with a temporary `playerId`, the version and roster. The player ID is **not** a login/account identity.

Commands: the server relays an opaque valid JSON object to all *other* connected peers, without interpreting simulation rules:

```json
{"type":"command","tick":32,"sequence":1,"payload":{"move":"left","throw":false}}
```

Received by other peers as:

```json
{"type":"command","playerId":1,"tick":32,"sequence":1,"payload":{"move":"left","throw":false}}
```

Each sender's `(tick, sequence)` must be strictly increasing (ticks may repeat with increasing sequence). For a state-hash checkpoint (every eight ticks):

```json
{"type":"hash","tick":32,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
```

The server relays hash reports with sender IDs. In **v0.1 it does not establish or enforce consensus**. Hashes must be 64 hexadecimal characters. Integration with the existing Java hash format remains to be designed.

Notifications: `player_joined`, `player_left`, `error`.

### Limits and expectations

- One shared in-memory Brawl relay, maximum 100 concurrent connections (capacity target, **not** a performance guarantee).
- Each received WebSocket text message max 32 KiB; command payload max 8 KiB; up to 128 queued outgoing messages per peer.
- Slow peers are disconnected to protect the relay.
- WebSocket heartbeat ping every 25 seconds; read timeout 75 seconds.
- One monotonically increasing temporary player ID per process.
- All connections are terminated on Render restart; no recovery yet.
- No Battle Royale matchmaking, PvE server, cloud saves, registration, or player inventory is implemented.
- No authentication yet: **do not treat this endpoint as a trusted production service**. Rate limits, identity and abuse controls will be required before an open public launch.
- At this stage, a client joining a live Brawl does **not** receive an authoritative snapshot. A full game must not start simulation for late-joining clients until snapshot/catch-up is implemented.

## Publish

Put all project files (`main.go`, `ws.go`, `main_test.go`, `go.mod`, `README.md`, `LICENSE` if applicable) in the **root** of the `snowyard-server` GitHub repository:

```sh
git add .
git commit -m "Add Snowyard Go relay server v0.1"
git push origin main
```

Render should then redeploy automatically and report success, provided the Web Service uses Go and the build/start commands above.

## License

If the GitHub repository already contains the GPLv3 LICENSE file, keep it. Code is intended to be distributed under the repository's GNU GPLv3 terms.
