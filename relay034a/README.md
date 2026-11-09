# Snowyard Relay Server 0.3.3

Новый протокол состава v2 реализован как opt-in. Спецификация: `PROTOCOL_033.md`; изменения: `RELEASE_NOTES_033.md`. Старый Java-клиент 0.12g остаётся на `rosterVersion=1` до его обновления.

# Snowyard Relay 0.3.2c

Replay catch-up fix: see RELEASE_NOTES_032c.md. Pair with Snowyard client 0.12g.

**Version 0.3.2:** deterministic roster protocol (opt-in). See [PROTOCOL_032.md](PROTOCOL_032.md) and [RELEASE_NOTES_032.md](RELEASE_NOTES_032.md). Existing client protocol remains compatible.

# Snowyard Relay Server 0.3.1 (Go 1.23)

WebSocket relay, session coordinator and **opaque** world-snapshot transport for Snowyard. No external Go dependencies. The server does **not** simulate the map, bots, collisions, snow, or combat.

## What is implemented

- **Legacy mode** (hello with no `mode` / `room`): original 0.11.1 desktop two-player protocol remains at `/ws`. 8-tick hashes and ordinary command messages are unchanged.
- **Permanent Brawl namespace**: `mode:"brawl",room:"global"`, with an elected coordinator, backup, binary snapshot upload/download, a bounded command replay journal, and a loading state for late joiners. No bots in Brawl.
- **Other explicit rooms**: isolated relay rooms (not persisted).
- **Battle Royale**: `mode:"battle_royale",room:"auto"` selects an open room of up to 10 human participants. The room starts when full or after 120 seconds from its first join; the coordinator is assigned the remaining bot slots. The server does not control bot AI. A started match cannot accept more participants. The room closes when empty.
- **Hash checkpoints**: new-mode rooms use a configurable interval (default 120 ticks). Disagreement requests a recheck. Persistent disagreement pauses the session until the coordinator resumes after client-side recovery. The relay does not judge simulation truth.
- **Coordinator change**: elects the lowest-ID remaining participant (preferring the previous backup); a coordinator departure pauses the session for recovery. The Java client must handle the pause/resume protocol.
- **Saving**: the coordinator periodically receives `snapshot_requested` (default 300 s). A graceful last-player `leave_request` also asks for a final snapshot. The relay validates snapshot size and SHA-256 before accepting it. After saving, it prompts the backup peer to cache a copy.
- **Hybrid storage**: atomic local-file cache plus asynchronous Google Sheets / Apps Script API via `SnapshotStore`. Google Sheets is the durable destination; local Render files are temporary. Without Sheets environment settings, temporary-file mode remains available.
- **Transport**: separate room locks, bounded outbound queues, JSON command batches (up to 32 individual commands per batch), snapshot blocks of at most 8192 decoded bytes. All commands are still delivered in room order.

## Not yet implemented

- Java client implementation of N-player gameplay, snapshot binary encoding and world restore.
- Battle Royale game rules and actual bot AI (bots are coordinator-owned slots only).
- Server-independent durable backups and automatic state reconciliation without client participation.
- Benchmarked 32/64/100-player load on Render Free. Capacity is an **engineering goal, not a confirmed result**.

You can deploy this Go version before updating the Java client **only to retain the old legacy two-player mode**. New-mode rooms require a new client. Before a production rollout, test the new protocol and back up existing server sources.

## Run

```
go test ./...
go test -race ./...
go vet ./...
go build -o app .
./app
```

Render settings remain: build `go build -tags netgo -ldflags '-s -w' -o app .`, start `./app`, port from `PORT` (default 10000).

HTTP `GET /health` shows relay availability and player count. WebSocket endpoint `GET /ws` uses RFC 6455 and application-protocol `version:1` for legacy compatibility.

Environment:

| Variable | Default | Description |
|---|---|---|
| `PORT` | `10000` | HTTP + WebSocket port |
| `SNOWYARD_SNAPSHOT_DIR` | OS temporary directory + `/snowyard-relay` | Ephemeral snapshot file directory |
| `SNOWYARD_SNAPSHOT_INTERVAL_SEC` | `300` | Periodic request, accepted range 30..3600 |
| `SNOWYARD_HASH_INTERVAL_TICKS` | `120` | New-room checkpoint interval, accepted range 60..3600 |
| `SNOWYARD_SHEETS_URL` | unset | Apps Script Web App HTTPS URL ending in `/exec` |
| `SNOWYARD_SHEETS_TOKEN` | unset | Same secret as Script Property `SY_SERVER_TOKEN`; configure as Render secret |

Important: the uploaded world snapshot has a 2 MiB limit and is **opaque** to the server. The Sheets API splits it into 8192-byte Base64 chunks, uploading or reading up to 16 chunks per request. The world binary format belongs to the future Java client.

### Google Sheets deployment (0.3.1)

1. Install the separate `Snowyard Google Sheets Macro 0.1`, run `setupWorldStorage` and deploy the Apps Script Web App. Copy the `/exec` URL.
2. In Render > your service > Environment, set `SNOWYARD_SHEETS_URL` to that URL, and `SNOWYARD_SHEETS_TOKEN` to the **same** secret set in Apps Script Script Properties as `SY_SERVER_TOKEN`. Never put the secret in Git or in Java.
3. Deploy Go Relay 0.3.1. The service continues to expose `/health` and `/ws` as before. The existing Java 0.11.1 only uses the legacy room and cannot create Brawl world snapshots yet.
4. After the N-player client is implemented, its coordinator's `snapshot_commit` triggers immediate atomic local caching and a background upload to Sheets. Google can be slow without blocking the room lock. Log entry `[storage] cloud checkpoint committed` confirms a successful cloud save.
5. When Brawl is next created, the server loads the newest valid local/cloud checkpoint, checking digest and tick. If Google is offline but a valid local file remains, play can continue; if **neither a verified local file nor an available cloud response** exists, the server refuses to initialize Brawl rather than silently overwrite an unknown persistent world.

**Limits / risks**: The upload is queued in memory until completed, so process termination before `[storage] cloud checkpoint committed` can lose that checkpoint; Render Free local files are ephemeral. If an external write advanced the cloud world after this process loaded it, stale uploads can be rejected. The Cloud read on first Brawl join can take time, but other room joins are not blocked by its network call. Tests exercise an HTTPS mock of the macro; live Google deployment has not been exercised in this environment. Do not mistake a successful `snapshot_saved` relay message for a durable cloud commit.

## Protocol

See [PROTOCOL_03.md](PROTOCOL_03.md) for messages, recovery rules and the integration contract for the Java client and Apps Script adapter.

## Licence

Follow the licence of the parent GitHub repository (GPLv3).


## v0.3.4

See `PROTOCOL_034.md` and `RELEASE_NOTES_034.md`. Java Snowyard 0.12i required for the fresh frozen roster-v2 checkpoint path.

## v0.3.4a

Correct /health version and explicit WebSocket welcome version; cancelled roster transactions resume established players. See RELEASE_NOTES_034a.md.
