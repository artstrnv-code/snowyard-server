# Snowyard Relay Server 0.3 (Go 1.23)

WebSocket relay, session coordinator and **opaque** world-snapshot transport for Snowyard. No external Go dependencies. The server does **not** simulate the map, bots, collisions, snow, or combat.

## What is implemented

- **Legacy mode** (hello with no `mode` / `room`): original 0.11.1 desktop two-player protocol remains at `/ws`. 8-tick hashes and ordinary command messages are unchanged.
- **Permanent Brawl namespace**: `mode:"brawl",room:"global"`, with an elected coordinator, backup, binary snapshot upload/download, a bounded command replay journal, and a loading state for late joiners. No bots in Brawl.
- **Other explicit rooms**: isolated relay rooms (not persisted).
- **Battle Royale**: `mode:"battle_royale",room:"auto"` selects an open room of up to 10 human participants. The room starts when full or after 120 seconds from its first join; the coordinator is assigned the remaining bot slots. The server does not control bot AI. A started match cannot accept more participants. The room closes when empty.
- **Hash checkpoints**: new-mode rooms use a configurable interval (default 120 ticks). Disagreement requests a recheck. Persistent disagreement pauses the session until the coordinator resumes after client-side recovery. The relay does not judge simulation truth.
- **Coordinator change**: elects the lowest-ID remaining participant (preferring the previous backup); a coordinator departure pauses the session for recovery. The Java client must handle the pause/resume protocol.
- **Saving**: the coordinator periodically receives `snapshot_requested` (default 300 s). A graceful last-player `leave_request` also asks for a final snapshot. The relay validates snapshot size and SHA-256 before accepting it. After saving, it prompts the backup peer to cache a copy.
- **Temporary storage**: `SnapshotStore` interface and atomic local-file implementation. This is **not permanent on Render Free**. A Google Sheets / Apps Script implementation will be supplied in a later release.
- **Transport**: separate room locks, bounded outbound queues, JSON command batches (up to 32 individual commands per batch), snapshot blocks of at most 8192 decoded bytes. All commands are still delivered in room order.

## Not yet implemented

- Google Sheets / Apps Script connection, retry and remote recovery.
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

Important: the uploaded world snapshot has a 2 MiB limit and is **opaque** to the server. Google Sheets may impose different limits, so the future Apps Script adapter will partition Base64-encoded compressed data into multiple sheet cells. Snapshot format is not defined by this Go server.

## Protocol

See [PROTOCOL_03.md](PROTOCOL_03.md) for messages, recovery rules and the integration contract for the Java client and future Apps Script adapter.

## Licence

Follow the licence of the parent GitHub repository (GPLv3).
