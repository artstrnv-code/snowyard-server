# Snowyard Relay 0.3 — protocol contract

All frames in 0.3 are **JSON WebSocket text messages**. Large binary snapshots are Base64-encoded in chunks; the Go relay does not interpret or compress those bytes. In-game deterministic simulation is client-only. This document describes implemented server behavior; client/macros remain separate tasks.

## Connection and rooms

`{"type":"hello","version":1,"nickname":"Player"}` → legacy compatible room, as in 0.2.

`{"type":"hello","version":1,"nickname":"Player","mode":"brawl","room":"global"}` → persistent Brawl. Static trees/swings and static map are supplied by the Java client, not the server. There are no bots.

`{"type":"hello","version":1,"nickname":"Player","mode":"battle_royale","room":"auto"}` → matchmaking; 10 humans max per match, otherwise missing slots assigned to bots after 120 s. Use returned `welcome.room` for the allocated match ID.

For advanced rooms, `welcome` includes `hashInterval` and is followed by `coordinator_changed` (`coordinator`,`backup`,`paused`). On BR start, `match_started` includes `botSlots` and `slots:10`. `botSlots` reserves virtual bot slots; the designated coordinator must generate corresponding deterministic input. The relay does not create or run bot objects.

## Commands / hashes

`{"type":"command","tick":1,"sequence":1,"payload":{"move":"left"}}` — relay to room; sequence and tick must be monotonic for each sender.

Optional inbound batch: `{"type":"command_batch","commands":[{"tick":1,"sequence":1,"payload":{}},{"tick":2,"sequence":1,"payload":{}}]}` (maximum 32 entries, validated atomically). Receivers get one `command_batch` with `playerId` and `commands`. Old clients continue to receive ordinary `command` messages in legacy mode when they send ordinary commands.

`{"type":"hash","tick":120,"hash":"<64 hex characters>"}` — a new-mode checkpoint, period configurable by `SNOWYARD_HASH_INTERVAL_TICKS`. The first mismatch generates `hash_recheck_requested`; submit `hash_recheck` for the same tick. A repeated mismatch generates `session_paused` with the majority hash if one exists; client-side snapshot repair must occur before the coordinator sends `coordinator_resume`. In legacy mode hashes remain spaced by 8 ticks.

`{"type":"bot_command","botId":1,"tick":123,"payload":{}}` — **only** the Battle Royale coordinator, after `match_started`; 1 ≤ botId ≤ botSlots. No bots exist in Brawl.

## Persistent Brawl world snapshot

The active coordinator uploads an opaque binary snapshot (maximum 2 MiB) using:

1. `snapshot_begin`: `{ "type":"snapshot_begin", "tick":120, "size":N, "digest":"<sha256 hex>" }`
2. `snapshot_chunk`: `{ "type":"snapshot_chunk", "index":0, "data":"<Base64 bytes>" }` — sequential indices, at most **8192 decoded bytes** per chunk.
3. `snapshot_commit`: `{ "type":"snapshot_commit" }`

Server verifies the total length and SHA-256. On success it emits `snapshot_saved` with `snapshotId`, `tick`, `digest` and `message` equal to `temporary_disk: Google Sheets adapter pending` or `memory_only: temporary disk save failed`.

`SNOWYARD_SNAPSHOT_INTERVAL_SEC` requests snapshots periodically via `snapshot_requested` to the coordinator. A graceful last-player `leave_request` also requests a snapshot; after successful `snapshot_commit` the server sends `leave_ready`. An abrupt disconnect cannot force a final snapshot; the periodic version and backup may be older. This risk is explicitly accepted by the project.

After a successful commit, the designated backup peer receives `backup_snapshot_available`. The backup may request `snapshot_info` and download the chunks via `snapshot_get`, then keep a local copy. No automatic peer-to-peer storage transfer is implemented.

### Join in progress

- Upon `hello` the newcomer receives `welcome` and `snapshot_manifest` (or `sync_wait` while no checkpoint exists). **Even the first participant must load a snapshot** if the server recovered one.
- `snapshot_manifest` includes `snapshotId`,`tick`,`size`,`digest`,`chunks`,`cursor`.
- Client displays a loading screen, requests every numbered chunk using `{"type":"snapshot_get","snapshotId":1,"index":0}`, assembles and validates the digest, then restores the world in Java.
- Client sends `{"type":"sync_poll","snapshotId":1,"cursor":<manifest cursor>}`. Server replies up to 8 `sync_batch` responses per poll, each with ordered `entries` (up to 64), each containing an `event` and the forwarded `command`, and the updated `cursor`. Repeat until caught up.
- After replay and world validation, client sends `{"type":"sync_finish","snapshotId":1,"cursor":<latest cursor>}`. Server answers `sync_ready`; only then may this client submit commands. If there is a newer snapshot or the bounded journal expires, the client must restart synchronization. The server rejects early completion.
- The server does NOT apply the replay to a world itself. No other new-mode client is implemented yet.

Snapshot commits retain all journal commands with tick **greater** than the committed snapshot tick. A 32768-entry / 8 MiB bounded rolling journal limits memory. On journal expiration the Java client must request a fresh snapshot.

### Backups and reliability

`SnapshotStore.Load/Save` is an interface ready for the future Sheets/Apps Script adapter. The included temporary disk implementation writes atomically and validates SHA-256 on load, but Render Free disks are ephemeral: sleeping, restarting or deploying can erase it. The upcoming adapter must implement completed/committed version records, chunked base64 payloads, retries and checksum verification. **Google Sheets is not connected in 0.3.**

For the permanent Brawl room, a coordinator failure triggers `coordinator_changed` and `resume_required`. Until coordinator-side resynchronization finishes and `coordinator_resume` is sent, the room rejects further commands. An undecidable 50/50 hash split remains paused; the server does not invent a correct state.

## Limits and compatibility

- 100 simultaneous sockets maximum by server policy, not benchmark-confirmed capacity.
- 10 humans per Battle Royale room; no late join into started matches.
- 8192 bytes for an individual ordinary command JSON payload; 32 KiB incoming WebSocket frame payload max.
- 2 MiB snapshot, 8192 bytes per snapshot chunk, 32768 replay events (or 8 MiB, whichever comes first).
- Multi-peer Brawl still uses fanout to all active peers. For 32+ active peers this remains a scaling risk; future client/server batching and interest management may be required.
- No account authentication, anti-cheat, peer authorization beyond coordinator role, or external cloud persistence in 0.3. Do **not** treat this as a secure production world-state service yet.
