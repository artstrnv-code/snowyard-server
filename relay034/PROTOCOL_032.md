# Snowyard Relay 0.3.2 — deterministic roster protocol v1

This document extends `PROTOCOL_03.md`. The protocol is **opt-in**; it is not a full N-player simulation engine. Java must implement the barrier, roster states, and snapshot reconciliation. Relay still forwards commands, stores opaque snapshots and coordinates participants; it **never computes world physics**.

## Opt-in, isolation and compatibility

- New client: `{"type":"hello","version":1,"nickname":"A","mode":"brawl","room":"global","rosterVersion":1}`. For Battle Royale, use `mode:"battle_royale",room:"auto"`.
- Existing clients omit `rosterVersion` and run **unchanged**. A room cannot mix legacy and roster-v1 clients; mixed joins are rejected.
- `welcome` replies with `rosterVersion:1` for opted clients and identifies the allocated room. The `welcome.players` list describes connected sockets, **not active simulated actors**.
- Server sends `player_waiting` upon a new connection, **not** a simulation-spawn event. A socket join alone never adds a world actor.
- Legacy `/ws` and existing server-side Google Sheets storage are unchanged.

## Deterministic roster barrier (join/leave)

An eligible joiner in persistent Brawl must first complete the existing `snapshot_manifest` / chunks / `sync_poll` / `sync_finish` workflow. On successful catch-up, it receives `sync_loaded` (not `sync_ready`). The relay then starts a roster barrier. The first player of an empty world is also subject to a barrier (tick 0 may be used).

1. Server sends all peers `roster_prepare` with `generation`, intended `players` (`[{id,nickname},...]`) and `paused:true`.
2. Clients **freeze simulation**, stop submitting new commands, and reconcile to one **identical confirmed world tick and hash**. They must not vote based on their local wall clock or a guessed current tick. They may need a confirmed snapshot/replay after unexpected disconnects or unequal progress. If an actor disappears suddenly, the server **does not invent neutral input**.
3. Every intended participant sends `{"type":"roster_ready","generation":2,"tick":1234,"hash":"<64 hex chars>"}`. `tick` refers to **last completed simulation tick**. A client must not send this until it has restored/validated state and is frozen at that exact tick.
4. Server checks all `roster_ready` votes. If ticks or hashes differ, sends `roster_disagreement` and remains paused. Clients must repair/roll back/replay until they genuinely agree, then send new votes for the *same* generation. The server does not choose a majority silently.
5. When every participant agrees, server emits `roster_commit` containing `generation`, `tick:1235` (**effective first tick with the new roster**), `hash` (hash of the preceding confirmed state), `players`, `coordinator`, `backup`, `paused`. Clients create/remove actors **at that exact tick**, never on receipt of a WebSocket event.

All roster generations increase monotonically **within the current room process**. A disconnect while a barrier is pending restarts it with a new generation. Old `roster_ready` is rejected. An abrupt coordinator departure may leave `paused:true` after the roster commit; the newly elected coordinator must use the existing recovery / `coordinator_resume` protocol. The server never automatically resumes a coordinator-failure pause.

**No automatic barrier timeout in 0.3.2**. A stalled peer can hold a room paused until it disconnects or is removed. This is intentional rather than risking a nondeterministic fallback; add negotiated timeout/repair policy in a later release.

## Commands bound to roster generations

Each opted-in `command` and `command_batch` includes `generation` matching the latest `roster_commit`. All command ticks must be **at least the commit's effective tick**. Otherwise the server rejects the message (without advancing its monotonic tick counters). Forwarded commands and hashes include `generation`. `hash` / `hash_recheck` and `bot_command` also carry the active generation. While a barrier is open no gameplay commands are accepted.

Persistent Brawl `roster_commit` messages are stored as **ordered replay journal entries** alongside `command`s. Late joiners must replay them in their original `event` order; snapshots need to contain the relevant current dynamic actors. A checkpoint at/after the membership event may subsume it.

## Battle Royale

A match still starts at 10 players or at 120 seconds with remaining bot slots. In roster-v1 rooms, `match_started` is followed by a roster barrier. Only after `roster_commit` may players submit commands. The coordinator generates bot inputs; the server only relays them. Brawl has **no bots**.

## Important limitations / client obligations

- Server **does not prove** the honesty of matching client-provided hashes. Matching votes provide deterministic scheduling, not anti-cheat. Snapshot and hash recovery are client responsibilities.
- If two players are frozen at different confirmed ticks, they must first reconcile via snapshot/replay. The relay will not guess which tick is safe or force an unconfirmed state forward.
- `roster_prepare` deliberately pauses command submission. A mid-tick disconnect may require state restoration; no synthetic missing commands are created on the server.
- This release does not implement the Java N-player simulation, independent client interest management, bot AI or reliable eviction of nonresponsive but still connected peers.
- The server still has a 100-socket *configuration limit*, not a benchmarked guarantee of 100 participants.
