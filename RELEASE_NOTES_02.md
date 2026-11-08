# Snowyard Relay 0.2 — Multi-room Core (2026-11-08)

## Scope
Server-side refactor only. Go relay remains a transport coordinator; no physics, authoritative simulation, snapshots, or matchmaking logic was added.

## Changes
1. Room-aware routing (`relayRoom`): optional `mode` and `room` fields in the initial `hello`. Room membership, commands, joins/leaves and hash checkpoints are strictly isolated.
2. Backward compatibility: an old `hello` without the two new fields joins the original `legacy/global` room. The existing 0.11.1 desktop client does not need to be rebuilt for server 0.2.
3. Independent per-room locks for command ordering, plus a short global registry lock for joins. No global mutex on the command fan-out hot path.
4. One JSON serialization per broadcast. Removed server-side hash counting that was not being used for consensus (hash relay remains).
5. WebSocket headers and data written in a single write loop to reduce syscalls. Bounds and mask checks unchanged.
6. The first `hello` must arrive within 15 seconds of upgrading; subsequent message handling retains the 75-second read deadline and existing ping frames.
7. Sorted join roster and queued welcome before all other messages to a newly admitted peer.
8. Empty rooms are garbage-collected. Relay-wide maximum remains 100 simultaneous peers; room registry maximum is 256.

## Optional hello fields
```json
{"type":"hello","version":1,"nickname":"Snowman","mode":"brawl","room":"global"}
```
Both fields must be present together; values must each be 1–48 characters drawn from letters, numbers, underscores and dashes. `legacy` is reserved. An unchanged client sends no `mode`/`room`.

The `mode` value is a routing namespace, **not game mechanics**. For example, rooms under `battle_royale` are isolated, but the server does NOT create matches, enforce BR rules, choose winners, or send world snapshots.

## Critical limitations
- The existing desktop v0.11.1 online adapter is still a **two-player** test. Server fan-out supports more recipients, but the client does not yet implement N-player synchronized simulation.
- A late join receives an accurate roster, but not a current world snapshot. Do not admit mid-match players until snapshot/catch-up protocol exists.
- Persistence of the global Brawl world, durable storage, snapshot recovery, anti-cheat, matchmaking, and player accounts remain **unimplemented**.
- For `n` peers receiving each other’s commands at 60 TPS, outbound relay fan-out is O(n²). The 100-peer connection cap is not a validated capacity at 0.1 CPU. Room isolation reduces contention but does not eliminate aggregate bandwidth/CPU constraints.
- On Render Free the instance may sleep. `/health` is retained as before; separate startup probing belongs in the client.

## Checks performed
- `go test -count=1 ./...` — PASS
- `go test -race -count=1 ./...` — PASS
- `go vet ./...` — PASS
- `go build -tags netgo -ldflags '-s -w' -o app .` — PASS (local Linux toolchain)
- End-to-end integration: 3 peers in one room, isolation from a fourth peer in a different room, old/new room segregation, duplicate command rejection, disconnect propagation.
- Benchmark `BenchmarkRoomBroadcast16`: local in-memory benchmark only; does not represent real Render latency or throughput.

## Render deployment
Update the public GitHub repository's Go source (`main.go`, `ws.go`, `main_test.go`, `go.mod`) and let Render rebuild with the existing build and start commands. Do not copy the local `app` executable; Render will compile it itself. Keep a copy of server 0.1 for rollback.
