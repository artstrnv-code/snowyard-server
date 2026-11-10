# Snowyard Relay 0.3.5 — live join protocol (rosterVersion=3)

## Scope and compatibility

`rosterVersion:3` is available only for `mode:brawl`, `room:global`. The legacy modes and roster v1/v2 paths remain in the server. The new client **requires Relay 0.3.5**. Do not mix Java 0.13a with an older Relay; a server room rejects peers with conflicting roster protocols.

The Relay **never computes game physics, fabricates missing input, or accepts a joining peer's world as authoritative**. It relays commands, maintains a bounded journal and schedules membership transitions.

## Normal join — without freezing established clients

1. P2 sends `hello` with `rosterVersion:3`. The server registers P2 as `syncing=true`; P2 cannot send gameplay commands, vote in active hash checks or become coordinator.
2. Every active `command` or `command_batch` is appended to the authoritative, event-numbered room journal. **Recording starts immediately**, including while P2 downloads the snapshot. Download uses `snapshot_manifest` and `snapshot_get` as before. During binary download the journal, rather than a second duplicate WebSocket stream, retains incoming events for P2.
3. P2 replays `sync_batch` entries in event order. `more:true` indicates that the snapshot replay is still incomplete. `sync_poll` uses an indexed lookup into the in-memory journal.
4. When P2 sends `sync_finish`, the server atomically (under `room.mu`) sends the final `sync_live` journal tail and switches P2 to a live command subscriber. New commands can no longer fall between replay and subscription. If the tail is too large, `sync_retry` instructs the client to continue polling without resetting the snapshot.
5. P2 rapidly executes deterministic game ticks from the downloaded world. As it approaches the current live command head, it sends `join_progress` with its simulated tick; P1 and other established players **do not pause**.
6. Relay selects a future effective tick (at least 180 ticks past the latest submitted active command) and sends `roster_plan` (generation, tick, ordered players) to P1…Pn and P2. Peers acknowledge with `join_plan_ready` after validating the future boundary. A third downloader waits for the next plan.
7. On unanimous acknowledgements the server journals and broadcasts `roster_commit` for that future tick; each client continues its existing roster until precisely `effectiveTick-1`, applies the roster before the next simulation tick and then sends input for the new generation. Inputs of old generations are accepted strictly before the boundary; inputs of the new generation only from the boundary onward.
8. A missing acknowledgement causes `roster_plan_cancelled`; if active commands reach 36 ticks before the boundary without a commit, the server cancels early rather than letting established players hit the boundary and freeze for the full timeout. Late acknowledgements are ignored.

### Important limitation

The expected outcome is *no full-world pause for ordinary successful joins*, not a hard real-time latency guarantee. A congested client or long network stall may still cause a short input barrier near a scheduled transition. The initial transfer is journal-caught-up after snapshot verification; the server does not redundantly transmit per-input frames while the binary snapshot is being downloaded.

## Message shape (illustrative; zero values may be omitted by Go)

```json
{"type":"hello","version":1,"mode":"brawl","room":"global","rosterVersion":3,"nickname":"Player"}
{"type":"sync_live","snapshotId":1,"cursor":56,"tick":999,"entries":[]}
{"type":"join_progress","tick":987}
{"type":"roster_plan","generation":3,"tick":1200,"players":[{"id":1,"nickname":"P1"},{"id":2,"nickname":"P2"}]}
{"type":"join_plan_ready","generation":3,"tick":1200}
{"type":"roster_commit","generation":3,"tick":1200,"players":[{"id":1,"nickname":"P1"},{"id":2,"nickname":"P2"}]}
{"type":"roster_plan_cancelled","generation":3,"message":"..."}
```

## Persistence and recovery

New checkpoints retain commands *submitted beyond the captured simulation tick*, rebasing their journal event IDs after the checkpoint cursor. If old history has expired or is missing, the server throttles requests for an up-to-date snapshot rather than letting a downloader enter with an incomplete command chain. Snapshot creation itself still runs synchronously on the Java game thread; it is **not** optimized away in this release.

An unexpected disconnect of a committed player is not yet recovered automatically: Relay pauses the room because its lockstep cannot safely synthesize the departed player's inputs. Automatic replay-based departure recovery, long-term persistent event storage, RTT metrics and 32–100-player load testing are still outstanding.
