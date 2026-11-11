# Snowyard Go Relay 0.3.9 — Battle Royale matchmaking prototype

Works with Snowyard 0.14d. Brawl and its SYB2 44 Hz traffic path are unchanged.

## Implemented
- Royale match search timeout changed from **120 to 90 seconds**.
- Ten human participants terminate matchmaking immediately; the room locks to new arrivals.
- At search completion with 2–10 humans, send `match_countdown` (8 seconds) with `botSlots`, `coordinator`, `backup`; send `match_started` at countdown expiry.
- Bots occupy vacant slots; `bot_command` is rejected until the countdown has actually completed.
- Solo waiting room receives `offline_fallback` instead of a server-driven match. The client closes its WebSocket and starts local gameplay.
- During countdown, departure of all but one participant also causes `offline_fallback`.
- Departures before the start recalculate `botSlots`; no phantom humans are left in the start count.
- `match_waiting` informs clients of the number of humans and approximate remaining search time.
- Existing coordinator/backup election retained; Go does not simulate AI or game physics.

## Protocol additions
- `match_waiting`: `size` (current human count), `slots` (10), `durationSeconds` (approximate remaining search time).
- `match_countdown`: `durationSeconds` (8), `botSlots`, `coordinator`, `backup`.
- `offline_fallback`: switch to existing local Battle Royale, close WebSocket.
- `match_started`: emitted **after** countdown, not at search completion.

## Out of scope
- Actual online Battle Royale gameplay, spectator streaming, hot handoff of bot internal AI state, safe mid-match offline transfer, reconnections and network AI commands are not yet implemented in the client.
- In this prototype, server does not force-close a solo user's socket immediately; the 0.14d client closes it on receiving `offline_fallback`. An incompatible client can remain connected until disconnect/timeout.

## Verification
- `go test ./...`, `go test -race ./...`, `go vet ./...` passed.
- Tests cover solo fallback, countdown closing admission, timer completion, bot-slot reassignment and departure during countdown.
- No live deployment or real multi-JVM session was performed.
