# Go Relay 0.3.16 — direct SYB3 forwarding

Paired client: Just a Snow Game 0.15.23.

## Changes

- `decodeClientBatch` retains the raw incoming tick slice rather than eagerly creating JSON/Base64 per tick.
- `encodeServerBatch` uses raw tick bytes directly when available; historical JSON payloads are still accepted.
- `broadcastLocked` constructs binary messages only for binary recipients and JSON messages only when a text recipient exists.
- Persistent Brawl still converts input to JSON for its journal, retaining existing catch-up/recovery compatibility. Non-persistent Battle Royale has no journal conversion.
- Added regression tests for binary and mixed JSON/binary rooms and for journal encoding.
- Relay version string is 0.3.16. No changes to the WebSocket on-wire envelopes SYB3/SYB2 or roster protocol.

## Diagnostics

`GET /health` now includes `traffic` with cumulative `inBytes`, `outBytes`,
`inMessages`, `outMessages`, and aggregated `activeRooms`,
`activeRoomInBytes`, `activeRoomOutBytes`. Values count WebSocket message
application payloads, not TCP/IP/TLS headers or ping/pong frames. Totals
reset on process restart; active room totals reset as rooms close.
No player identifiers or room names are published and these counters add
no messages to game traffic. Divide differences between consecutive `/health`
reads by elapsed seconds to estimate application throughput.

## Tradeoffs and verification

- Ordinary SYB3 tick byte count on the network is unchanged: this is a CPU/allocation optimization, not additional packet compression.
- The Java 0.15.22 client checks the exact relay version; use client 0.15.23 with relay 0.3.16.
- `go test -race ./...` and `go vet ./...` passed; no live Render load test was performed.
- A microbenchmark of `encodeServerBatch` with one 8-byte tick yielded ~77 ns/op, 64 B/op, 1 alloc/op on the test machine; not an end-to-end performance figure or comparison against 0.3.15.
