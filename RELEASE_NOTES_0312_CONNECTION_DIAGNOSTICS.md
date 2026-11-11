# Go Relay 0.3.12 — WebSocket diagnostics

- Ping every 10 seconds, with a unique 8-byte timestamp payload; a client Pong only acknowledges the *matching* outstanding probe.
- If there is no matching Pong for over 50 seconds, close that connection and record the specific timeout in the log. Other players remain connected.
- Each disconnected peer's log entry includes last frame received/sent, last Pong, outstanding Ping age, measured RTT, room/mode, and current outbound-queue depth. Log timestamps now include microseconds.
- Room transitions from multiplayer Royale to offline report `royale_handoff` with room and the departing peer ID.
- Replies to normal client Close frames echo the client's Close payload (when valid).
- Peer queues, write deadline, 44 Hz simulation, room roster rules, SYB2 format, and existing Brawl behavior remain unchanged.

## Test results

`go test ./...`, `go test -race ./...`, `go vet ./...` pass. Includes matching and stale-Pong regression tests.

The previous 1006 incident has not been reproduced or attributed to a specific network layer. These changes improve evidence and timeouts; they do not guarantee that transient network interruptions stop occurring.
