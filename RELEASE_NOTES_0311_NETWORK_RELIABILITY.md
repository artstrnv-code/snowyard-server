# Go Relay 0.3.11 — Disconnect Diagnostics + SYB2 Royale

## Changes

- Structured disconnection logs: peer ID, room/mode, first close cause, outgoing queue occupancy, and session duration. No game message bodies or nicknames are logged.
- The relay records distinct reasons when a broadcast/control outgoing queue fills, a WebSocket data frame write fails, a ping write fails, or reading an incoming frame ends in error.
- `/health` reports the actual server version 0.3.11 (formerly stale).
- Binary SYB2 command transmission is now negotiated by Royale 0.14g; relay encoding and decoding already existed and remain unchanged. Added a Royale-specific binary broadcast regression test.
- No queue sizes, read/write timeouts, gameplay rules, or physics were changed.

## Validation

`go test ./...`, `go vet ./...`, `go test -race ./...`: pass.

## Deployment

Update client 0.14g and Relay 0.3.11 together, because clients verify the exact server version. The event logs can help identify whether a 1006 disconnect follows queue overflow, write errors, or read failures; they cannot independently prove the root cause of a network failure.
