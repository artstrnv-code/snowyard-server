# Snowyard Relay 0.3.2b — Empty roster barrier fix

- An all-syncing room no longer creates a zero-player roster barrier when the last active peer disconnects.
- Keeps the world paused until a synced participant completes `sync_finish`, which initiates a normal membership barrier.
- No changes to snapshot storage, physics, or legacy two-player protocol.
- Pair with Snowyard 0.12f client for tolerance to empty prepares from older servers.
