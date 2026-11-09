# Snowyard Relay 0.3.2 — deterministic roster barriers

## Added
- Opt-in `rosterVersion:1` on the existing WebSocket handshake, restricted to advanced game rooms. Legacy two-player / old clients remain supported without opt-in; mixed-protocol rooms are rejected.
- Two-phase change-of-membership barrier (`roster_prepare`, `roster_ready`, `roster_disagreement`, `roster_commit`) with server-assigned effective tick equal to **agreed confirmed tick + 1**.
- Atomic roster generations and generation-tagged inputs / hashes; input before the new effective tick or from an old generation is refused.
- Player disconnect triggers a new barrier; coordinator handoff still requires an explicit resume and cannot be accidentally undone by roster commit.
- Brawl snapshot catch-up now ends with `sync_loaded` for opted clients; only roster commit adds a simulation participant. First-world bootstrap also uses a roster barrier.
- Accepted membership events enter persistent Brawl's bounded replay journal so late joiners can reproduce roster changes after the stored snapshot.
- Server-side regression tests for first player, joined player, disagreement, re-vote, stale votes, epochs, disconnect, coordinator failover, BR bot slots, mixed protocol rejection, and catch-up replay.

## Fixed
- Rejected inputs during a pause no longer advance sender monotonic sequence counters.
- Syncing/joining peers are not chosen as coordinators for opted-in rooms when no active participant exists.
- Coordinator-failure pause is preserved after a roster barrier commits.

## Unchanged
- Google Sheets / Apps Script snapshot integration from 0.3.1.
- Binary snapshot transport, SHA-256 verification, room isolation and bot command delegation.
- Go relay does not simulate the world, snow, collisions or bot AI.

## Compatibility and testing
- Full `go test ./...`, `go test -race ./...`, `go vet ./...` and `go build ./...` passed locally.
- Real Render deployment and game-client integration are **not** tested. New N-player functionality requires Java client changes. New protocol is opt-in to avoid surprising existing two-player clients.
- See PROTOCOL_032.md for the client-side freeze/reconciliation requirements and limitations.
