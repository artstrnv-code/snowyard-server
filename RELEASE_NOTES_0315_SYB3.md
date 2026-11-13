# Go Relay 0.3.15 — SYB3 tick support

- Binary command batches accept the SYB3 tick marker `0xB5` while still parsing historical SYB2 (`0xB2`) and journal (`SYJ1`) payloads for transition and recovery.
- `welcome` and health endpoints identify the server as `0.3.15`; clients 0.15.21 validate this version before entering online Brawl/Battle Royale.
- Relay forwards compressed tick bytes verbatim; the deterministic throw is reconstructed by the Java world on each client.
- No server physics or matchmaking behavior has changed. JSON/Base64 checkpoint bridge remains for persistent recovery.
- Tests: `go test ./...`, plus compact SYB3 byte-preservation test.
