# Snowyard Relay 0.3.2a — roster generation command cursor fix

Compatibility patch required for the new N-player Snowyard 0.12b test client.

On `roster_commit`, reset each committed peer's last-command tick/sequence to the common confirmed barrier tick. Otherwise valid post-barrier commands may be rejected as non-monotonic when a previous client had submitted commands ahead of the barrier. The existing generation and effective-tick checks still reject stale commands.

No gameplay simulation or Google Sheets changes. The fundamental requirement for identical frozen tick/hash on all peers remains: a barrier can stall and need an explicit state repair if peers stop on different ticks.
