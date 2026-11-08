package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func rosterPeer(t *testing.T, r *relay, mode, name string) *peer {
	t.Helper()
	p := &peer{id: r.nextID.Add(1), nickname: "member", rosterVersion: 1, send: make(chan []byte, 256)}
	if err := r.add(p, mode, name); err != nil {
		t.Fatal(err)
	}
	return p
}
func readyRoster(t *testing.T, r *relay, p *peer, gen, tick uint64, hash string) {
	t.Helper()
	if err := r.handle(p, clientMessage{Type: "roster_ready", Generation: gen, Tick: tick, Hash: hash}); err != nil {
		t.Fatal(err)
	}
}
func hasRoster(msgs []serverMessage, typ string, gen uint64) bool {
	for _, m := range msgs {
		if m.Type == typ && m.Generation == gen {
			return true
		}
	}
	return false
}
func TestRosterJoinRequiresMatchingTickAndHash(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := rosterPeer(t, r, "brawl", "staging")
	g1 := a.room.barrier.generation
	if g1 != 1 || !a.room.state.paused {
		t.Fatal("initial barrier missing")
	}
	// No commands before commit; importantly a rejected command must NOT
	// advance monotonic sequence bookkeeping.
	command := clientMessage{Type: "command", Tick: 1, Sequence: 1, Generation: g1, Payload: json.RawMessage(`{"go":1}`)}
	if err := r.handle(a, command); err == nil {
		t.Fatal("allowed pre-commit command")
	}
	if a.lastCommandTick != 0 {
		t.Fatal("rejected command advanced counter")
	}
	readyRoster(t, r, a, g1, 0, strings.Repeat("a", 64))
	if a.room.rosterApplied != g1 || a.room.state.paused {
		t.Fatal("initial roster never committed")
	}
	if !hasRoster(drain(a), "roster_commit", g1) {
		t.Fatal("missing initial commit")
	}
	if err := r.handle(a, command); err != nil {
		t.Fatal(err)
	}
	b := rosterPeer(t, r, "brawl", "staging")
	g2 := a.room.barrier.generation
	if g2 != g1+1 {
		t.Fatal("generation not advanced")
	}
	if err := r.handle(a, clientMessage{Type: "coordinator_resume"}); err == nil {
		t.Fatal("resumed before barrier")
	}
	if err := r.handle(a, clientMessage{Type: "command", Tick: 2, Sequence: 2, Generation: g1, Payload: json.RawMessage(`{"go":2}`)}); err == nil {
		t.Fatal("command accepted during barrier")
	}
	readyRoster(t, r, a, g2, 20, strings.Repeat("a", 64))
	readyRoster(t, r, b, g2, 21, strings.Repeat("a", 64))
	if a.room.barrier == nil || !a.room.state.paused {
		t.Fatal("mismatched tick committed")
	}
	if !hasRoster(drain(a), "roster_disagreement", g2) {
		t.Fatal("missing mismatch event")
	}
	// Re-vote after both clients converge on same tick and world hash.
	readyRoster(t, r, b, g2, 20, strings.Repeat("b", 64))
	if a.room.barrier == nil {
		t.Fatal("mismatched hashes committed")
	}
	readyRoster(t, r, b, g2, 20, strings.Repeat("a", 64))
	if a.room.barrier != nil || len(a.room.rosterMembers) != 2 {
		t.Fatal("roster did not commit")
	}
	saw := false
	for _, m := range drain(a) {
		if m.Type == "roster_commit" && m.Generation == g2 {
			if m.Tick != 21 || len(m.Players) != 2 {
				t.Fatalf("wrong commit: %+v", m)
			}
			saw = true
		}
	}
	if !saw {
		t.Fatal("missing second commit")
	}
	if err := r.handle(a, clientMessage{Type: "command", Tick: 2, Sequence: 2, Generation: g1, Payload: json.RawMessage(`{"go":2}`)}); err == nil {
		t.Fatal("accepted stale epoch command")
	}
	if err := r.handle(a, clientMessage{Type: "command", Tick: 21, Sequence: 2, Generation: g2, Payload: json.RawMessage(`{"go":2}`)}); err != nil {
		t.Fatal(err)
	}
	r.remove(a)
	g3 := b.room.barrier.generation
	if b.room.state.coordinator != b.id || !b.room.state.paused {
		t.Fatal("handoff didn't pause")
	}
	readyRoster(t, r, b, g3, 25, strings.Repeat("a", 64))
	if !b.room.state.paused {
		t.Fatal("roster commit silently resumed coordinator handoff")
	}
	if err := r.handle(b, clientMessage{Type: "coordinator_resume"}); err != nil {
		t.Fatal(err)
	}
	r.remove(b)
}
func TestRosterVersionMixedRoomRejectedAndLegacyUnaffected(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := peerFor(t, r, "brawl", "mixed")
	p := &peer{id: r.nextID.Add(1), nickname: "opted", rosterVersion: 1, send: make(chan []byte, 256)}
	if err := r.add(p, "brawl", "mixed"); err == nil {
		t.Fatal("mixed room accepted")
	}
	if a.room.barrier != nil {
		t.Fatal("legacy accidentally entered roster mode")
	}
	if err := r.handle(a, clientMessage{Type: "command", Tick: 1, Sequence: 1, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	r.remove(a)
}
func TestRoyaleRosterBarrierAtStart(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := rosterPeer(t, r, "battle_royale", "auto")
	b := rosterPeer(t, r, "battle_royale", "auto")
	if a.room.barrier != nil {
		t.Fatal("BR roster activated before start")
	}
	r.startRoyale(a.room)
	g := a.room.barrier.generation
	if a.room.state.botSlots != 8 {
		t.Fatal("missing bot slots")
	}
	readyRoster(t, r, a, g, 0, strings.Repeat("b", 64))
	readyRoster(t, r, b, g, 0, strings.Repeat("b", 64))
	if a.room.rosterApplied != g || a.room.state.paused {
		t.Fatal("BR did not transition into active roster")
	}
	if err := r.handle(a, clientMessage{Type: "bot_command", Generation: g, BotID: 1, Tick: 1, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	r.remove(a)
	r.remove(b)
}
func TestRosterStaleAckAndLeaveDuringBarrier(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := rosterPeer(t, r, "brawl", "depart")
	readyRoster(t, r, a, 1, 0, strings.Repeat("0", 64))
	b := rosterPeer(t, r, "brawl", "depart")
	g := a.room.barrier.generation
	r.remove(b) // restart barrier for surviving participant
	if a.room.barrier == nil || a.room.barrier.generation <= g {
		t.Fatal("barrier wasn't restarted")
	}
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: g, Tick: 20, Hash: strings.Repeat("0", 64)}); err == nil {
		t.Fatal("stale ack accepted")
	}
	readyRoster(t, r, a, a.room.barrier.generation, 20, strings.Repeat("0", 64))
	if len(a.room.rosterMembers) != 1 {
		t.Fatal("disconnected player in roster")
	}
	r.remove(a)
}

// A late join must replay roster transitions after its checkpoint, otherwise
// the snow cells may match but the number of simulated players will not.
func TestPersistentRosterCommitAppearsInCatchupJournal(t *testing.T) {
	store := &mockStore{}
	r := newRelayWithStore(store)
	a := rosterPeer(t, r, "brawl", "global")
	readyRoster(t, r, a, 1, 0, strings.Repeat("0", 64))
	payload := []byte("opaque binary world at tick 1")
	sum := sha256.Sum256(payload)
	dig := hex.EncodeToString(sum[:])
	for _, m := range []clientMessage{
		{Type: "snapshot_begin", Tick: 1, Size: len(payload), Digest: dig},
		{Type: "snapshot_chunk", Index: 0, Data: base64.StdEncoding.EncodeToString(payload)},
		{Type: "snapshot_commit"},
	} {
		if err := r.handle(a, m); err != nil {
			t.Fatal(err)
		}
	}
	b := rosterPeer(t, r, "brawl", "global")
	if !b.syncing {
		t.Fatal("late player not loading")
	}
	manifest := drain(b)
	if !containsType(manifest, "snapshot_manifest") {
		t.Fatal("manifest missing")
	}
	if err := r.handle(b, clientMessage{Type: "snapshot_get", SnapshotID: 1, Index: 0}); err != nil {
		t.Fatal(err)
	}
	drain(b)
	if err := r.handle(b, clientMessage{Type: "sync_poll", SnapshotID: 1, Cursor: 1}); err != nil {
		// Snapshot cursor is set to 1 because initial roster commit was the first event.
		t.Fatal(err)
	}
	drain(b)
	if err := r.handle(b, clientMessage{Type: "sync_finish", SnapshotID: 1, Cursor: 1}); err != nil {
		t.Fatal(err)
	}
	g := a.room.barrier.generation
	readyRoster(t, r, a, g, 20, strings.Repeat("a", 64))
	readyRoster(t, r, b, g, 20, strings.Repeat("a", 64))
	if len(a.room.state.journal) != 1 || a.room.state.journal[0].Command.Type != "roster_commit" {
		t.Fatalf("roster event missing from command journal: %+v", a.room.state.journal)
	}
	c := rosterPeer(t, r, "brawl", "global")
	if !c.syncing {
		t.Fatal("third joiner should catch up")
	}
	drain(c)
	if err := r.handle(c, clientMessage{Type: "snapshot_get", SnapshotID: 1, Index: 0}); err != nil {
		t.Fatal(err)
	}
	drain(c)
	if err := r.handle(c, clientMessage{Type: "sync_poll", SnapshotID: 1, Cursor: 1}); err != nil {
		t.Fatal(err)
	}
	got := drain(c)
	found := false
	for _, m := range got {
		if m.Type == "sync_batch" {
			for _, entry := range m.Entries {
				if entry.Command.Type == "roster_commit" && entry.Command.Generation == g && entry.Command.Tick == 21 {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatalf("new player did not receive roster history: %+v", got)
	}
	r.remove(c)
	r.remove(b)
	r.remove(a)
}
