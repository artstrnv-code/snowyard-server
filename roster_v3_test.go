package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

func v3Peer(t *testing.T, r *relay) *peer {
	t.Helper()
	p := &peer{id: r.nextID.Add(1), nickname: "v3", rosterVersion: 3, send: make(chan []byte, 256)}
	if err := r.add(p, "brawl", "global"); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestV3SnapshotLiveJoinDoesNotPauseExistingPlayer(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v3Peer(t, r)
	room := a.room
	if room.state.paused || !room.rosterMembers[a.id] || room.rosterApplied == 0 {
		t.Fatal("first player was not bootstrapped without freeze")
	}
	drain(a)
	data := []byte("test opaque snapshot")
	digest := sha256.Sum256(data)
	room.mu.Lock()
	room.state.snapshot = &worldSnapshot{Tick: 0, Version: 1, Digest: hex.EncodeToString(digest[:]), Data: data}
	room.state.snapshotCursor = room.state.nextEvent
	room.mu.Unlock()
	b := v3Peer(t, r)
	if !b.syncing || room.state.paused || room.barrier != nil {
		t.Fatal("second player paused active room")
	}
	drain(b)
	if err := r.handle(b, clientMessage{Type: "snapshot_get", SnapshotID: 1, Index: 0}); err != nil {
		t.Fatal(err)
	}
	drain(b)
	payload := json.RawMessage(`{"b":"AA=="}`)
	if err := r.handle(a, clientMessage{Type: "command", Generation: 1, Tick: 1, Sequence: 1, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(b, clientMessage{Type: "sync_poll", SnapshotID: 1, Cursor: room.state.snapshotCursor}); err != nil {
		t.Fatal(err)
	}
	var cursor uint64
	for _, m := range drain(b) {
		if m.Type == "sync_batch" {
			cursor = m.Cursor
		}
	}
	if cursor == 0 {
		t.Fatal("no replay journal")
	}
	// A command arrives while the joining peer is confirming its last replay cursor.
	if err := r.handle(a, clientMessage{Type: "command", Generation: 1, Tick: 2, Sequence: 2, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(b, clientMessage{Type: "sync_finish", SnapshotID: 1, Cursor: cursor}); err != nil {
		t.Fatal(err)
	}
	if room.state.paused || b.syncing || !b.awaitingJoin {
		t.Fatal("live transition froze room or lost subscriber")
	}
	seen := false
	for _, m := range drain(b) {
		if m.Type == "sync_live" {
			seen = true
			if len(m.Entries) != 1 || m.Entries[0].Command.Tick != 2 {
				t.Fatalf("missing final live gap %+v", m)
			}
		}
	}
	if !seen {
		t.Fatal("no atomic sync_live handoff")
	}
	if err := r.handle(a, clientMessage{Type: "command", Generation: 1, Tick: 3, Sequence: 3, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(b, clientMessage{Type: "join_progress", Tick: 3}); err != nil {
		t.Fatal(err)
	}
	plan := room.v3Plan
	if plan == nil || room.state.paused || room.barrier != nil || plan.effectiveTick <= 3 {
		t.Fatal("plan was not scheduled for a future tick")
	}
	// A third download is not allowed to overwrite P2's scheduled transaction.
	c := v3Peer(t, r)
	if !c.syncing || room.v3Plan != plan || room.state.paused {
		t.Fatal("P3 altered P2's live join transaction")
	}
	drain(c)
	if err := r.handle(b, clientMessage{Type: "join_plan_ready", Generation: plan.generation, Tick: plan.effectiveTick}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(a, clientMessage{Type: "join_plan_ready", Generation: plan.generation, Tick: plan.effectiveTick}); err != nil {
		t.Fatal(err)
	}
	if room.v3Plan != nil || room.state.paused || room.rosterApplied != plan.generation {
		t.Fatal("committed join paused active room")
	}
	if err := r.handle(a, clientMessage{Type: "command", Generation: 1, Tick: plan.effectiveTick - 1, Sequence: 4, Payload: payload}); err != nil {
		t.Fatalf("old-gen input before boundary rejected: %v", err)
	}
	if err := r.handle(a, clientMessage{Type: "command", Generation: plan.generation, Tick: plan.effectiveTick, Sequence: 5, Payload: payload}); err != nil {
		t.Fatalf("new-gen input after boundary rejected: %v", err)
	}
	if err := r.handle(b, clientMessage{Type: "command", Generation: plan.generation, Tick: plan.effectiveTick, Sequence: 1, Payload: payload}); err != nil {
		t.Fatalf("joining peer blocked after commit: %v", err)
	}
	if err := r.handle(a, clientMessage{Type: "command", Generation: 1, Tick: plan.effectiveTick + 1, Sequence: 6, Payload: payload}); err == nil {
		t.Fatal("old generation accepted beyond future boundary")
	}
	r.remove(c)
	r.remove(b)
	r.remove(a)
}

func TestV3SavedWorldSoloRecoveryWithoutVoting(t *testing.T) {
	data := []byte("old world with actor IDs")
	h := sha256.Sum256(data)
	store := &mockStore{v: &worldSnapshot{Tick: 904, Version: 1, Digest: hex.EncodeToString(h[:]), Data: data}}
	r := newRelayWithStore(store)
	p := v3Peer(t, r)
	if !p.syncing || len(p.room.rosterMembers) != 0 {
		t.Fatal("stale world was bootstrapped before download")
	}
	drain(p)
	if err := r.handle(p, clientMessage{Type: "snapshot_get", SnapshotID: 1, Index: 0}); err != nil {
		t.Fatal(err)
	}
	drain(p)
	if err := r.handle(p, clientMessage{Type: "sync_poll", SnapshotID: 1, Cursor: 0}); err != nil {
		t.Fatal(err)
	}
	drain(p)
	if err := r.handle(p, clientMessage{Type: "sync_finish", SnapshotID: 1, Cursor: 0}); err != nil {
		t.Fatal(err)
	}
	drain(p)
	if err := r.handle(p, clientMessage{Type: "join_progress", Tick: 904}); err != nil {
		t.Fatal(err)
	}
	if !p.room.rosterMembers[p.id] || p.room.rosterStartTick != 905 || p.room.state.paused {
		t.Fatal("solo restore did not advance at the snapshot boundary")
	}
	r.remove(p)
}

func TestV3CancellingUnacknowledgedJoinKeepsGameActive(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v3Peer(t, r)
	drain(a)
	b := v3Peer(t, r)
	room := a.room
	room.mu.Lock()
	// Isolate the roster plan without coupling this test to binary snapshots.
	b.syncing = false
	b.awaitingJoin = true
	err := room.planV3Locked(b.id)
	if err == nil {
		room.cancelV3PlanLocked("test cancellation")
	}
	paused := room.state.paused
	room.mu.Unlock()
	if err != nil || paused || !room.rosterMembers[a.id] || room.rosterMembers[b.id] {
		t.Fatal("failed join affected existing match")
	}
	r.remove(b)
	r.remove(a)
}
