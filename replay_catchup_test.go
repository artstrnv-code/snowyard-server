package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

// A live 60 TPS stream must not cause sync_finish to race with an advancing tail.
func TestLateJoinReplayFreezesFinalCursor(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := rosterPeer(t, r, "brawl", "global")
	g := a.room.barrier.generation
	readyRoster(t, r, a, g, 0, strings.Repeat("a", 64))
	drain(a)
	room := a.room
	room.mu.Lock()
	data := []byte("valid-test-snapshot")
	sum := sha256.Sum256(data)
	room.state.snapshot = &worldSnapshot{Tick: 1, Version: 1, Digest: hex.EncodeToString(sum[:]), Data: data}
	room.state.snapshotCursor = room.state.nextEvent
	room.mu.Unlock()
	b := rosterPeer(t, r, "brawl", "global")
	drain(a)
	drain(b)
	if err := r.handle(b, clientMessage{Type: "snapshot_get", SnapshotID: 1, Index: 0}); err != nil {
		t.Fatal(err)
	}
	drain(b)
	cmd := clientMessage{Type: "command", Tick: 2, Sequence: 1, Generation: g, Payload: json.RawMessage(`{"b":1}`)}
	if err := r.handle(a, cmd); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(b, clientMessage{Type: "sync_poll", SnapshotID: 1, Cursor: room.state.snapshotCursor}); err != nil {
		t.Fatal(err)
	}
	messages := drain(b)
	var cursor uint64
	gotBatch := false
	for _, m := range messages {
		if m.Type == "sync_batch" {
			cursor = m.Cursor
			gotBatch = true
		}
	}
	if !gotBatch || cursor != room.state.nextEvent {
		t.Fatalf("last batch did not capture tail %+v", messages)
	}
	if !room.state.paused || room.catchupOwner != b.id {
		t.Fatal("final replay did not freeze active room")
	}
	if err := r.handle(a, clientMessage{Type: "command", Tick: 3, Sequence: 2, Generation: g, Payload: json.RawMessage(`{"b":2}`)}); err == nil {
		t.Fatal("commands accepted while joiner finishes")
	}
	if err := r.handle(b, clientMessage{Type: "sync_finish", SnapshotID: 1, Cursor: cursor}); err != nil {
		t.Fatal(err)
	}
	if b.syncing || room.catchupOwner != 0 || room.barrier == nil {
		t.Fatal("joiner not transferred to membership barrier")
	}
	r.remove(b)
	r.remove(a)
}
