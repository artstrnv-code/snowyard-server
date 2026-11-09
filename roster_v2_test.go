package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
)

// These tests isolate v2 from legacy v1, and cover the exact P2/P3 race:
// a ready joiner must not vote among active members and a third arrival
// cannot overwrite the transaction while it is being verified.
func v2peer(t *testing.T, r *relay, mode, name string) *peer {
	t.Helper()
	p := &peer{id: r.nextID.Add(1), nickname: "v2", rosterVersion: 2, send: make(chan []byte, 256)}
	if err := r.add(p, mode, name); err != nil {
		t.Fatal(err)
	}
	return p
}
func TestV2StagedJoinAndQueuedThirdPlayer(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v2peer(t, r, "brawl", "testv2")
	b := a.room.barrier
	if b == nil || len(b.members) != 1 {
		t.Fatal("bootstrap not isolated")
	}
	hash := strings.Repeat("a", 64)
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: b.generation, Tick: 0, Hash: hash}); err != nil {
		t.Fatal(err)
	}
	if len(a.room.rosterMembers) != 1 {
		t.Fatal("bootstrap not committed")
	}
	c := v2peer(t, r, "brawl", "testv2")
	if c.syncing {
		t.Fatal("test room should not need snapshot")
	}
	b = a.room.barrier
	if b == nil || len(b.members) != 1 || b.members[0] != a.id || len(b.joining) != 1 || b.joining[0] != c.id {
		t.Fatalf("joiner incorrectly voting: %+v", b)
	}
	d := v2peer(t, r, "brawl", "testv2")
	if a.room.barrier != b {
		t.Fatal("third player reset active vote")
	}
	if err := r.handle(c, clientMessage{Type: "roster_ready", Generation: b.generation, Tick: 7, Hash: hash}); err == nil {
		t.Fatal("joining player voted as active")
	}
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: b.generation, Tick: 7, Hash: hash}); err != nil {
		t.Fatal(err)
	}
	if b.phase != rosterJoinVerify {
		t.Fatal("no join verification")
	}
	if err := r.handle(d, clientMessage{Type: "roster_join_ready", Generation: b.generation, Tick: 7, Hash: hash}); err == nil {
		t.Fatal("wrong joiner admitted")
	}
	if err := r.handle(c, clientMessage{Type: "roster_join_ready", Generation: b.generation, Tick: 8, Hash: hash}); err == nil {
		t.Fatal("wrong tick accepted")
	}
	if err := r.handle(c, clientMessage{Type: "roster_join_ready", Generation: b.generation, Tick: 7, Hash: hash}); err != nil {
		t.Fatal(err)
	}
	if !a.room.rosterMembers[c.id] || a.room.rosterMembers[d.id] {
		t.Fatal("incorrect roster commit")
	}
	if a.room.barrier == nil || len(a.room.barrier.joining) != 1 || a.room.barrier.joining[0] != d.id {
		t.Fatal("queued third player not processed")
	}
	r.remove(d)
	r.remove(c)
	r.remove(a)
}
func TestV2ActiveDisagreementCannotCommit(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v2peer(t, r, "brawl", "multiple")
	h := strings.Repeat("1", 64)
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: 1, Hash: h}); err != nil {
		t.Fatal(err)
	}
	b := v2peer(t, r, "brawl", "multiple")
	gen := a.room.barrier.generation
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: gen, Tick: 4, Hash: h}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(b, clientMessage{Type: "roster_join_ready", Generation: gen, Tick: 4, Hash: h}); err != nil {
		t.Fatal(err)
	}
	c := v2peer(t, r, "brawl", "multiple")
	gen = a.room.barrier.generation
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: gen, Tick: 12, Hash: h}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(b, clientMessage{Type: "roster_ready", Generation: gen, Tick: 13, Hash: h}); err != nil {
		t.Fatal(err)
	}
	if a.room.barrier == nil || a.room.barrier.phase != rosterFreeze || a.room.rosterMembers[c.id] {
		t.Fatal("conflicting active state committed")
	}
	if err := r.handle(b, clientMessage{Type: "roster_ready", Generation: gen, Tick: 12, Hash: h}); err != nil {
		t.Fatal(err)
	}
	if a.room.barrier.phase != rosterJoinVerify {
		t.Fatal("no retry after correction")
	}
	r.remove(c)
	r.remove(b)
	r.remove(a)
}
func TestV2RoomRejectsMixedRosterVersions(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v2peer(t, r, "brawl", "protocol")
	p := &peer{id: r.nextID.Add(1), nickname: "v1", rosterVersion: 1, send: make(chan []byte, 256)}
	if err := r.add(p, "brawl", "protocol"); err == nil {
		t.Fatal("mixed v1/v2 accepted")
	}
	r.remove(a)
}
func TestV2PersistentJoinerWaitsForSnapshotBeforeVoting(t *testing.T) {
	store := &mockStore{}
	r := newRelayWithStore(store)
	a := v2peer(t, r, "brawl", "global")
	h := strings.Repeat("a", 64)
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: 1, Hash: h}); err != nil {
		t.Fatal(err)
	}
	payload := []byte("verified opaque world data")
	sha := sha256.Sum256(payload)
	for _, msg := range []clientMessage{{Type: "snapshot_begin", Tick: 1, Size: len(payload), Digest: hex.EncodeToString(sha[:])}, {Type: "snapshot_chunk", Data: base64.StdEncoding.EncodeToString(payload)}, {Type: "snapshot_commit"}} {
		if err := r.handle(a, msg); err != nil {
			t.Fatal(err)
		}
	}
	b := v2peer(t, r, "brawl", "global")
	if !b.syncing || a.room.barrier != nil {
		t.Fatal("downloading peer entered vote")
	}
	if err := r.handle(b, clientMessage{Type: "roster_ready", Generation: 1, Hash: h}); err == nil {
		t.Fatal("syncing peer voted")
	}
	r.remove(b)
	r.remove(a)
}

func TestV2ActiveDepartureNeedsNewRosterCommit(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v2peer(t, r, "brawl", "leavev2")
	hash := strings.Repeat("a", 64)
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: 1, Hash: hash}); err != nil {
		t.Fatal(err)
	}
	b := v2peer(t, r, "brawl", "leavev2")
	gen := a.room.barrier.generation
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: gen, Tick: 10, Hash: hash}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(b, clientMessage{Type: "roster_join_ready", Generation: gen, Tick: 10, Hash: hash}); err != nil {
		t.Fatal(err)
	}
	r.remove(b)
	if !a.room.rosterChangePending || a.room.barrier == nil {
		t.Fatal("lost the required departure barrier")
	}
	if len(a.room.barrier.members) != 1 || a.room.barrier.members[0] != a.id {
		t.Fatal("wrong remaining voters")
	}
	newGen := a.room.barrier.generation
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: newGen, Tick: 20, Hash: hash}); err != nil {
		t.Fatal(err)
	}
	if a.room.rosterChangePending || a.room.barrier != nil || a.room.rosterMembers[b.id] {
		t.Fatal("departure not committed")
	}
	r.remove(a)
}

func TestV2RecoveryRequiresEveryActiveMemberAndValidSnapshot(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v2peer(t, r, "brawl", "recovery")
	h := strings.Repeat("1", 64)
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: 1, Hash: h}); err != nil {
		t.Fatal(err)
	}
	b := v2peer(t, r, "brawl", "recovery")
	g := a.room.barrier.generation
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: g, Tick: 11, Hash: h}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(b, clientMessage{Type: "roster_join_ready", Generation: g, Tick: 11, Hash: h}); err != nil {
		t.Fatal(err)
	}
	s := a.room.state
	s.snapshot = &worldSnapshot{Version: 3, Tick: 10, Digest: strings.Repeat("f", 64), Data: []byte("opaque")}
	s.paused = true
	s.recoveryRequired = true
	s.recoveryTick = 120
	s.recoveryHash = h
	if err := r.handle(a, clientMessage{Type: "coordinator_resume"}); err == nil {
		t.Fatal("coordinator bypassed recovery")
	}
	if err := r.handle(a, clientMessage{Type: "recovery_ready", SnapshotID: 2, Tick: 120, Hash: h}); err == nil {
		t.Fatal("old snapshot accepted")
	}
	if err := r.handle(a, clientMessage{Type: "recovery_ready", SnapshotID: 3, Tick: 121, Hash: h}); err == nil {
		t.Fatal("wrong tick accepted")
	}
	if err := r.handle(a, clientMessage{Type: "recovery_ready", SnapshotID: 3, Tick: 120, Hash: h}); err != nil {
		t.Fatal(err)
	}
	if !s.paused {
		t.Fatal("one vote lifted recovery pause")
	}
	if err := r.handle(b, clientMessage{Type: "recovery_ready", SnapshotID: 3, Tick: 120, Hash: h}); err != nil {
		t.Fatal(err)
	}
	if s.paused || s.recoveryRequired {
		t.Fatal("unanimous valid recovery did not resume")
	}
	r.remove(b)
	r.remove(a)
}

func TestV2PrepareUsesPinnedConfirmedCheckpoint(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v2peer(t, r, "brawl", "pinned")
	h := strings.Repeat("a", 64)
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: 1, Hash: h}); err != nil {
		t.Fatal(err)
	}
	// A solo player can attest its own checkpoint; the relay remembers it.
	if err := r.handle(a, clientMessage{Type: "hash", Tick: 120, Hash: h, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if a.room.state.confirmedTick != 120 || a.room.state.confirmedHash != h {
		t.Fatal("checkpoint not remembered")
	}
	b := v2peer(t, r, "brawl", "pinned")
	barrier := a.room.barrier
	if barrier == nil || !barrier.targetPinned || barrier.reference.tick != 120 {
		t.Fatal("no pinned target")
	}
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: barrier.generation, Tick: 121, Hash: h}); err == nil {
		t.Fatal("wrong tick accepted")
	}
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: barrier.generation, Tick: 120, Hash: h}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(b, clientMessage{Type: "roster_join_ready", Generation: barrier.generation, Tick: 120, Hash: h}); err != nil {
		t.Fatal(err)
	}
	r.remove(b)
	r.remove(a)
}
