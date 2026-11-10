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

// Voting-unit tests use non-persistent rooms and isolate the roster layer.
// Snapshot transfer is exercised separately with a real brawl/global room.
func unitVerifiedJoin(b *rosterBarrier) {
	b.awaitingSnapshot = false
	b.joinSnapshotID = 1
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
	unitVerifiedJoin(b)
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
	unitVerifiedJoin(a.room.barrier)
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
	unitVerifiedJoin(a.room.barrier)
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
	unitVerifiedJoin(a.room.barrier)
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

// A historic confirmed hash must never force a live peer to roll back.
// For admission the relay votes at the frozen current tick and requires
// a fresh snapshot of exactly that tick for the joiner.
func TestV2PrepareDoesNotPinHistoricCheckpoint(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v2peer(t, r, "brawl", "pinned")
	h := strings.Repeat("a", 64)
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: 1, Hash: h}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(a, clientMessage{Type: "hash", Tick: 120, Hash: h, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if a.room.state.confirmedTick != 120 {
		t.Fatal("no remembered historical tick")
	}
	b := v2peer(t, r, "brawl", "pinned")
	barrier := a.room.barrier
	if barrier == nil || barrier.targetPinned || barrier.reference.tick != 0 {
		t.Fatal("historical tick incorrectly pinned")
	}
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: barrier.generation, Tick: 121, Hash: h}); err != nil {
		t.Fatal(err)
	}
	if !barrier.awaitingSnapshot {
		t.Fatal("new admission not waiting for fresh snapshot")
	}
	unitVerifiedJoin(barrier)
	if err := r.handle(b, clientMessage{Type: "roster_join_ready", Generation: barrier.generation, Tick: 121, Hash: h}); err != nil {
		t.Fatal(err)
	}
	r.remove(b)
	r.remove(a)
}

func TestV2FreshSnapshotJoinEndToEndProtocol(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v2peer(t, r, "brawl", "global")
	h := strings.Repeat("c", 64)
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: 1, Tick: 0, Hash: h}); err != nil {
		t.Fatal(err)
	}
	room := a.room
	// Seed an older saved world. Byte semantics belong to Java, not Go.
	old := []byte("opaque old snapshot")
	oldHash := sha256.Sum256(old)
	room.state.snapshot = &worldSnapshot{Tick: 1, Version: 1, Digest: hex.EncodeToString(oldHash[:]), Data: old}
	room.state.snapshotCursor = room.state.nextEvent
	b := v2peer(t, r, "brawl", "global")
	if !b.syncing || room.barrier != nil {
		t.Fatal("joining client voted before old snapshot replay")
	}
	if err := r.handle(b, clientMessage{Type: "snapshot_get", SnapshotID: 1, Index: 0}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(b, clientMessage{Type: "sync_poll", SnapshotID: 1, Cursor: room.state.snapshotCursor}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(b, clientMessage{Type: "sync_finish", SnapshotID: 1, Cursor: room.state.nextEvent}); err != nil {
		t.Fatal(err)
	}
	bar := room.barrier
	if bar == nil || bar.phase != rosterFreeze {
		t.Fatal("no established-cohort vote")
	}
	gen := bar.generation
	drain(a)
	drain(b)
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: gen, Tick: 7, Hash: h}); err != nil {
		t.Fatal(err)
	}
	if bar.phase != rosterJoinVerify || !bar.awaitingSnapshot {
		t.Fatal("no fresh checkpoint required")
	}
	if !containsType(drain(a), "snapshot_requested") {
		t.Fatal("coordinator not asked for fresh frozen snapshot")
	}
	if containsType(drain(b), "roster_checkpoint") {
		t.Fatal("joiner verified before fresh snapshot")
	}
	data := []byte("frozen state at tick seven")
	digest := sha256.Sum256(data)
	for _, msg := range []clientMessage{
		{Type: "snapshot_begin", Tick: 7, Size: len(data), Digest: hex.EncodeToString(digest[:])},
		{Type: "snapshot_chunk", Index: 0, Data: base64.StdEncoding.EncodeToString(data)},
		{Type: "snapshot_commit"},
	} {
		if err := r.handle(a, msg); err != nil {
			t.Fatal(err)
		}
	}
	if !b.syncing || bar.awaitingSnapshot || bar.joinSnapshotID != 2 {
		t.Fatal("fresh state was not offered to joiner")
	}
	if !containsType(drain(b), "snapshot_manifest") {
		t.Fatal("new snapshot manifest not sent")
	}
	if err := r.handle(b, clientMessage{Type: "snapshot_get", SnapshotID: 2, Index: 0}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(b, clientMessage{Type: "sync_poll", SnapshotID: 2, Cursor: room.state.snapshotCursor}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(b, clientMessage{Type: "sync_finish", SnapshotID: 2, Cursor: room.state.nextEvent}); err != nil {
		t.Fatal(err)
	}
	if !containsType(drain(b), "roster_checkpoint") {
		t.Fatal("no dedicated checkpoint after verified download")
	}
	if err := r.handle(b, clientMessage{Type: "roster_join_ready", Generation: gen, Tick: 7, Hash: h}); err != nil {
		t.Fatal(err)
	}
	if !room.rosterMembers[b.id] || room.barrier != nil {
		t.Fatal("joined player not committed")
	}
	r.remove(b)
	r.remove(a)
}

func TestV2FreshRosterCheckpointAllowsTickZero(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v2peer(t, r, "brawl", "global")
	h := strings.Repeat("b", 64)
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: 1, Tick: 0, Hash: h}); err != nil {
		t.Fatal(err)
	}
	// A second player joins before any gameplay tick; bootstrap snapshot may
	// genuinely be tick zero and must not be rejected as invalid metadata.
	room := a.room
	bytes := []byte("zero-tick brawl")
	digest := sha256.Sum256(bytes)
	room.state.snapshot = &worldSnapshot{Version: 1, Tick: 0, Digest: hex.EncodeToString(digest[:]), Data: bytes}
	room.state.snapshotCursor = room.state.nextEvent
	b := v2peer(t, r, "brawl", "global")
	for _, msg := range []clientMessage{
		{Type: "snapshot_get", SnapshotID: 1, Index: 0},
		{Type: "sync_poll", SnapshotID: 1, Cursor: room.state.snapshotCursor},
		{Type: "sync_finish", SnapshotID: 1, Cursor: room.state.nextEvent},
	} {
		if err := r.handle(b, msg); err != nil {
			t.Fatal(err)
		}
	}
	bar := room.barrier
	if bar == nil {
		t.Fatal("no barrier")
	}
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: bar.generation, Tick: 0, Hash: h}); err != nil {
		t.Fatal(err)
	}
	for _, msg := range []clientMessage{
		{Type: "snapshot_begin", Tick: 0, Size: len(bytes), Digest: hex.EncodeToString(digest[:])},
		{Type: "snapshot_chunk", Index: 0, Data: base64.StdEncoding.EncodeToString(bytes)},
		{Type: "snapshot_commit"},
	} {
		if err := r.handle(a, msg); err != nil {
			t.Fatal(err)
		}
	}
	if bar.awaitingSnapshot || bar.joinSnapshotID != 2 {
		t.Fatal("zero-tick fresh snapshot rejected")
	}
	r.remove(b)
	r.remove(a)
}

// A failed join must resume established players without immediately looping
// into another barrier; the newcomer remains unadmitted until reconnect.
func TestV2AbortedJoinResumesActiveAndDoesNotRetry(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v2peer(t, r, "brawl", "abort-resume")
	h := strings.Repeat("f", 64)
	if err := r.handle(a, clientMessage{Type: "roster_ready", Generation: 1, Hash: h}); err != nil {
		t.Fatal(err)
	}
	drain(a)
	b := v2peer(t, r, "brawl", "abort-resume")
	if a.room.barrier == nil {
		t.Fatal("missing join transaction")
	}
	drain(a)
	drain(b)
	a.room.mu.Lock()
	a.room.abortRosterV2Locked("test failed checkpoint")
	stillPaused := a.room.state.paused
	stillBarrier := a.room.barrier != nil
	bSyncing := b.syncing
	a.room.mu.Unlock()
	if stillPaused || stillBarrier || !bSyncing {
		t.Fatalf("stale state paused=%v barrier=%v syncing=%v", stillPaused, stillBarrier, bSyncing)
	}
	msgs := drain(a)
	if !containsType(msgs, "roster_aborted") || !containsType(msgs, "session_resumed") {
		t.Fatalf("active player not resumed: %+v", msgs)
	}
	if containsType(drain(b), "session_resumed") {
		t.Fatal("joiner was incorrectly promoted/resumed")
	}
	r.remove(b)
	r.remove(a)
}

func TestV2WelcomeIdentifiesServerBuild(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	p := v2peer(t, r, "brawl", "version")
	msgs := drain(p)
	found := false
	for _, msg := range msgs {
		if msg.Type == "welcome" {
			found = true
			if msg.ServerVersion != "0.3.5" {
				t.Fatalf("wrong build: %q", msg.ServerVersion)
			}
		}
	}
	if !found {
		t.Fatal("missing welcome")
	}
	r.remove(p)
}
