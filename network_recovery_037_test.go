package main

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// Verify restart only affects a JOINING client. The old checkpoint cannot
// safely be replayed again once its deterministic input window was exhausted.
func TestV3CatchupRestartWaitsForFreshCheckpoint(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v3RecoveryPeer(t, r)
	room := a.room
	data := []byte("checkpoint")
	digest := sha256.Sum256(data)
	room.mu.Lock()
	room.state.snapshot = &worldSnapshot{Version: 1, Tick: 1, Data: data, Digest: hex.EncodeToString(digest[:])}
	room.mu.Unlock()
	b := v3RecoveryPeer(t, r)
	if !b.syncing {
		t.Fatal("second player should begin checkpoint sync")
	}
	drain(a)
	drain(b)
	b.awaitingJoin = true
	b.syncing = false // replay caught up, but the joiner could not step its lockstep window
	b.syncID = 1
	if err := r.handle(b, clientMessage{Type: "sync_restart"}); err != nil {
		t.Fatal(err)
	}
	if !b.syncing || b.awaitingJoin || b.syncID != 0 {
		t.Fatal("restart did not reset joiner")
	}
	msgs := drain(b)
	if !containsType(msgs, "sync_wait") || containsType(msgs, "snapshot_manifest") {
		t.Fatalf("restart must wait for fresh checkpoint: %+v", msgs)
	}
	if !containsType(drain(a), "snapshot_requested") {
		t.Fatal("coordinator not asked for fresh checkpoint")
	}
	if err := r.handle(a, clientMessage{Type: "sync_restart"}); err == nil {
		t.Fatal("active player restarted without coordinated recovery")
	}
	r.remove(b)
	r.remove(a)
}

func v3RecoveryPeer(t *testing.T, r *relay) *peer {
	t.Helper()
	p := &peer{id: r.nextID.Add(1), nickname: "recover", rosterVersion: rosterProtocolV3, binary: true, send: make(chan []byte, 256)}
	if err := r.add(p, "brawl", "global"); err != nil {
		t.Fatal(err)
	}
	return p
}

// A clean departure must publish a deterministic roster transition rather
// than freeze the remaining player. Explicit recovery still guards rooms
// which were already paused or had unverified state.
func TestV3DepartureSchedulesDeterministicTransition(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v3RecoveryPeer(t, r)
	b := v3RecoveryPeer(t, r)
	room := a.room
	room.mu.Lock()
	room.rosterMembers[b.id] = true
	b.syncing = false
	b.awaitingJoin = false
	room.rosterApplied = 2
	room.rosterGeneration = 2
	room.state.coordinator = a.id
	room.mu.Unlock()
	drain(b)
	r.remove(a)
	if room.state.paused || room.state.recoveryRequired {
		t.Fatal("clean departure incorrectly froze the survivor")
	}
	if !containsType(drain(b), "roster_commit") {
		t.Fatal("clean departure did not publish membership commit")
	}
	r.remove(b)
}

func TestV3SoloRestoreClearsDeparturePause(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v3RecoveryPeer(t, r)
	room := a.room
	room.mu.Lock()
	room.rosterMembers[a.id] = true
	room.state.coordinator = a.id
	room.mu.Unlock()
	b := v3RecoveryPeer(t, r)
	r.remove(a)
	room.mu.Lock()
	b.syncing = false
	b.awaitingJoin = true
	err := room.restoreSoloV3Locked(b, 15)
	paused := room.state.paused
	blocked := room.state.recoveryRequired
	room.mu.Unlock()
	if err != nil || paused || blocked {
		t.Fatalf("solo restore remains frozen: err=%v paused=%v blocked=%v", err, paused, blocked)
	}
	if !containsType(drain(b), "roster_commit") {
		t.Fatal("no deterministic solo roster commit")
	}
	r.remove(b)
}
