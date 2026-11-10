package main

import (
	"encoding/json"
	"testing"
)

// A peer may leave without submitting any tick; everyone is still at tick 0.
// The surviving peer must apply a single roster commit at tick 1, not freeze.
func TestV3DepartureAtFirstTick(t *testing.T) {
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
	if room.state.paused || room.state.recoveryRequired || room.rosterStartTick != 1 {
		t.Fatal("departure at tick 0 must not freeze the surviving player")
	}
	commits := 0
	for _, m := range drain(b) {
		if m.Type == "roster_commit" {
			commits++
			if m.Tick != 1 || m.Generation != 3 || len(m.Players) != 1 || m.Players[0].ID != b.id || m.Coordinator != b.id {
				t.Fatalf("invalid departure commit: %+v", m)
			}
		}
	}
	if commits != 1 {
		t.Fatalf("expected one departure commit, got %d", commits)
	}
	r.remove(b)
}

func TestV3DepartureCutoverAndOldInputs(t *testing.T) {
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
	room.rosterStartTick = 1
	room.state.coordinator = a.id
	room.mu.Unlock()
	drain(a)
	drain(b)
	payload := json.RawMessage(`{"b":"AA=="}`)
	for tick := uint64(1); tick <= 5; tick++ {
		if err := r.handle(a, clientMessage{Type: "command", Generation: 2, Tick: tick, Sequence: tick, Payload: payload}); err != nil {
			t.Fatal(err)
		}
		if err := r.handle(b, clientMessage{Type: "command", Generation: 2, Tick: tick, Sequence: tick, Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	// One client has already speculatively sent old-gen commands for future ticks.
	if err := r.handle(b, clientMessage{Type: "command", Generation: 2, Tick: 6, Sequence: 6, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	drain(a)
	drain(b)
	r.remove(a)
	if room.state.paused || room.state.recoveryRequired || room.rosterStartTick != 6 || room.rosterApplied != 3 {
		t.Fatalf("invalid live departure transition, pause=%v recovery=%v tick=%d gen=%d", room.state.paused, room.state.recoveryRequired, room.rosterStartTick, room.rosterApplied)
	}
	found := false
	for _, m := range drain(b) {
		if m.Type == "roster_commit" {
			found = true
			if m.Tick != 6 || len(m.Players) != 1 || m.Players[0].ID != b.id {
				t.Fatalf("bad departure commit: %+v", m)
			}
		}
	}
	if !found {
		t.Fatal("survivor did not receive transition")
	}
	// Old in-flight input must be harmless and never block the new generation.
	if err := r.handle(b, clientMessage{Type: "command", Generation: 2, Tick: 7, Sequence: 7, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(b, clientMessage{Type: "command_batch", Generation: 2, Commands: []batchedCommand{{Tick: 7, Sequence: 8, Payload: payload}}}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(b, clientMessage{Type: "command_batch", Generation: 2, Commands: []batchedCommand{
		{Tick: 5, Sequence: 5, Payload: payload},
		{Tick: 6, Sequence: 6, Payload: payload},
	}}); err != nil {
		t.Fatalf("crossing old-generation batch raised a fatal error: %v", err)
	}
	if err := r.handle(b, clientMessage{Type: "command", Generation: 3, Tick: 6, Sequence: 1, Payload: payload}); err != nil {
		t.Fatalf("new generation blocked: %v", err)
	}
	if room.peers[b.id].lastCommandTick != 6 {
		t.Fatal("new-generation clock did not restart")
	}
	r.remove(b)
}

func TestV3DepartureDuringExistingPauseRemainsSafe(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v3RecoveryPeer(t, r)
	b := v3RecoveryPeer(t, r)
	room := a.room
	room.mu.Lock()
	room.rosterMembers[b.id] = true
	b.syncing = false
	room.state.paused = true
	room.mu.Unlock()
	drain(b)
	r.remove(a)
	if !room.state.paused || !room.state.recoveryRequired {
		t.Fatal("already-paused room must require checkpoint recovery")
	}
	if containsType(drain(b), "roster_commit") {
		t.Fatal("unsafe transition during recovery")
	}
	r.remove(b)
}

func TestV3DepartureTooFarBehindRequiresRecovery(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := v3RecoveryPeer(t, r)
	b := v3RecoveryPeer(t, r)
	room := a.room
	room.mu.Lock()
	room.rosterMembers[b.id] = true
	room.rosterApplied = 2
	room.rosterGeneration = 2
	b.syncing = false
	b.awaitingJoin = false
	a.lastCommandTick = 250
	b.lastCommandTick = 100
	room.mu.Unlock()
	drain(b)
	r.remove(a)
	if !room.state.paused || !room.state.recoveryRequired {
		t.Fatal("unsafe catch-up must require explicit checkpoint recovery")
	}
	if containsType(drain(b), "roster_commit") {
		t.Fatal("published departure commit across an exhausted input window")
	}
	r.remove(b)
}
