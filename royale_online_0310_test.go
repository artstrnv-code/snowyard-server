package main

import (
	"encoding/json"
	"testing"
)

func startTestRoyale(t *testing.T, r *relay, count int) []*peer {
	t.Helper()
	peers := make([]*peer, count)
	for i := range peers {
		peers[i] = peerFor(t, r, "battle_royale", "auto")
	}
	room := peers[0].room
	room.mu.Lock()
	room.startRoyaleLocked()
	room.finishRoyaleStartLocked()
	room.mu.Unlock()
	return peers
}

func TestRoyaleOnlineRosterAndInput0310(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	peers := startTestRoyale(t, r, 3)
	a, b, c := peers[0], peers[1], peers[2]
	found := false
	for _, event := range drain(b) {
		if event.Type == "match_started" {
			found = true
			if len(event.Players) != 3 || event.BotSlots != 7 || event.Coordinator != a.id {
				t.Fatalf("bad Royale start packet: %+v", event)
			}
		}
	}
	if !found {
		t.Fatal("missing match_started")
	}
	drain(a)
	drain(c)
	cmd := clientMessage{Type: "command_batch", Commands: []batchedCommand{
		{Tick: 1, Sequence: 1, Payload: json.RawMessage(`{"b":"test1"}`)},
		{Tick: 2, Sequence: 2, Payload: json.RawMessage(`{"b":"test2"}`)},
	}}
	if err := r.handle(b, cmd); err != nil {
		t.Fatal(err)
	}
	if !containsType(drain(a), "command_batch") || !containsType(drain(c), "command_batch") {
		t.Fatal("Royale human commands not relayed")
	}
	for _, p := range peers {
		r.remove(p)
	}
}

func TestRoyaleHostHandoffAndDeparture0310(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	peers := startTestRoyale(t, r, 3)
	a, b, c := peers[0], peers[1], peers[2]
	for _, p := range peers {
		drain(p)
	}
	if err := r.handle(a, clientMessage{Type: "bot_command", BotID: 1, Tick: 30, Payload: json.RawMessage(`{"targets":[]}`)}); err != nil {
		t.Fatal(err)
	}
	drain(b)
	drain(c)
	// Report distinct last accepted input ticks for a meaningful future boundary.
	for i, p := range peers {
		if err := r.handle(p, clientMessage{Type: "command", Tick: uint64(18 + i), Sequence: 1, Payload: json.RawMessage(`{"b":"t"}`)}); err != nil {
			t.Fatal(err)
		}
	}
	drain(b)
	drain(c)
	r.remove(a)
	if b.room.state.coordinator != b.id || b.room.state.paused {
		t.Fatal("Royale coordinator migration incorrectly pauses lockstep")
	}
	if err := r.handle(b, clientMessage{Type: "bot_command", BotID: 1, Tick: 20, Payload: json.RawMessage(`{"targets":[]}`)}); err != nil {
		t.Fatalf("new coordinator cannot resume AI at earlier tick: %v", err)
	}
	var departure *serverMessage
	for _, event := range drain(c) {
		if event.Type == "royale_departure" {
			e := event
			departure = &e
		}
	}
	if departure == nil || departure.PlayerID != a.id || departure.LastCommandTick != 18 || departure.Tick != 32 {
		t.Fatalf("bad departure transition: %+v", departure)
	}
	r.remove(b)
	if !containsType(drain(c), "offline_fallback") {
		t.Fatal("solo survivor was not returned to offline Royale")
	}
	r.remove(c)
}
