package main

import "testing"

// The survivor acknowledges receipt without changing the room membership or
// delaying the local transition for a server round trip.
func TestRoyaleHandoffAck0314(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	peers := startTestRoyale(t, r, 2)
	a, b := peers[0], peers[1]
	drain(a)
	drain(b)
	if err := r.handle(b, clientMessage{Type: "handoff_ack"}); err == nil {
		t.Fatal("server accepted handoff acknowledgement before room became solo")
	}
	r.remove(a)
	if !containsType(drain(b), "offline_fallback") {
		t.Fatal("remaining client did not receive fallback notification")
	}
	if err := r.handle(b, clientMessage{Type: "handoff_ack"}); err != nil {
		t.Fatalf("valid handoff acknowledgement rejected: %v", err)
	}
	if r.size() != 1 {
		t.Fatalf("handoff acknowledgement changed membership: %d", r.size())
	}
	r.remove(b)
}
