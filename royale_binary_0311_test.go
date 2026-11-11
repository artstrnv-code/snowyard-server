package main

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

// Royale opts into SYB2 without changing the server's handling of game ticks.
func TestRoyaleBinaryBroadcast0311(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	peers := startTestRoyale(t, r, 2)
	sender, receiver := peers[0], peers[1]
	receiver.binary = true
	drain(sender)
	drain(receiver)

	payload, err := json.Marshal(map[string]string{"b": base64.StdEncoding.EncodeToString([]byte{0xB2, 0x01})})
	if err != nil {
		t.Fatal(err)
	}
	msg := clientMessage{Type: "command_batch", Commands: []batchedCommand{{Tick: 1, Sequence: 1, Payload: payload}}}
	if err := r.handle(sender, msg); err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-receiver.send:
		if len(frame) == 0 || frame[0] != serverBatchMarker {
			t.Fatalf("expected SYB2 binary packet, got %q", frame)
		}
	default:
		t.Fatal("binary receiver did not get command batch")
	}
	for _, p := range peers {
		r.remove(p)
	}
}
