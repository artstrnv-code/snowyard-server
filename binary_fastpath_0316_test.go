package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"
)

// A SYB3 room must not pass through JSON/Base64 for normal binary peers.
// A mixed-protocol room must still produce the exact legacy JSON envelope.
func TestBinaryFastPathAndTextFallback0316(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	peers := startTestRoyale(t, r, 3)
	defer func() {
		for _, p := range peers {
			r.remove(p)
		}
	}()
	for _, p := range peers {
		drain(p)
	}
	sender, binaryPeer, textPeer := peers[0], peers[1], peers[2]
	sender.binary = true
	binaryPeer.binary = true

	sample := []byte{0xB5, 0x12, 0x45, 0x78, 0x90, 0x22, 0x13, 0x48}
	var input bytes.Buffer
	input.WriteByte(clientBatchMarker)
	putVar(&input, 1)
	input.WriteByte(1)
	putVar(&input, 1)
	putVar(&input, 1)
	putVar(&input, uint64(len(sample)))
	input.Write(sample)
	cmd, err := decodeClientBatch(input.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(cmd.Commands[0].Payload) != 0 {
		t.Fatal("unexpected eager JSON")
	}
	if err := r.handle(sender, cmd); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-binaryPeer.send:
		if len(data) == 0 || data[0] != serverBatchMarker || !bytes.Equal(data[len(data)-len(sample):], sample) {
			t.Fatalf("binary peer received invalid frame: %x", data)
		}
	default:
		t.Fatal("no binary broadcast")
	}
	select {
	case data := <-textPeer.send:
		var msg serverMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Fatal(err)
		}
		if len(msg.Commands) != 1 {
			t.Fatalf("missing commands: %s", data)
		}
		var payload struct {
			B string `json:"b"`
		}
		if err := json.Unmarshal(msg.Commands[0].Payload, &payload); err != nil {
			t.Fatal(err)
		}
		decoded, err := base64.StdEncoding.DecodeString(payload.B)
		if err != nil || !bytes.Equal(decoded, sample) {
			t.Fatal("JSON fallback changed bytes")
		}
	default:
		t.Fatal("no text broadcast")
	}
}

// Persistent replay stays JSON-compatible even when incoming SYB3 is binary.
func TestJournalMaterialization0316(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	p := peerFor(t, r, "brawl", "global")
	defer r.remove(p)
	room := p.room
	sample := []byte{0xB5, 0x13, 0x14}
	payload := wrapBinaryCommand(sample)
	room.mu.Lock()
	room.appendJournalLocked(serverMessage{Type: "command", PlayerID: p.id, Tick: 1, Payload: payload})
	entries := append([]journalEntry(nil), room.state.journal...)
	room.mu.Unlock()
	if len(entries) == 0 {
		t.Fatal("missing journal event")
	}
	var parsed struct {
		B string `json:"b"`
	}
	if err := json.Unmarshal(entries[len(entries)-1].Command.Payload, &parsed); err != nil {
		t.Fatal(err)
	}
	restored, err := base64.StdEncoding.DecodeString(parsed.B)
	if err != nil || !bytes.Equal(restored, sample) {
		t.Fatal("journal not replayable")
	}
}

func BenchmarkBinaryBatchRelay0316(b *testing.B) {
	cmd := serverMessage{Type: "command_batch", PlayerID: 1, Generation: 1,
		Commands: []batchedCommand{{Tick: 100, Sequence: 1, BinaryPayload: []byte{0xB5, 1, 2, 3, 4, 5, 6, 7}}}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := encodeServerBatch(cmd); err != nil {
			b.Fatal(err)
		}
	}
}

func TestTrafficCounters0316(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	p := peerFor(t, r, "battle_royale", "auto")
	defer r.remove(p)
	r.traffic.received(18)
	r.traffic.sent(24)
	p.room.traffic.received(18)
	p.room.traffic.sent(24)
	stats := r.trafficSnapshot()
	if stats["inBytes"] != 18 || stats["outBytes"] != 24 || stats["inMessages"] != 1 || stats["outMessages"] != 1 {
		t.Fatalf("incorrect global counters: %+v", stats)
	}
	if stats["activeRooms"] != 1 || stats["activeRoomInBytes"] != 18 || stats["activeRoomOutBytes"] != 24 {
		t.Fatalf("incorrect anonymous room counters: %+v", stats)
	}
}
