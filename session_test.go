package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type mockStore struct {
	v     *worldSnapshot
	saves int
}

func (m *mockStore) Load() (*worldSnapshot, error) {
	if m.v == nil {
		return nil, os.ErrNotExist
	}
	return m.v, nil
}
func (m *mockStore) Save(v *worldSnapshot) error { m.v = v; m.saves++; return nil }
func peerFor(t *testing.T, r *relay, mode, name string) *peer {
	t.Helper()
	p := &peer{id: r.nextID.Add(1), nickname: "test", send: make(chan []byte, 256)}
	if err := r.add(p, mode, name); err != nil {
		t.Fatal(err)
	}
	return p
}
func drain(p *peer) []serverMessage {
	var out []serverMessage
	for {
		select {
		case b := <-p.send:
			var m serverMessage
			_ = json.Unmarshal(b, &m)
			out = append(out, m)
		default:
			return out
		}
	}
}
func containsType(msgs []serverMessage, want string) bool {
	for _, m := range msgs {
		if m.Type == want {
			return true
		}
	}
	return false
}
func TestRoyaleStartsAtTenAndBotsAreCoordinatorOnly(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	var players []*peer
	for i := 0; i < royaleSlots; i++ {
		players = append(players, peerFor(t, r, "battle_royale", "auto"))
	}
	room := players[0].room
	for _, p := range players {
		if p.room != room {
			t.Fatal("matchmaking split waiting players")
		}
	}
	room.mu.Lock()
	if !room.state.started || room.state.botSlots != 0 {
		t.Fatal("full match did not start without bots")
	}
	room.mu.Unlock()
	if err := r.handle(players[1], clientMessage{Type: "bot_command", BotID: 1, Tick: 1, Payload: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("non-coordinator bot command accepted")
	}
	for _, p := range players {
		r.remove(p)
	}
}
func TestRoyaleTimeoutAllocatesBots(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := peerFor(t, r, "battle_royale", "auto")
	b := peerFor(t, r, "battle_royale", "auto")
	drain(a)
	drain(b)
	r.startRoyale(a.room)
	if a.room.state.botSlots != 8 {
		t.Fatalf("expected 8 bot slots, got %d", a.room.state.botSlots)
	}
	if !containsType(drain(a), "match_started") {
		t.Fatal("no match_started event")
	}
	if err := r.handle(a, clientMessage{Type: "bot_command", BotID: 1, Tick: 1, Payload: json.RawMessage(`{"left":true}`)}); err != nil {
		t.Fatal(err)
	}
	if err := r.handle(a, clientMessage{Type: "bot_command", BotID: 1, Tick: 1, Payload: json.RawMessage(`{"left":true}`)}); err == nil {
		t.Fatal("duplicate bot tick accepted")
	}
	if !containsType(drain(b), "bot_command") {
		t.Fatal("no bot command forwarded")
	}
	c := peerFor(t, r, "battle_royale", "auto")
	if c.room == a.room {
		t.Fatal("player joined already started match")
	}
	for _, p := range []*peer{a, b, c} {
		r.remove(p)
	}
}
func TestCoordinatorHandoffPauses(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := peerFor(t, r, "brawl", "global")
	b := peerFor(t, r, "brawl", "global")
	drain(b)
	r.remove(a)
	if b.room.state.coordinator != b.id || !b.room.state.paused {
		t.Fatal("handoff did not pause")
	}
	if err := r.handle(b, clientMessage{Type: "command", Tick: 1, Sequence: 1, Payload: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("command allowed during coordinator handoff")
	}
	if err := r.handle(b, clientMessage{Type: "coordinator_resume"}); err != nil {
		t.Fatal(err)
	}
	if b.room.state.paused {
		t.Fatal("resume failed")
	}
	r.remove(b)
}
func TestSnapshotUploadReplaysCommandsAndPersists(t *testing.T) {
	store := &mockStore{}
	r := newRelayWithStore(store)
	a := peerFor(t, r, "brawl", "global")
	b := peerFor(t, r, "brawl", "global")
	drain(a)
	drain(b)
	if !b.syncing {
		t.Fatal("late join should be loading")
	}
	if err := r.handle(a, clientMessage{Type: "command", Tick: 101, Sequence: 1, Payload: json.RawMessage(`{"x":1}`)}); err != nil {
		t.Fatal(err)
	}
	content := []byte("SYWS simulated snow-cell payload, opaque to Go")
	hash := sha256.Sum256(content)
	digest := hex.EncodeToString(hash[:])
	begin := clientMessage{Type: "snapshot_begin", Tick: 100, Size: len(content), Digest: digest}
	if err := r.handle(b, begin); err == nil {
		t.Fatal("non-coordinator uploaded world")
	}
	for _, m := range []clientMessage{begin, {Type: "snapshot_chunk", Index: 0, Data: base64.StdEncoding.EncodeToString(content)}, {Type: "snapshot_commit"}} {
		if err := r.handle(a, m); err != nil {
			t.Fatalf("snapshot %s: %v", m.Type, err)
		}
	}
	if store.saves != 1 || !bytes.Equal(store.v.Data, content) {
		t.Fatal("snapshot not saved byte-for-byte")
	}
	manifest := drain(b)
	if !containsType(manifest, "snapshot_manifest") {
		t.Fatalf("no manifest: %+v", manifest)
	}
	if err := r.handle(b, clientMessage{Type: "sync_finish", SnapshotID: 1, Cursor: 0}); err == nil {
		t.Fatal("joiner skipped snapshot download")
	}
	if err := r.handle(b, clientMessage{Type: "snapshot_get", SnapshotID: 1, Index: 0}); err != nil {
		t.Fatal(err)
	}
	if !containsType(drain(b), "snapshot_part") {
		t.Fatal("missing snapshot chunk")
	}
	if err := r.handle(b, clientMessage{Type: "sync_poll", SnapshotID: 1, Cursor: 0}); err != nil {
		t.Fatal(err)
	}
	messages := drain(b)
	found := false
	cursor := uint64(0)
	for _, m := range messages {
		if m.Type == "sync_batch" {
			found = true
			cursor = m.Cursor
			if len(m.Entries) != 1 || m.Entries[0].Command.Tick != 101 {
				t.Fatalf("wrong replay: %+v", m)
			}
		}
	}
	if !found || cursor != 1 {
		t.Fatalf("replay missing; cursor %d", cursor)
	}
	if err := r.handle(b, clientMessage{Type: "sync_finish", SnapshotID: 1, Cursor: cursor}); err != nil {
		t.Fatal(err)
	}
	if b.syncing {
		t.Fatal("client never became ready")
	}
	r.remove(a)
	r.remove(b)
	// After a full room shutdown, the first joiner must receive saved state.
	c := peerFor(t, r, "brawl", "global")
	if !c.syncing || !containsType(drain(c), "snapshot_manifest") {
		t.Fatal("restored first joiner bypassed snapshot")
	}
	r.remove(c)
}
func TestTemporaryDiskStoreEnvelope(t *testing.T) {
	dir := t.TempDir()
	store := newTempStore(dir)
	v := &worldSnapshot{Tick: 999, Data: []byte{0, 1, 255, 3}}
	if err := store.Save(v); err != nil {
		t.Fatal(err)
	}
	restored, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if restored.Tick != v.Tick || !bytes.Equal(restored.Data, v.Data) {
		t.Fatal("round-trip failed")
	}
	path := filepath.Join(dir, "brawl-global.syws")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 1
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(); err == nil {
		t.Fatal("corrupt disk snapshot accepted")
	}
}
func TestHashMismatchRecheckThenPause(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := peerFor(t, r, "brawl", "testhash")
	b := peerFor(t, r, "brawl", "testhash")
	drain(a)
	drain(b)
	hashA := strings.Repeat("a", 64)
	hashB := strings.Repeat("b", 64)
	for _, v := range []struct {
		p    *peer
		hash string
	}{{a, hashA}, {b, hashB}} {
		if err := r.handle(v.p, clientMessage{Type: "hash", Tick: 120, Hash: v.hash}); err != nil {
			t.Fatal(err)
		}
	}
	if !containsType(drain(a), "hash_recheck_requested") {
		t.Fatal("missing hash recheck")
	}
	if a.room.state.paused {
		t.Fatal("first disagreement should not pause")
	}
	for _, v := range []struct {
		p    *peer
		hash string
	}{{a, hashA}, {b, hashB}} {
		if err := r.handle(v.p, clientMessage{Type: "hash_recheck", Tick: 120, Hash: v.hash}); err != nil {
			t.Fatal(err)
		}
	}
	if !a.room.state.paused {
		t.Fatal("repeated mismatch did not pause")
	}
	r.remove(a)
	r.remove(b)
}
func TestBatchCommandsValidAndAtomic(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	a := peerFor(t, r, "legacy", "global")
	b := peerFor(t, r, "legacy", "global")
	drain(a)
	drain(b)
	batch := clientMessage{Type: "command_batch", Commands: []batchedCommand{{Tick: 1, Sequence: 1, Payload: json.RawMessage(`{"k":1}`)}, {Tick: 2, Sequence: 1, Payload: json.RawMessage(`{"k":2}`)}}}
	if err := r.handle(a, batch); err != nil {
		t.Fatal(err)
	}
	if !containsType(drain(b), "command_batch") {
		t.Fatal("command batch not forwarded")
	}
	batch.Commands[1].Tick = 1
	batch.Commands[1].Sequence = 0
	if err := r.handle(a, batch); err == nil {
		t.Fatal("non-monotonic batch accepted")
	}
	r.remove(a)
	r.remove(b)
}
