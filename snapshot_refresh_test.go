package main

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// A new checkpoint invalidates all queued requests for the previous version.
// This is common with many concurrent joiners and must not abort the new sync.
func TestV3OldSnapshotRequestsAfterRefresh(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	owner := v3Peer(t, r)
	room := owner.room
	drain(owner)
	data := []byte("snapshot version one")
	sum := sha256.Sum256(data)
	room.mu.Lock()
	room.state.snapshot = &worldSnapshot{Tick: 1, Version: 1, Digest: hex.EncodeToString(sum[:]), Data: data}
	room.state.snapshotCursor = room.state.nextEvent
	room.mu.Unlock()

	joiner := v3Peer(t, r)
	drain(joiner)
	if !joiner.syncing || joiner.syncID != 1 {
		t.Fatal("joiner did not start on checkpoint 1")
	}

	// Emulate the coordinator publishing a newer checkpoint while the old
	// snapshot_get, sync_poll and sync_finish requests are still in flight.
	fresh := []byte("snapshot version two")
	freshSum := sha256.Sum256(fresh)
	room.mu.Lock()
	room.state.snapshot = &worldSnapshot{Tick: 2, Version: 2, Digest: hex.EncodeToString(freshSum[:]), Data: fresh}
	room.state.snapshotCursor = room.state.nextEvent
	room.sendManifestLocked(joiner)
	room.mu.Unlock()
	if joiner.syncID != 2 {
		t.Fatal("refresh did not replace checkpoint")
	}

	for _, old := range []clientMessage{
		{Type: "snapshot_get", SnapshotID: 1, Index: 0},
		{Type: "sync_poll", SnapshotID: 1, Cursor: 0},
		{Type: "sync_finish", SnapshotID: 1, Cursor: 0},
	} {
		if err := r.handle(joiner, old); err != nil {
			t.Fatalf("old %s must not abort fresh synchronization: %v", old.Type, err)
		}
	}
	if !joiner.syncing || joiner.syncID != 2 {
		t.Fatal("stale request changed active checkpoint")
	}
	if err := r.handle(joiner, clientMessage{Type: "snapshot_get", SnapshotID: 2, Index: 0}); err != nil {
		t.Fatalf("fresh checkpoint transfer failed: %v", err)
	}
	r.remove(joiner)
	r.remove(owner)
}

// Eight simultaneously connected peers may have old download requests
// in flight when a replacement checkpoint is published.
func TestV3EightPeersSurviveCheckpointReplacement(t *testing.T) {
	r := newRelayWithStore(&mockStore{})
	peers := make([]*peer, 0, 8)
	owner := v3Peer(t, r)
	peers = append(peers, owner)
	room := owner.room
	drain(owner)
	oldData := []byte("old")
	oldHash := sha256.Sum256(oldData)
	room.mu.Lock()
	room.state.snapshot = &worldSnapshot{Tick: 1, Version: 1, Digest: hex.EncodeToString(oldHash[:]), Data: oldData}
	room.state.snapshotCursor = room.state.nextEvent
	room.mu.Unlock()
	for i := 0; i < 7; i++ {
		p := v3Peer(t, r)
		if p.syncID != 1 || !p.syncing {
			t.Fatalf("peer %d did not start checkpoint 1", i+2)
		}
		peers = append(peers, p)
		drain(p)
	}
	newData := []byte("new")
	newHash := sha256.Sum256(newData)
	room.mu.Lock()
	room.state.snapshot = &worldSnapshot{Tick: 2, Version: 2, Digest: hex.EncodeToString(newHash[:]), Data: newData}
	room.state.snapshotCursor = room.state.nextEvent
	for _, p := range peers[1:] {
		room.sendManifestLocked(p)
	}
	room.mu.Unlock()
	for _, p := range peers[1:] {
		if err := r.handle(p, clientMessage{Type: "sync_poll", SnapshotID: 1}); err != nil {
			t.Fatalf("peer %d stale replay aborted: %v", p.id, err)
		}
		if err := r.handle(p, clientMessage{Type: "snapshot_get", SnapshotID: 2, Index: 0}); err != nil {
			t.Fatalf("peer %d current download failed: %v", p.id, err)
		}
	}
	for i := len(peers) - 1; i >= 0; i-- {
		r.remove(peers[i])
	}
}
