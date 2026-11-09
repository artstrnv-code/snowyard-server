package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

// This fake implements the SAME wire protocol as Code.gs, including batching.
// The real Google deployment remains an external integration test.
type fakeMacro struct {
	mu       sync.Mutex
	secret   string
	snapshot *worldSnapshot
	stage    *worldSnapshot
	uploadID string
	next     int
	failure  bool
}

func (m *fakeMacro) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Token      string   `json:"token"`
		Action     string   `json:"action"`
		Tick       uint64   `json:"tick"`
		Bytes      int      `json:"bytes"`
		SHA256     string   `json:"sha256"`
		UploadID   string   `json:"uploadId"`
		SnapshotID string   `json:"snapshotId"`
		Index      int      `json:"index"`
		Count      int      `json:"count"`
		Chunks     []string `json:"chunks"`
	}
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		http.Error(w, "bad json", 400)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	reply := map[string]any{"ok": true}
	if q.Token != m.secret {
		reply = map[string]any{"ok": false, "error": "Unauthorized."}
	} else if m.failure {
		http.Error(w, "unavailable", 503)
		return
	} else {
		switch q.Action {
		case "latest":
			if m.snapshot == nil {
				reply["exists"] = false
			} else {
				reply["exists"] = true
				reply["snapshotId"] = "committed"
				reply["tick"] = m.snapshot.Tick
				reply["bytes"] = len(m.snapshot.Data)
				reply["sha256"] = m.snapshot.Digest
				reply["chunks"] = (len(m.snapshot.Data) + 8191) / 8192
			}
		case "read":
			if m.snapshot == nil || q.SnapshotID != "committed" {
				reply = map[string]any{"ok": false, "error": "not found"}
				break
			}
			data := []string{}
			for i := q.Index; i < q.Index+q.Count && i*8192 < len(m.snapshot.Data); i++ {
				from := i * 8192
				to := from + 8192
				if to > len(m.snapshot.Data) {
					to = len(m.snapshot.Data)
				}
				data = append(data, base64.StdEncoding.EncodeToString(m.snapshot.Data[from:to]))
			}
			reply["snapshotId"] = "committed"
			reply["index"] = q.Index
			reply["nextIndex"] = q.Index + len(data)
			reply["chunks"] = data
		case "begin":
			if m.snapshot != nil && q.Tick < m.snapshot.Tick {
				reply = map[string]any{"ok": false, "error": "stale"}
				break
			}
			if m.stage == nil {
				m.stage = &worldSnapshot{Tick: q.Tick, Digest: q.SHA256}
				m.uploadID = "stage-id"
				m.next = 0
			}
			reply["uploadId"] = m.uploadID
			reply["nextIndex"] = m.next
			reply["chunks"] = (q.Bytes + 8191) / 8192
		case "put":
			if m.stage == nil || q.UploadID != m.uploadID || q.Index != m.next {
				reply = map[string]any{"ok": false, "error": "bad stage"}
				break
			}
			for _, s := range q.Chunks {
				b, err := base64.StdEncoding.DecodeString(s)
				if err != nil {
					panic(err)
				}
				m.stage.Data = append(m.stage.Data, b...)
			}
			m.next += len(q.Chunks)
			reply["nextIndex"] = m.next
		case "commit":
			if m.stage == nil || m.uploadID != q.UploadID {
				reply = map[string]any{"ok": false, "error": "bad commit"}
				break
			}
			sum := sha256.Sum256(m.stage.Data)
			if hex.EncodeToString(sum[:]) != m.stage.Digest {
				reply = map[string]any{"ok": false, "error": "bad hash"}
				break
			}
			m.snapshot = copySnapshot(m.stage)
			m.stage = nil
			reply["exists"] = true
			reply["snapshotId"] = "committed"
			reply["tick"] = m.snapshot.Tick
			reply["bytes"] = len(m.snapshot.Data)
			reply["sha256"] = m.snapshot.Digest
		default:
			reply = map[string]any{"ok": false, "error": fmt.Sprintf("unknown action %s", q.Action)}
		}
	}
	_ = json.NewEncoder(w).Encode(reply)
}

func testSnapshot(tick uint64, size int) *worldSnapshot {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*47 + 3) % 256)
	}
	hash := sha256.Sum256(data)
	return &worldSnapshot{Tick: tick, Digest: hex.EncodeToString(hash[:]), Data: data, Version: 1}
}
func mockAPI(t *testing.T, mock *fakeMacro) (*sheetsAPI, func()) {
	t.Helper()
	server := httptest.NewTLSServer(mock)
	api := &sheetsAPI{endpoint: server.URL + "/exec", token: mock.secret, client: server.Client()}
	api.client.Timeout = 3 * time.Second
	return api, server.Close
}
func TestSheetsSaveLoadBatchAndChecksums(t *testing.T) {
	for _, size := range []int{1, 8192, 8193, 131075, 2 * 1024 * 1024} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			m := &fakeMacro{secret: "01234567890123456789012345678901"}
			api, done := mockAPI(t, m)
			defer done()
			original := testSnapshot(120, size)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := api.save(ctx, original); err != nil {
				t.Fatal(err)
			}
			loaded, err := api.load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if loaded.Tick != 120 || loaded.Digest != original.Digest || len(loaded.Data) != size {
				t.Fatal("snapshot not restored")
			}
		})
	}
}
func TestSheetsTamperingRejected(t *testing.T) {
	m := &fakeMacro{secret: "01234567890123456789012345678901", snapshot: testSnapshot(7, 16000)}
	api, done := mockAPI(t, m)
	defer done()
	m.snapshot.Data[4] ^= 0xff
	if _, err := api.load(context.Background()); err == nil {
		t.Fatal("accepted corrupt remote data")
	}
}
func TestHybridLocalFallbackAndRecovery(t *testing.T) {
	m := &fakeMacro{secret: "01234567890123456789012345678901"}
	api, done := mockAPI(t, m)
	defer done()
	local := newTempStore(t.TempDir())
	s := newHybridStore(local, api)
	a := testSnapshot(100, 19000)
	m.mu.Lock()
	m.failure = true
	m.mu.Unlock()
	if err := s.Save(a); err != nil {
		t.Fatal(err)
	}
	loaded, err := s.Load()
	if err != nil || loaded.Tick != 100 {
		t.Fatalf("local fallback failed: %v", err)
	}
	m.mu.Lock()
	m.failure = false
	m.mu.Unlock()
	s.enqueue(a)
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		saved := m.snapshot
		m.mu.Unlock()
		if saved != nil && saved.Tick == 100 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("cloud did not recover")
}
func TestHybridWillNotBootstrapEmptyWhenCloudUnreachable(t *testing.T) {
	m := &fakeMacro{secret: "01234567890123456789012345678901", failure: true}
	api, done := mockAPI(t, m)
	defer done()
	s := newHybridStore(newTempStore(t.TempDir()), api)
	_, err := s.Load()
	if err == nil || os.IsNotExist(err) {
		t.Fatalf("silently bootstrapped empty world: %v", err)
	}
}
func TestHybridPrefersNewerCloud(t *testing.T) {
	m := &fakeMacro{secret: "01234567890123456789012345678901", snapshot: testSnapshot(200, 19000)}
	api, done := mockAPI(t, m)
	defer done()
	local := newTempStore(t.TempDir())
	if err := local.Save(testSnapshot(100, 15000)); err != nil {
		t.Fatal(err)
	}
	s := newHybridStore(local, api)
	snap, err := s.Load()
	if err != nil || snap.Tick != 200 {
		t.Fatalf("wrong snapshot: %v", err)
	}
}
