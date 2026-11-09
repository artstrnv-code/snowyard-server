package main

// Google Apps Script snapshot storage. Game simulation never runs on this server.
// Store writes always land on ephemeral disk first; a single background worker
// serializes durable uploads, so slow Apps Script requests never block room locks.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const sheetsBatchChunks = 16

type sheetsAPI struct {
	endpoint string
	token    string
	client   *http.Client
}

// Payload for calls that supply chunks: the "chunks" field must be an array,
// while in begin responses it is a numeric count.
type sheetsUploadReply struct {
	OK        bool   `json:"ok"`
	Error     string `json:"error"`
	UploadID  string `json:"uploadId"`
	NextIndex int    `json:"nextIndex"`
	Chunks    int    `json:"chunks"`
}

type sheetsManifest struct {
	OK         bool   `json:"ok"`
	Error      string `json:"error"`
	Exists     bool   `json:"exists"`
	SnapshotID string `json:"snapshotId"`
	Tick       uint64 `json:"tick"`
	SHA256     string `json:"sha256"`
	Bytes      int    `json:"bytes"`
	Chunks     int    `json:"chunks"`
}

type sheetsChunksReply struct {
	OK         bool     `json:"ok"`
	Error      string   `json:"error"`
	SnapshotID string   `json:"snapshotId"`
	Index      int      `json:"index"`
	Chunks     []string `json:"chunks"`
	NextIndex  int      `json:"nextIndex"`
}

func newSheetsAPI(endpoint, token string) (*sheetsAPI, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || !strings.HasSuffix(u.Path, "/exec") || u.RawQuery != "" || u.User != nil {
		return nil, errors.New("SNOWYARD_SHEETS_URL must be an HTTPS Apps Script /exec URL")
	}
	if len(token) < 32 {
		return nil, errors.New("SNOWYARD_SHEETS_TOKEN must be at least 32 characters")
	}
	return &sheetsAPI{endpoint: endpoint, token: token, client: &http.Client{
		Timeout: 28 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) > 5 || req.URL.Scheme != "https" {
				return errors.New("unsafe redirect")
			}
			host := strings.ToLower(req.URL.Hostname())
			if host != "script.google.com" && host != "script.googleusercontent.com" {
				return errors.New("unexpected Apps Script redirect host")
			}
			// Apps Script returns a 302 for ContentService, which redirects as GET
			// without retaining the JSON body containing the token.
			if req.Method != http.MethodGet {
				return errors.New("redirect would forward credentials")
			}
			return nil
		},
	}}, nil
}

func (a *sheetsAPI) call(ctx context.Context, body map[string]any, result any) error {
	body["token"] = a.token
	wire, err := json.Marshal(body)
	if err != nil {
		return err
	}
	// Reuse byte payload on transient transport failures. Apps Script writes
	// are idempotent for begin/put/commit, with matching metadata.
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 600 * time.Millisecond):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, bytes.NewReader(wire))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := a.client.Do(req)
		if err != nil {
			last = fmt.Errorf("Apps Script request failed: %w", err)
			continue
		}
		raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 350000))
		_ = resp.Body.Close()
		if readErr != nil {
			last = readErr
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			last = fmt.Errorf("Apps Script HTTP %d", resp.StatusCode)
			if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 429 {
				return last
			}
			continue
		}
		var status struct {
			OK    bool   `json:"ok"`
			Error string `json:"error"`
		}
		if err := json.Unmarshal(raw, &status); err != nil {
			last = fmt.Errorf("Apps Script invalid JSON: %w", err)
			continue
		}
		if !status.OK {
			return fmt.Errorf("Apps Script rejected request: %s", status.Error)
		}
		if err := json.Unmarshal(raw, result); err != nil {
			return fmt.Errorf("Apps Script result malformed: %w", err)
		}
		return nil
	}
	return last
}

func (a *sheetsAPI) load(ctx context.Context) (*worldSnapshot, error) {
	var m sheetsManifest
	if err := a.call(ctx, map[string]any{"action": "latest"}, &m); err != nil {
		return nil, err
	}
	if !m.Exists {
		return nil, os.ErrNotExist
	}
	if m.Tick == 0 || m.Bytes < 1 || m.Bytes > maxSnapshotBytes || m.Chunks != (m.Bytes+snapshotChunkBytes-1)/snapshotChunkBytes || !isDigest(m.SHA256) || m.SnapshotID == "" {
		return nil, errors.New("Apps Script returned invalid snapshot manifest")
	}
	buf := make([]byte, 0, m.Bytes)
	for i := 0; i < m.Chunks; {
		count := m.Chunks - i
		if count > sheetsBatchChunks {
			count = sheetsBatchChunks
		}
		var got sheetsChunksReply
		if err := a.call(ctx, map[string]any{"action": "read", "snapshotId": m.SnapshotID, "index": i, "count": count}, &got); err != nil {
			return nil, err
		}
		if got.SnapshotID != m.SnapshotID || got.Index != i || got.NextIndex != i+count || len(got.Chunks) != count {
			return nil, errors.New("Apps Script returned incomplete chunk batch")
		}
		for j, encoded := range got.Chunks {
			part, err := base64.StdEncoding.Strict().DecodeString(encoded)
			expected := snapshotChunkBytes
			if i+j == m.Chunks-1 {
				expected = m.Bytes - snapshotChunkBytes*(m.Chunks-1)
			}
			if err != nil || len(part) != expected {
				return nil, errors.New("Apps Script returned invalid snapshot chunk")
			}
			buf = append(buf, part...)
		}
		i += count
	}
	digest := sha256.Sum256(buf)
	if len(buf) != m.Bytes || hex.EncodeToString(digest[:]) != strings.ToLower(m.SHA256) {
		return nil, errors.New("Apps Script snapshot checksum mismatch")
	}
	return &worldSnapshot{Tick: m.Tick, Digest: hex.EncodeToString(digest[:]), Data: buf, Version: 1}, nil
}

func (a *sheetsAPI) save(ctx context.Context, v *worldSnapshot) error {
	if err := validateStoredSnapshot(v); err != nil {
		return err
	}
	sum := sha256.Sum256(v.Data)
	digest := hex.EncodeToString(sum[:])
	var begin sheetsUploadReply
	if err := a.call(ctx, map[string]any{"action": "begin", "tick": v.Tick, "bytes": len(v.Data), "sha256": digest}, &begin); err != nil {
		return err
	}
	count := (len(v.Data) + snapshotChunkBytes - 1) / snapshotChunkBytes
	if begin.UploadID == "" || begin.Chunks != count || begin.NextIndex < 0 || begin.NextIndex > count {
		return errors.New("Apps Script invalid upload begin response")
	}
	for i := begin.NextIndex; i < count; {
		batch := make([]string, 0, sheetsBatchChunks)
		for j := 0; j < sheetsBatchChunks && i+j < count; j++ {
			from := (i + j) * snapshotChunkBytes
			to := from + snapshotChunkBytes
			if to > len(v.Data) {
				to = len(v.Data)
			}
			batch = append(batch, base64.StdEncoding.EncodeToString(v.Data[from:to]))
		}
		var put sheetsUploadReply
		if err := a.call(ctx, map[string]any{"action": "put", "uploadId": begin.UploadID, "index": i, "chunks": batch}, &put); err != nil {
			return err
		}
		if put.NextIndex != i+len(batch) {
			return errors.New("Apps Script did not accept complete chunk batch")
		}
		i = put.NextIndex
	}
	var committed sheetsManifest
	if err := a.call(ctx, map[string]any{"action": "commit", "uploadId": begin.UploadID}, &committed); err != nil {
		return err
	}
	if !committed.Exists || committed.Tick != v.Tick || committed.Bytes != len(v.Data) || strings.ToLower(committed.SHA256) != digest {
		return errors.New("Apps Script commit metadata mismatch")
	}
	return nil
}

func validateStoredSnapshot(v *worldSnapshot) error {
	if v == nil || v.Tick == 0 || len(v.Data) == 0 || len(v.Data) > maxSnapshotBytes {
		return errors.New("invalid snapshot")
	}
	sum := sha256.Sum256(v.Data)
	if v.Digest != "" && !strings.EqualFold(v.Digest, hex.EncodeToString(sum[:])) {
		return errors.New("snapshot digest mismatch")
	}
	return nil
}

// hybridSnapshotStore prioritizes local writes and asynchronously uploads the
// newest checkpoint. Local files are an emergency cache, not durable on Render.
// At startup, an unreachable remote store with no local checkpoint is an
// error: silently bootstrapping an empty world would erase the real world.
type hybridSnapshotStore struct {
	local      SnapshotStore
	remote     *sheetsAPI
	mu         sync.Mutex
	pending    *worldSnapshot
	wake       chan struct{}
	retryDelay time.Duration
}

func newHybridStore(local SnapshotStore, remote *sheetsAPI) *hybridSnapshotStore {
	s := &hybridSnapshotStore{local: local, remote: remote, wake: make(chan struct{}, 1), retryDelay: 5 * time.Second}
	go s.worker()
	return s
}

func copySnapshot(v *worldSnapshot) *worldSnapshot {
	if v == nil {
		return nil
	}
	out := *v
	out.Data = append([]byte(nil), v.Data...)
	return &out
}
func (s *hybridSnapshotStore) enqueue(v *worldSnapshot) {
	s.mu.Lock()
	if s.pending == nil || v.Tick >= s.pending.Tick {
		s.pending = copySnapshot(v)
	}
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *hybridSnapshotStore) Save(v *worldSnapshot) error {
	if err := validateStoredSnapshot(v); err != nil {
		return err
	}
	err := s.local.Save(v)
	s.enqueue(v)
	return err
}
func (s *hybridSnapshotStore) Load() (*worldSnapshot, error) {
	local, localErr := s.local.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	remote, remoteErr := s.remote.load(ctx)
	cancel()
	if remoteErr == nil && (local == nil || remote.Tick > local.Tick) {
		if err := s.local.Save(remote); err != nil {
			log.Printf("[storage] local cache write failed: %v", err)
		}
		return remote, nil
	}
	if localErr == nil {
		if remoteErr != nil {
			log.Printf("[storage] remote unavailable; using local snapshot: %v", remoteErr)
		}
		if remoteErr == nil && remote.Tick == local.Tick && remote.Digest != local.Digest {
			return nil, errors.New("cloud and local snapshots disagree at the same tick")
		}
		if remoteErr != nil || local.Tick > remote.Tick {
			s.enqueue(local)
		}
		return local, nil
	}
	if remoteErr == nil {
		return remote, nil
	}
	if errors.Is(remoteErr, os.ErrNotExist) && errors.Is(localErr, os.ErrNotExist) {
		return nil, os.ErrNotExist
	}
	return nil, fmt.Errorf("no recoverable snapshot: local=%v; Sheets=%w", localErr, remoteErr)
}
func (s *hybridSnapshotStore) worker() {
	for range s.wake {
		for {
			s.mu.Lock()
			current := copySnapshot(s.pending)
			s.mu.Unlock()
			if current == nil {
				break
			}
			ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
			err := s.remote.save(ctx, current)
			cancel()
			if err == nil {
				log.Printf("[storage] cloud checkpoint committed (tick=%d, bytes=%d)", current.Tick, len(current.Data))
				s.mu.Lock()
				if s.pending != nil && s.pending.Tick == current.Tick && s.pending.Digest == current.Digest {
					s.pending = nil
				}
				s.mu.Unlock()
				continue
			}
			log.Printf("[storage] cloud checkpoint failed (tick=%d), retrying: %v", current.Tick, err)
			select {
			case <-time.After(s.retryDelay):
			case <-s.wake:
			}
		}
	}
}
