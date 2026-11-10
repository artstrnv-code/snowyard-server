package main

// World state is opaque binary. The Java client is responsible for its exact
// schema, simulation and deterministic validation. The relay only bounds,
// authenticates transport roles, checks SHA-256 and stores confirmed bytes.
import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	snapshotChunkBytes = 8192
	maxSnapshotBytes   = 2 * 1024 * 1024
	journalLimit       = 32768
	journalByteLimit   = 8 * 1024 * 1024
	maxReplayBatch     = 64
	maxReplayBurst     = 8
)

type worldSnapshot struct {
	Tick    uint64
	Digest  string
	Data    []byte
	Version uint64
}
type snapshotUpload struct {
	Owner     uint64
	Tick      uint64
	Size      int
	Digest    string
	Data      []byte
	Next      int
	StartedAt time.Time
}
type journalEntry struct {
	Event   uint64        `json:"event"`
	Command serverMessage `json:"command"`
}

// SnapshotStore will later be implemented by the Google Apps Script adapter.
// The default temporary filesystem store is NOT durable on Render Free.
type SnapshotStore interface {
	Load() (*worldSnapshot, error)
	Save(*worldSnapshot) error
}
type tempFileStore struct{ dir string }

func newTempStore(dir string) SnapshotStore { return &tempFileStore{dir: dir} }
func (s *tempFileStore) filename() string   { return filepath.Join(s.dir, "brawl-global.syws") }
func (s *tempFileStore) Load() (*worldSnapshot, error) {
	b, err := os.ReadFile(s.filename())
	if err != nil {
		return nil, err
	}
	// Binary envelope: ASCII SYWS, version byte 1, tick 8-byte BE,
	// 32-byte SHA-256, then opaque payload.
	if len(b) < 45 || string(b[:4]) != "SYWS" || b[4] != 1 {
		return nil, errors.New("invalid snapshot envelope")
	}
	tick := readUint64(b[5:13])
	payload := b[45:]
	if len(payload) > maxSnapshotBytes {
		return nil, errors.New("snapshot oversized")
	}
	digest := sha256.Sum256(payload)
	if !equal32(digest[:], b[13:45]) {
		return nil, errors.New("snapshot digest mismatch")
	}
	return &worldSnapshot{Tick: tick, Digest: hex.EncodeToString(digest[:]), Data: payload, Version: 1}, nil
}
func (s *tempFileStore) Save(v *worldSnapshot) error {
	if err := os.MkdirAll(s.dir, 0700); err != nil {
		return err
	}
	sum := sha256.Sum256(v.Data)
	buf := make([]byte, 45+len(v.Data))
	copy(buf, "SYWS")
	buf[4] = 1
	writeUint64(buf[5:13], v.Tick)
	copy(buf[13:45], sum[:])
	copy(buf[45:], v.Data)
	tmp := s.filename() + ".tmp"
	if err := os.WriteFile(tmp, buf, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, s.filename())
}
func readUint64(b []byte) uint64 {
	var n uint64
	for _, v := range b {
		n = n<<8 | uint64(v)
	}
	return n
}
func writeUint64(b []byte, v uint64) {
	for i := 7; i >= 0; i-- {
		b[i] = byte(v)
		v >>= 8
	}
}
func equal32(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := range a {
		v |= a[i] ^ b[i]
	}
	return v == 0
}

func (room *relayRoom) sendToLocked(id uint64, msg serverMessage) {
	p := room.peers[id]
	if p == nil {
		return
	}
	wire, err := json.Marshal(msg)
	if err != nil {
		return
	}
	select {
	case p.send <- wire:
	default:
		go p.stop()
	}
}
func (room *relayRoom) sendManifestLocked(p *peer) {
	s := room.state
	if s == nil || !room.persistent() {
		return
	}
	if s.snapshot == nil {
		room.sendToLocked(p.id, serverMessage{Type: "sync_wait", Message: "awaiting first confirmed world snapshot"})
		if s.coordinator != 0 && s.coordinator != p.id {
			room.sendToLocked(s.coordinator, serverMessage{Type: "snapshot_requested"})
		}
		return
	}
	// A bounded journal may no longer cover a five-minute-old checkpoint.
	// Never offer a snapshot that cannot be replayed to the live tail.
	if room.rosterProtocol == rosterProtocolV3 && len(s.journal) > 0 && s.snapshotCursor+1 < s.journal[0].Event {
		room.sendToLocked(p.id, serverMessage{Type: "sync_wait", Message: "refreshing old checkpoint: journal history expired"})
		room.requestRefreshV3Locked()
		return
	}
	v := s.snapshot
	chunks := (len(v.Data) + snapshotChunkBytes - 1) / snapshotChunkBytes
	p.syncID = v.Version
	p.replayCursor = s.snapshotCursor
	p.chunksSeen = make([]bool, chunks)
	room.sendToLocked(p.id, serverMessage{Type: "snapshot_manifest", Tick: v.Tick, Size: len(v.Data), Digest: v.Digest, Version: 1, SnapshotID: v.Version, Chunks: chunks, Cursor: s.snapshotCursor})
}
func (room *relayRoom) appendJournalLocked(msg serverMessage) {
	s := room.state
	if s == nil || !room.persistent() {
		return
	}
	s.nextEvent++
	s.journal = append(s.journal, journalEntry{Event: s.nextEvent, Command: msg})
	s.journalBytes += journalCost(msg)
	for len(s.journal) > journalLimit || s.journalBytes > journalByteLimit {
		if len(s.journal) == 0 {
			break
		}
		s.journalBytes -= journalCost(s.journal[0].Command)
		s.journal[0] = journalEntry{}
		s.journal = s.journal[1:]
	}

}

func journalCost(m serverMessage) int {
	// Membership events contain the roster, not a single command payload.
	return len(m.Payload) + 96 + len(m.Players)*88
}

func (r *relay) handleSnapshotLocked(room *relayRoom, p *peer, msg clientMessage) error {
	if !room.persistent() || room.state == nil {
		return errors.New("snapshots only available in global brawl")
	}
	s := room.state
	// A newer checkpoint can supersede a joiner's download/replay while
	// old WebSocket requests are already in flight. These requests belong
	// to the retired snapshot, not to the new transfer: ignore them rather
	// than treating a normal refresh as a fatal protocol violation.
	if p.syncing && s.snapshot != nil && p.syncID == s.snapshot.Version &&
		msg.SnapshotID > 0 && msg.SnapshotID < p.syncID {
		switch msg.Type {
		case "snapshot_get", "sync_poll", "sync_finish":
			return nil
		}
	}
	switch msg.Type {
	case "snapshot_begin":
		if p.id != s.coordinator {
			return errors.New("snapshot requires coordinator")
		}
		if s.uploading != nil && time.Since(s.uploading.StartedAt) > 2*time.Minute {
			s.uploading = nil
		}
		if s.uploading != nil {
			return errors.New("snapshot upload already in progress")
		}
		if msg.Size <= 0 || msg.Size > maxSnapshotBytes || !isDigest(msg.Digest) ||
			(msg.Tick == 0 && !(room.rosterProtocol == rosterProtocolV2 && room.barrier != nil && room.barrier.awaitingSnapshot && room.barrier.reference.tick == 0)) {
			return errors.New("invalid snapshot metadata")
		}
		// During admission, the snapshot must represent the exact voted
		// frozen world, never an arbitrary periodic/old checkpoint.
		if b := room.barrier; room.rosterProtocol == rosterProtocolV2 && b != nil && b.awaitingSnapshot &&
			(p.id != s.coordinator || msg.Tick != b.reference.tick) {
			return errors.New("roster snapshot must match the active frozen vote")
		}
		if s.snapshot != nil && msg.Tick < s.snapshot.Tick {
			return errors.New("stale snapshot")
		}
		s.uploading = &snapshotUpload{Owner: p.id, Tick: msg.Tick, Size: msg.Size, Digest: strings.ToLower(msg.Digest), Data: make([]byte, 0, msg.Size), StartedAt: time.Now()}
	case "snapshot_chunk":
		up := s.uploading
		if up == nil || up.Owner != p.id || msg.Index != up.Next || time.Since(up.StartedAt) > 2*time.Minute {
			return errors.New("unexpected snapshot chunk")
		}
		part, err := base64.StdEncoding.DecodeString(msg.Data)
		if err != nil || len(part) == 0 || len(part) > snapshotChunkBytes || len(up.Data)+len(part) > up.Size {
			return errors.New("invalid snapshot chunk")
		}
		up.Data = append(up.Data, part...)
		up.Next++
	case "snapshot_commit":
		up := s.uploading
		if up == nil || up.Owner != p.id || len(up.Data) != up.Size {
			return errors.New("incomplete snapshot")
		}
		sum := sha256.Sum256(up.Data)
		if hex.EncodeToString(sum[:]) != up.Digest {
			s.uploading = nil
			return errors.New("snapshot checksum mismatch")
		}
		v := &worldSnapshot{Tick: up.Tick, Digest: up.Digest, Data: append([]byte(nil), up.Data...)}
		if s.snapshot != nil {
			v.Version = s.snapshot.Version + 1
		} else {
			v.Version = 1
		}
		// Persist before publishing. If disk is unavailable, keep a memory
		// copy: this is an explicitly acknowledged data-loss risk.
		persistErr := r.store.Save(v)
		s.snapshot = v
		s.snapshotCursor = s.nextEvent
		if p.leaving {
			p.leaving = false
			room.sendToLocked(p.id, serverMessage{Type: "leave_ready"})
		}
		s.uploading = nil
		// Journal is only safe relative to the new checkpoint. Drop entries
		// through its tick, retaining later changes for catch-up.
		kept := s.journal[:0]
		// The new roster begins at checkpoint tick+1. Commands submitted by
		// the OLD generation beyond the freeze boundary must not be replayed
		// into a joiner's fresh checkpoint (or leak into the new generation).
		freshRoster := room.rosterProtocol == rosterProtocolV2 && room.barrier != nil &&
			room.barrier.phase == rosterJoinVerify && room.barrier.awaitingSnapshot
		if !freshRoster {
			for _, entry := range s.journal {
				if entry.Command.Tick > v.Tick {
					kept = append(kept, entry)
				}
			}
		}
		for i := len(kept); i < len(s.journal); i++ {
			s.journal[i] = journalEntry{}
		}
		s.journal = kept
		s.journalBytes = 0
		for _, entry := range kept {
			s.journalBytes += journalCost(entry.Command)
		}
		if room.rosterProtocol == rosterProtocolV3 {
			// Rebase post-checkpoint commands onto fresh journal IDs. A snapshot
			// may be uploaded while inputs for future ticks are already queued.
			// The old cursor cannot represent that snapshot boundary reliably.
			for i := range s.journal {
				s.nextEvent++
				s.journal[i].Event = s.nextEvent
			}
		} else if len(kept) > 0 {
			s.snapshotCursor = kept[0].Event - 1
		}
		room.broadcastLocked(serverMessage{Type: "snapshot_saved", Tick: v.Tick, Digest: v.Digest, SnapshotID: v.Version, Message: storageStatus(persistErr)}, 0)
		if s.backup != 0 {
			room.sendToLocked(s.backup, serverMessage{Type: "backup_snapshot_available", SnapshotID: v.Version, Digest: v.Digest, Tick: v.Tick})
		}
		// An admitted joiner must download this newly captured state, not
		// merely confirm the older checkpoint used for preliminary catch-up.
		room.rosterSnapshotSavedV2Locked(p.id, v.Tick, v.Version)
		for _, other := range room.peers {
			if other.syncing && (room.barrier == nil ||
				len(room.barrier.joining) == 0 || other.id != room.barrier.joining[0]) {
				room.sendManifestLocked(other)
			}
		}
	case "snapshot_info":
		if s.snapshot == nil {
			return errors.New("no snapshot available")
		}
		p.backupDownload = true
		room.sendManifestLocked(p)
	case "snapshot_get":
		v := s.snapshot
		if v == nil {
			return errors.New("no snapshot available")
		}
		if msg.SnapshotID != v.Version || (!p.syncing && !p.backupDownload) || p.syncID != v.Version {
			return errors.New("snapshot changed or client not loading")
		}
		chunks := (len(v.Data) + snapshotChunkBytes - 1) / snapshotChunkBytes
		if msg.Index < 0 || msg.Index >= chunks {
			return errors.New("snapshot chunk index out of range")
		}
		from := msg.Index * snapshotChunkBytes
		to := from + snapshotChunkBytes
		if to > len(v.Data) {
			to = len(v.Data)
		}
		p.chunksSeen[msg.Index] = true
		room.sendToLocked(p.id, serverMessage{Type: "snapshot_part", SnapshotID: v.Version, Index: msg.Index, Data: base64.StdEncoding.EncodeToString(v.Data[from:to])})
	case "sync_poll":
		if !p.syncing {
			return errors.New("client already synchronized")
		}
		if s.snapshot == nil {
			return errors.New("no snapshot to replay")
		}
		if msg.SnapshotID != s.snapshot.Version || p.syncID != s.snapshot.Version {
			return errors.New("snapshot changed; restart sync")
		}
		for _, seen := range p.chunksSeen {
			if !seen {
				return errors.New("snapshot chunks not fully transferred")
			}
		}
		if msg.Cursor != p.replayCursor {
			return errors.New("invalid replay cursor")
		}
		if len(s.journal) > 0 && p.replayCursor+1 < s.journal[0].Event {
			if room.rosterProtocol == rosterProtocolV3 {
				room.sendToLocked(p.id, serverMessage{Type: "sync_wait", Message: "journal expired during transfer; requesting fresh snapshot"})
				room.requestRefreshV3Locked()
				return nil
			}
			return errors.New("command journal expired; request new snapshot")
		}
		for burst := 0; burst < maxReplayBurst; burst++ {
			// Journal entries are strictly event-sorted; repeated full scans
			// are wasteful when several peers catch up simultaneously.
			first := sort.Search(len(s.journal), func(i int) bool {
				return s.journal[i].Event > p.replayCursor
			})
			batch := make([]journalEntry, 0, maxReplayBatch)
			end := first
			budget := 0
			for end < len(s.journal) && len(batch) < maxReplayBatch {
				entry := s.journal[end]
				// v3 must fit the Java WebSocket's 64 KiB frame guard.
				// v1/v2 keep their exact legacy batch boundaries.
				if room.rosterProtocol == rosterProtocolV3 {
					cost := journalCost(entry.Command) + 160
					if len(batch) > 0 && budget+cost > 40000 {
						break
					}
					budget += cost
				}
				batch = append(batch, entry)
				end++
			}
			// v1/v2 retain their old brief freeze for compatibility only.
			if room.rosterProtocol != rosterProtocolV3 && room.rosterEnabled && len(batch) < maxReplayBatch && room.catchupOwner == 0 {
				room.catchupOwner = p.id
				room.catchupWasPaused = s.paused
				s.paused = true
				room.broadcastLocked(serverMessage{Type: "session_paused", Paused: true,
					Message: "Briefly freezing Brawl to complete a player's world replay"}, 0)
			}
			if len(batch) > 0 {
				p.replayCursor = batch[len(batch)-1].Event
			} else {
				p.replayCursor = s.nextEvent
			}
			more := end < len(s.journal)
			room.sendToLocked(p.id, serverMessage{Type: "sync_batch", SnapshotID: s.snapshot.Version,
				Entries: batch, Cursor: p.replayCursor, More: more})
			if !more {
				break
			}
		}

	case "sync_finish":
		if !p.syncing {
			return nil
		}
		if s.snapshot == nil || msg.SnapshotID != s.snapshot.Version {
			return errors.New("snapshot changed")
		}
		if room.rosterProtocol == rosterProtocolV3 {
			return room.finishLiveReplayV3Locked(p, msg)
		}
		if msg.Cursor != s.nextEvent || p.replayCursor != s.nextEvent {
			return errors.New("replay not caught up")
		}
		for _, seen := range p.chunksSeen {
			if !seen {
				return errors.New("snapshot chunks missing")
			}
		}
		p.syncing = false
		if room.catchupOwner == p.id {
			// The upcoming membership barrier takes ownership of the pause;
			// preserve any earlier coordinator-recovery pause across it.
			s.paused = room.catchupWasPaused
			room.catchupOwner = 0
		}
		if room.rosterEnabled {
			room.sendToLocked(p.id, serverMessage{Type: "sync_loaded", SnapshotID: s.snapshot.Version, Cursor: s.nextEvent,
				Message: "world loaded; wait for a roster commit before simulating"})
			if b := room.barrier; room.rosterProtocol == rosterProtocolV2 && b != nil &&
				b.phase == rosterJoinVerify && !b.awaitingSnapshot && b.joinSnapshotID == s.snapshot.Version &&
				len(b.joining) == 1 && b.joining[0] == p.id {
				room.sendToLocked(p.id, serverMessage{Type: "roster_checkpoint",
					Generation: b.generation, Tick: b.reference.tick, Hash: b.reference.hash,
					Paused: true, Message: "fresh frozen checkpoint loaded; verify and join"})
			} else {
				room.beginRosterLocked()
			}
		} else {
			room.sendToLocked(p.id, serverMessage{Type: "sync_ready", SnapshotID: s.snapshot.Version, Cursor: s.nextEvent})
			room.broadcastLocked(serverMessage{Type: "player_ready", PlayerID: p.id}, p.id)
		}
	default:
		return fmt.Errorf("unsupported snapshot message %s", msg.Type)
	}
	return nil
}
func isDigest(s string) bool { return len(s) == 64 && isHex(s) }
func storageStatus(err error) string {
	if err != nil {
		return "memory_only: temporary disk save failed"
	}
	return "snapshot cached locally; cloud upload queued when configured"
}

// Throttle refresh requests to prevent dozens of simultaneous loading peers
// from asking the coordinator to serialize the same world every frame.
func (room *relayRoom) requestRefreshV3Locked() {
	if room.state == nil || room.rosterProtocol != rosterProtocolV3 || room.state.coordinator == 0 || room.state.uploading != nil {
		return
	}
	if time.Since(room.state.lastRefreshRequest) < 5*time.Second {
		return
	}
	room.state.lastRefreshRequest = time.Now()
	room.sendToLocked(room.state.coordinator, serverMessage{Type: "snapshot_requested", Message: "refresh checkpoint to cover expired live replay journal"})
}
