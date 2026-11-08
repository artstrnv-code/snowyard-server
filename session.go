package main

// Session control is separate from the transport. The relay never simulates
// physics, chooses bot actions, or interprets snow cell contents.
import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

const (
	royaleSlots = 10
	royaleWait  = 120 * time.Second
)

type sessionState struct {
	coordinator uint64
	backup      uint64
	paused      bool
	started     bool
	startedAt   time.Time
	timer       *time.Timer
	saveTimer   *time.Timer
	botSlots    int
	// Two phases: first disagreement requests a recheck, second pauses.
	hashVotes    map[uint64]map[uint64]string
	recheckVotes map[uint64]map[uint64]string
	// Persistence and catch-up apply ONLY to the official brawl/global world.
	snapshot       *worldSnapshot
	uploading      *snapshotUpload
	journal        []journalEntry
	journalBytes   int
	nextEvent      uint64
	snapshotCursor uint64
	botTicks       map[uint64]uint64
}

func (room *relayRoom) advanced() bool   { return room.mode == "brawl" || room.mode == "battle_royale" }
func (room *relayRoom) persistent() bool { return room.mode == "brawl" && room.name == "global" }
func (room *relayRoom) royale() bool     { return room.mode == "battle_royale" }

func (room *relayRoom) orderedPlayersLocked() []uint64 {
	ids := make([]uint64, 0, len(room.peers))
	for id := range room.peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (room *relayRoom) electLocked() {
	if room.state == nil {
		return
	}
	s := room.state
	ids := room.orderedPlayersLocked()
	if room.rosterEnabled {
		filtered := ids[:0]
		for _, id := range ids {
			p := room.peers[id]
			if p != nil && !p.syncing && (room.rosterApplied == 0 || room.rosterMembers[id]) {
				filtered = append(filtered, id)
			}
		}
		ids = filtered
	}
	present := func(id uint64) bool {
		_, ok := room.peers[id]
		return ok && (!room.rosterEnabled || (!room.peers[id].syncing && (room.rosterApplied == 0 || room.rosterMembers[id])))
	}
	prev, oldBackup := s.coordinator, s.backup
	if !present(s.coordinator) {
		s.coordinator = 0
	}
	if s.coordinator == 0 {
		if present(oldBackup) {
			s.coordinator = oldBackup
		} else if len(ids) > 0 {
			s.coordinator = ids[0]
		}
	}
	s.backup = 0
	for _, id := range ids {
		if id != s.coordinator {
			s.backup = id
			break
		}
	}
	if prev != s.coordinator || oldBackup != s.backup {
		if prev != 0 && prev != s.coordinator {
			s.paused = true
		}
		room.broadcastLocked(serverMessage{Type: "coordinator_changed", Coordinator: s.coordinator, Backup: s.backup, Paused: s.paused}, 0)
		if s.paused && s.coordinator != 0 {
			room.sendToLocked(s.coordinator, serverMessage{Type: "resume_required", Message: "coordinator must confirm synchronized state"})
		}
	}
}

func (r *relay) startRoyale(room *relayRoom) {
	r.mu.Lock()
	room.mu.Lock()
	defer room.mu.Unlock()
	defer r.mu.Unlock()
	if r.rooms[room.key] != room || room.state == nil || room.state.started || len(room.peers) == 0 {
		return
	}
	room.startRoyaleLocked()
}
func (room *relayRoom) startRoyaleLocked() {
	s := room.state
	if s == nil || s.started {
		return
	}
	s.started = true
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	s.botSlots = royaleSlots - len(room.peers)
	if s.botSlots < 0 {
		s.botSlots = 0
	}
	// Bot slots are reserved IDs, not server-side bot simulation.
	room.broadcastLocked(serverMessage{Type: "match_started", Mode: room.mode, Room: room.name, Coordinator: s.coordinator, Backup: s.backup, BotSlots: s.botSlots, Slots: royaleSlots}, 0)
	if room.rosterEnabled {
		room.beginRosterLocked()
	}
}

func (room *relayRoom) handleSessionLocked(p *peer, msg clientMessage) error {
	s := room.state
	if s == nil {
		return errors.New("session unavailable")
	}
	switch msg.Type {
	case "coordinator_resume":
		if room.rosterEnabled && room.barrier != nil {
			return errors.New("cannot resume during a roster barrier")
		}
		if p.id != s.coordinator {
			return errors.New("only coordinator may resume")
		}
		s.paused = false
		room.broadcastLocked(serverMessage{Type: "session_resumed", Coordinator: s.coordinator}, 0)
	case "bot_command":
		if room.rosterEnabled && msg.Generation != room.rosterApplied {
			return errors.New("bot command roster generation mismatch")
		}
		if !room.royale() || !s.started || s.paused || !room.isActiveLocked(p) || p.id != s.coordinator {
			return errors.New("bot command not authorized")
		}
		if msg.BotID == 0 || msg.BotID > uint64(s.botSlots) || msg.Tick == 0 || (room.rosterEnabled && msg.Tick < room.rosterStartTick) || len(msg.Payload) == 0 || len(msg.Payload) > maxPayloadBytes || !jsonValid(msg.Payload) {
			return errors.New("invalid bot command")
		}
		if s.botTicks == nil {
			s.botTicks = make(map[uint64]uint64)
		}
		if msg.Tick <= s.botTicks[msg.BotID] {
			return errors.New("non-monotonic bot command")
		}
		s.botTicks[msg.BotID] = msg.Tick
		room.broadcastLocked(serverMessage{Type: "bot_command", PlayerID: p.id, BotID: msg.BotID, Tick: msg.Tick, Sequence: msg.Sequence, Payload: msg.Payload, Generation: room.rosterApplied}, p.id)
	case "hash_recheck":
		if room.rosterEnabled && (!room.isActiveLocked(p) || msg.Generation != room.rosterApplied) {
			return errors.New("hash recheck roster generation mismatch")
		}
		if p.syncing || msg.Tick == 0 || msg.Tick%room.hashInterval != 0 || len(msg.Hash) != 64 || !isHex(msg.Hash) {
			return errors.New("invalid recheck hash")
		}
		room.acceptHashLocked(p.id, msg.Tick, msg.Hash, true)
	default:
		return fmt.Errorf("unsupported session message %s", msg.Type)
	}
	return nil
}

func (room *relayRoom) acceptHashLocked(id, tick uint64, hash string, recheck bool) {
	s := room.state
	if s == nil {
		return
	}
	votes := s.hashVotes
	if recheck {
		votes = s.recheckVotes
	}
	if votes[tick] == nil {
		votes[tick] = make(map[uint64]string)
	}
	votes[tick][id] = hash
	if len(votes) > 12 {
		for old := range votes {
			if old+room.hashInterval*12 < tick {
				delete(votes, old)
			}
		}
	}
	active := 0
	for _, peer := range room.peers {
		if !peer.syncing {
			active++
		}
	}
	if len(votes[tick]) < active || active < 2 {
		return
	}
	counts := make(map[string]int)
	for peerID, h := range votes[tick] {
		if _, ok := room.peers[peerID]; ok {
			counts[h]++
		}
	}
	delete(votes, tick)
	if len(counts) == 1 {
		if recheck {
			room.broadcastLocked(serverMessage{Type: "hash_verified", Tick: tick}, 0)
		}
		return
	}
	if !recheck {
		room.broadcastLocked(serverMessage{Type: "hash_recheck_requested", Tick: tick}, 0)
		return
	}
	s.paused = true
	// An explicit majority can identify the candidate reference hash; ties
	// remain unresolved. The relay NEVER treats a hash as a world snapshot.
	majorityHash := ""
	for h, n := range counts {
		if n > active/2 {
			majorityHash = h
		}
	}
	room.broadcastLocked(serverMessage{Type: "session_paused", Tick: tick, Hash: majorityHash, Message: "repeated hash mismatch; require snapshot recovery"}, 0)
	if s.coordinator != 0 {
		room.sendToLocked(s.coordinator, serverMessage{Type: "snapshot_requested", Tick: tick})
	}
}

func jsonValid(b []byte) bool { return len(b) > 0 && json.Valid(b) }

// Arm the next periodic snapshot request without keeping an empty room alive.
// A user may also request a final snapshot before leaving.
func (r *relay) armSnapshotTimerLocked(room *relayRoom) {
	if !room.persistent() || room.state == nil || room.state.saveTimer != nil {
		return
	}
	room.state.saveTimer = time.AfterFunc(r.snapshotInterval, func() {
		r.mu.Lock()
		room.mu.Lock()
		if r.rooms[room.key] == room && len(room.peers) > 0 {
			room.state.saveTimer = nil
			if room.state.coordinator != 0 {
				room.sendToLocked(room.state.coordinator, serverMessage{Type: "snapshot_requested", Message: "periodic checkpoint"})
			}
			r.armSnapshotTimerLocked(room)
		}
		room.mu.Unlock()
		r.mu.Unlock()
	})
}
