package main

// Roster protocol v2 is opt-in. It separates the stable voting cohort from
// joining peers: completing snapshot replay NEVER grants a vote in the stable
// cohort. The relay never guesses a tick or computes game physics.
import (
	"errors"
	"sort"
	"strings"
	"time"
)

const rosterProtocolV2 = 2
const rosterBarrierTimeout = 20 * time.Second

type rosterPhase uint8

const (
	rosterFreeze rosterPhase = iota
	rosterJoinVerify
)

// Called only under room.mu. Start at most one transaction at a time.
func (room *relayRoom) beginRosterV2Locked() {
	if !room.rosterEnabled || room.rosterProtocol != rosterProtocolV2 || room.state == nil || room.barrier != nil {
		return
	}
	if room.royale() && !room.state.started {
		return
	}
	active := make([]uint64, 0, len(room.rosterMembers))
	joining := make([]uint64, 0)
	for id, p := range room.peers {
		if p.syncing || p.leaving {
			continue
		}
		if room.rosterMembers[id] {
			active = append(active, id)
		} else {
			joining = append(joining, id)
		}
	}
	sort.Slice(active, func(i, j int) bool { return active[i] < active[j] })
	sort.Slice(joining, func(i, j int) bool { return joining[i] < joining[j] })
	if len(active) == 0 && len(joining) == 0 {
		return
	}
	if len(active) > 0 && len(joining) == 0 && !room.rosterChangePending {
		return // no membership change
	}
	// The bootstrap cohort is a single ready peer. More arrivals wait for the
	// next transaction, avoiding a three-way barrier over unrelated states.
	bootstrap := false
	if len(active) == 0 {
		active = joining[:1]
		joining = joining[1:]
		bootstrap = true
	}
	room.rosterGeneration++
	b := &rosterBarrier{generation: room.rosterGeneration, members: active, votes: make(map[uint64]rosterVote), wasPaused: room.state.paused, joining: joining, phase: rosterFreeze, deadline: time.Now().Add(rosterBarrierTimeout), bootstrap: bootstrap}
	// Prefer a server-observed confirmed checkpoint, rather than allowing
	// active clients to vote on unrelated current ticks. Clients may need
	// to restore/replay to this exact point before acknowledging it.
	if room.state.confirmedHash != "" && !bootstrap {
		b.reference = rosterVote{tick: room.state.confirmedTick, hash: room.state.confirmedHash}
		b.targetPinned = true
	}
	room.barrier = b
	// Bounded server-side timeout: an unresponsive client cannot pause the
	// whole courtyard forever. Its socket may still be healthy (ping/pong).
	if room.barrierTimer != nil {
		room.barrierTimer.Stop()
	}
	room.barrierTimer = time.AfterFunc(rosterBarrierTimeout, func() {
		room.mu.Lock()
		defer room.mu.Unlock()
		if room.barrier != nil && room.barrier.generation == b.generation {
			room.abortRosterV2Locked("roster transaction timed out; retry join")
		}
	})
	room.state.paused = true
	room.broadcastLocked(serverMessage{Type: "session_paused", Paused: true, Generation: b.generation, Message: "roster v2: stop sending commands; existing members must freeze at one confirmed tick"}, 0)
	for _, id := range active {
		room.sendToLocked(id, serverMessage{Type: "roster_prepare", Generation: b.generation, Tick: b.reference.tick, Hash: b.reference.hash, Players: room.rosterPlayersLocked(active), Paused: true, Message: "freeze, align to the indicated confirmed checkpoint if supplied, and vote; joiners do not vote yet"})
	}
}

func (room *relayRoom) handleRosterReadyV2Locked(p *peer, msg clientMessage) error {
	b := room.barrier
	if b == nil || msg.Generation != b.generation {
		return errors.New("no matching roster v2 transaction")
	}
	if time.Now().After(b.deadline) {
		room.abortRosterV2Locked("roster verification timed out")
		return errors.New("roster verification timed out")
	}
	if len(msg.Hash) != 64 || !isHex(msg.Hash) {
		return errors.New("invalid roster state hash")
	}
	hash := strings.ToLower(msg.Hash)
	if b.phase == rosterFreeze {
		if b.targetPinned && (msg.Tick != b.reference.tick || hash != b.reference.hash) {
			return errors.New("active participant has not reached the server confirmed roster checkpoint")
		}
		if !containsPeerID(b.members, p.id) {
			return errors.New("joining client cannot vote as an active roster member")
		}
		b.votes[p.id] = rosterVote{tick: msg.Tick, hash: hash}
		if len(b.votes) != len(b.members) {
			return nil
		}
		reference := b.votes[b.members[0]]
		for _, id := range b.members[1:] {
			if b.votes[id] != reference {
				room.broadcastLocked(serverMessage{Type: "roster_disagreement", Generation: b.generation, Paused: true, Message: "active members disagree; require recovery, not a speculative roster commit"}, 0)
				// Votes may be resubmitted after repair; never commit an unverified state.
				return nil
			}
		}
		if reference.tick == ^uint64(0) {
			return errors.New("tick overflow")
		}
		b.reference = reference
		b.phase = rosterJoinVerify
		b.votes = make(map[uint64]rosterVote)
		b.deadline = time.Now().Add(rosterBarrierTimeout)
		if len(b.joining) == 0 {
			room.commitRosterV2Locked()
			return nil
		}
		// Only the first waiting joiner is admitted during this transaction.
		// Other joiners are queued for a subsequent generation.
		b.joining = b.joining[:1]
		room.sendToLocked(b.joining[0], serverMessage{Type: "roster_checkpoint", Generation: b.generation, Tick: reference.tick, Hash: reference.hash, Paused: true, Message: "verify your replayed world at exactly this tick, then send roster_join_ready"})
		return nil
	}
	return errors.New("expected roster_join_ready from the joining client")
}

func (room *relayRoom) handleRosterJoinReadyV2Locked(p *peer, msg clientMessage) error {
	b := room.barrier
	if room.rosterProtocol != rosterProtocolV2 || b == nil || b.phase != rosterJoinVerify || msg.Generation != b.generation {
		return errors.New("no pending v2 join verification")
	}
	if time.Now().After(b.deadline) {
		room.abortRosterV2Locked("join verification timed out")
		return errors.New("join verification timed out")
	}
	if len(b.joining) != 1 || b.joining[0] != p.id || p.syncing {
		return errors.New("client is not the designated prepared joiner")
	}
	if msg.Tick != b.reference.tick || !strings.EqualFold(msg.Hash, b.reference.hash) {
		return errors.New("joiner has not reached the confirmed checkpoint")
	}
	room.commitRosterV2Locked()
	return nil
}

func (room *relayRoom) commitRosterV2Locked() {
	b := room.barrier
	if b == nil {
		return
	}
	ids := append(append([]uint64(nil), b.members...), b.joining...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	room.rosterMembers = make(map[uint64]bool, len(ids))
	for _, id := range ids {
		room.rosterMembers[id] = true
	}
	room.rosterApplied = b.generation
	// Hash votes from a previous generation cannot influence the new roster.
	room.state.hashVotes = make(map[uint64]map[uint64]string)
	room.state.recheckVotes = make(map[uint64]map[uint64]string)
	room.rosterChangePending = false
	room.rosterStartTick = b.reference.tick + 1
	for _, id := range ids {
		if p := room.peers[id]; p != nil {
			p.lastCommandTick = b.reference.tick
			p.lastSequence = 0
		}
	}
	room.barrier = nil
	if room.barrierTimer != nil {
		room.barrierTimer.Stop()
		room.barrierTimer = nil
	}
	room.state.paused = b.wasPaused
	room.electLocked()
	commit := serverMessage{Type: "roster_commit", Generation: b.generation, Tick: b.reference.tick + 1, Hash: b.reference.hash, Players: room.rosterPlayersLocked(ids), Coordinator: room.state.coordinator, Backup: room.state.backup, Paused: room.state.paused}
	room.appendJournalLocked(commit)
	room.broadcastLocked(commit, 0)
	if room.persistent() && room.state.snapshot == nil && len(ids) == 1 && !room.state.paused {
		room.sendToLocked(ids[0], serverMessage{Type: "snapshot_requested", Message: "bootstrap first checkpoint"})
	}
	if room.state.paused && room.state.coordinator != 0 {
		room.sendToLocked(room.state.coordinator, serverMessage{Type: "resume_required"})
	}
	// A queued joiner starts its own transaction, never changes this commit.
	room.beginRosterV2Locked()
}

func (room *relayRoom) abortRosterV2Locked(reason string) {
	b := room.barrier
	if b == nil {
		return
	}
	room.barrier = nil
	if room.barrierTimer != nil {
		room.barrierTimer.Stop()
		room.barrierTimer = nil
	}
	room.state.paused = b.wasPaused || room.rosterChangePending
	// Joiners that missed the cut cannot silently remain in a live roster.
	// Require a fresh snapshot before another attempt; active members resume
	// only when no uncommitted leave is waiting.
	for _, id := range b.joining {
		if p := room.peers[id]; p != nil {
			p.syncing = true
			if room.persistent() {
				room.sendManifestLocked(p)
			}
		}
	}
	room.broadcastLocked(serverMessage{Type: "roster_aborted", Generation: b.generation, Paused: room.state.paused, Message: reason}, 0)
}

func containsPeerID(ids []uint64, id uint64) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}
