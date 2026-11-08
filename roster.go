package main

// Deterministic membership barriers. The relay never chooses a simulation
// state or extrapolates a tick from wall-clock time. New clients opt into this
// protocol using rosterVersion=1 in hello; legacy rooms keep old behavior.
import (
	"errors"
	"sort"
	"strings"
)

const rosterProtocolVersion = 1

type rosterVote struct {
	tick uint64
	hash string
}

type rosterBarrier struct {
	generation uint64
	members    []uint64
	votes      map[uint64]rosterVote
	wasPaused  bool
}

// Called under room.mu. Participants are all synchronized connected peers,
// including currently active players; newly connected syncing peers wait.
func (room *relayRoom) beginRosterLocked() {
	if !room.rosterEnabled || room.state == nil {
		return
	}
	if room.royale() && !room.state.started {
		return
	}
	ids := make([]uint64, 0, len(room.peers))
	for id, p := range room.peers {
		if !p.syncing && !p.leaving {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	room.rosterGeneration++
	pausedBefore := room.state.paused
	if room.barrier != nil {
		pausedBefore = room.barrier.wasPaused
	}
	room.barrier = &rosterBarrier{generation: room.rosterGeneration, members: ids, votes: make(map[uint64]rosterVote), wasPaused: pausedBefore}
	room.state.paused = true
	players := room.rosterPlayersLocked(ids)
	room.broadcastLocked(serverMessage{Type: "roster_prepare", Generation: room.rosterGeneration, Players: players, Paused: true,
		Message: "freeze simulation and report an identical confirmed tick and world hash; relay will apply the roster on tick+1"}, 0)
	// A disconnected member can require a state repair, so the server never
	// manufactures neutral inputs or silently commits a roster on its behalf.
}

func (room *relayRoom) rosterPlayersLocked(ids []uint64) []playerInfo {
	out := make([]playerInfo, 0, len(ids))
	for _, id := range ids {
		if p := room.peers[id]; p != nil {
			out = append(out, playerInfo{ID: id, Nickname: p.nickname})
		}
	}
	return out
}

func (room *relayRoom) handleRosterReadyLocked(p *peer, msg clientMessage) error {
	if !room.rosterEnabled || room.barrier == nil {
		return errors.New("no roster barrier in progress")
	}
	b := room.barrier
	if msg.Generation != b.generation {
		return errors.New("stale roster generation")
	}
	permitted := false
	for _, id := range b.members {
		if id == p.id {
			permitted = true
			break
		}
	}
	if !permitted {
		return errors.New("participant not eligible for roster barrier")
	}
	if len(msg.Hash) != 64 || !isHex(msg.Hash) {
		return errors.New("invalid roster state hash")
	}
	// Tick 0 is valid for the first (unsimulated) world, and also lets an
	// initially empty Brawl bootstrap without inventing phantom ticks.
	b.votes[p.id] = rosterVote{tick: msg.Tick, hash: strings.ToLower(msg.Hash)}
	if len(b.votes) != len(b.members) {
		return nil
	}
	first := b.votes[b.members[0]]
	for _, id := range b.members[1:] {
		v := b.votes[id]
		if v.tick != first.tick || v.hash != first.hash {
			room.broadcastLocked(serverMessage{Type: "roster_disagreement", Generation: b.generation, Paused: true,
				Message: "participants disagree on confirmed tick/hash; repair state and resend roster_ready"}, 0)
			return nil
		}
	}
	if first.tick == ^uint64(0) {
		return errors.New("tick overflow")
	}
	// Membership is committed only after every affected client attests an
	// identical frozen state; it takes effect on the next simulation tick.
	room.rosterMembers = make(map[uint64]bool, len(b.members))
	for _, id := range b.members {
		room.rosterMembers[id] = true
	}
	room.rosterApplied = b.generation
	room.rosterStartTick = first.tick + 1
	room.barrier = nil
	room.state.paused = b.wasPaused
	room.electLocked()
	commit := serverMessage{Type: "roster_commit", Generation: b.generation, Tick: first.tick + 1,
		Hash: first.hash, Players: room.rosterPlayersLocked(b.members), Coordinator: room.state.coordinator,
		Backup: room.state.backup, Paused: room.state.paused}
	// A late joiner replaying a checkpoint must also replay every intervening
	// membership change, not just the movement commands.
	room.appendJournalLocked(commit)
	room.broadcastLocked(commit, 0)
	if room.persistent() && room.state.snapshot == nil && len(b.members) == 1 && !room.state.paused {
		room.sendToLocked(b.members[0], serverMessage{Type: "snapshot_requested", Message: "bootstrap first confirmed world state after roster commit"})
	}
	if room.state.paused && room.state.coordinator != 0 {
		room.sendToLocked(room.state.coordinator, serverMessage{Type: "resume_required", Message: "resolve paused state before continuing"})
	}
	return nil
}

func (room *relayRoom) isActiveLocked(p *peer) bool {
	return !room.rosterEnabled || (room.barrier == nil && room.rosterMembers[p.id])
}
