package main

// Roster v3 admits live players without pausing an established Brawl session.
// The joining client restores a versioned snapshot, replays the journal, then
// subscribes to ongoing inputs. The relay schedules a deterministic future
// effective tick; existing clients continue playing their old roster until then.
// The relay never simulates physics or supplies fabricated player commands.
import (
	"encoding/json"
	"errors"
	"sort"
	"time"
)

const rosterProtocolV3 = 3
const joinLeadTicks uint64 = 180  // 3 s at 60 TPS for plan delivery and catch-up
const joinAbortMargin uint64 = 36 // cancel slow plans before old inputs reach the boundary
const joinProgressLag uint64 = 24
const maxLiveTailEntries = 256

type liveJoinPlan struct {
	generation    uint64
	effectiveTick uint64
	joiner        uint64
	members       []uint64
	ready         map[uint64]bool
	timer         *time.Timer
}

func (room *relayRoom) bootstrapV3Locked() {
	if !room.rosterEnabled || room.rosterProtocol != rosterProtocolV3 || room.state == nil || room.rosterApplied != 0 {
		return
	}
	ids := room.orderedPlayersLocked()
	if len(ids) == 0 {
		return
	}
	first := ids[0]
	room.rosterGeneration++
	room.rosterApplied = room.rosterGeneration
	room.rosterStartTick = 1
	room.rosterMembers[first] = true
	commit := serverMessage{Type: "roster_commit", Generation: room.rosterApplied, Tick: 1,
		Players: room.rosterPlayersLocked([]uint64{first}), Coordinator: room.state.coordinator}
	room.appendJournalLocked(commit)
	room.sendToLocked(first, commit)
	if room.state.snapshot == nil {
		room.sendToLocked(first, serverMessage{Type: "snapshot_requested", Message: "bootstrap Brawl checkpoint"})
	}
}

func (room *relayRoom) maxCommandTickV3Locked() uint64 {
	max := uint64(0)
	for id := range room.rosterMembers {
		if p := room.peers[id]; p != nil && p.lastCommandTick > max {
			max = p.lastCommandTick
		}
	}
	return max
}

// Finish the last replay batch ATOMICALLY with admission to the live stream.
// All subsequent broadcasts are appended to the same peer send queue in
// room-lock order, so there is no gap between historical and live commands.
func (room *relayRoom) finishLiveReplayV3Locked(p *peer, msg clientMessage) error {
	s := room.state
	if s == nil || s.snapshot == nil || !p.syncing || msg.SnapshotID != s.snapshot.Version || p.syncID != s.snapshot.Version {
		return errors.New("invalid live replay snapshot")
	}
	for _, ok := range p.chunksSeen {
		if !ok {
			return errors.New("snapshot chunks incomplete")
		}
	}
	if msg.Cursor != p.replayCursor {
		return errors.New("invalid final replay cursor")
	}
	if len(s.journal) > 0 && p.replayCursor+1 < s.journal[0].Event {
		room.sendManifestLocked(p)
		return nil
	}
	remaining := make([]journalEntry, 0)
	for _, e := range s.journal {
		if e.Event > p.replayCursor {
			remaining = append(remaining, e)
			if len(remaining) > maxLiveTailEntries {
				room.sendToLocked(p.id, serverMessage{Type: "sync_retry", Message: "replay tail is busy; poll again"})
				return nil
			}
		}
	}
	// The Java WebSocket listener accepts frames of at most 64 KiB.
	// Keep the final batch comfortably below that bound; retry without
	// breaking cursor continuity if a burst arrived during sync_finish.
	wire, err := json.Marshal(serverMessage{Type: "sync_live", Entries: remaining})
	if err != nil || len(wire) > 48000 {
		room.sendToLocked(p.id, serverMessage{Type: "sync_retry", Message: "live tail exceeds safe frame limit"})
		return nil
	}
	p.syncing = false
	p.awaitingJoin = true
	p.replayCursor = s.nextEvent
	// Must be sent BEFORE starting live command forwarding and a join plan.
	room.sendToLocked(p.id, serverMessage{Type: "sync_live", SnapshotID: s.snapshot.Version, Cursor: s.nextEvent,
		Entries: remaining, Tick: room.maxCommandTickV3Locked(), Message: "live journal subscription active"})
	return nil
}

func (room *relayRoom) handleJoinProgressV3Locked(p *peer, msg clientMessage) error {
	if !p.awaitingJoin || p.syncing || room.rosterMembers[p.id] {
		// An in-flight progress report may arrive just after admission.
		return nil
	}
	if room.v3Plan != nil {
		return nil
	}
	latest := room.maxCommandTickV3Locked()
	if msg.Tick+joinProgressLag < latest {
		return nil
	}
	// A previous future roster commit must have actually taken effect before
	// the next one can be scheduled; otherwise a third joiner can overtake P2.
	if room.v3EffectiveTick != 0 {
		for id := range room.rosterMembers {
			member := room.peers[id]
			if member == nil || member.lastCommandTick < room.v3EffectiveTick {
				return nil
			}
		}
	}
	if len(room.rosterMembers) == 0 {
		// First player after a relay restart restores the persisted snapshot.
		// No active participants exist to prepare/confirm a freeze barrier.
		return room.restoreSoloV3Locked(p, msg.Tick)
	}
	return room.planV3Locked(p.id)
}

func (room *relayRoom) planV3Locked(joiner uint64) error {
	if room.v3Plan != nil || room.state == nil || room.state.paused {
		return nil
	}
	p := room.peers[joiner]
	if p == nil || !p.awaitingJoin || p.syncing {
		return errors.New("joining peer not ready")
	}
	ids := make([]uint64, 0, len(room.rosterMembers)+1)
	for id := range room.rosterMembers {
		if room.peers[id] != nil {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 || len(ids) >= 100 {
		return errors.New("invalid active roster size")
	}
	ids = append(ids, joiner)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	latest := room.maxCommandTickV3Locked()
	if latest > ^uint64(0)-joinLeadTicks {
		return errors.New("join tick overflow")
	}
	room.rosterGeneration++
	plan := &liveJoinPlan{generation: room.rosterGeneration, effectiveTick: latest + joinLeadTicks,
		joiner: joiner, members: ids, ready: make(map[uint64]bool)}
	room.v3Plan = plan
	for _, id := range ids {
		room.sendToLocked(id, serverMessage{Type: "roster_plan", Generation: plan.generation, Tick: plan.effectiveTick,
			Players: room.rosterPlayersLocked(ids), Message: "continue old roster until the scheduled tick; do not pause"})
	}
	plan.timer = time.AfterFunc(10*time.Second, func() {
		room.mu.Lock()
		defer room.mu.Unlock()
		if room.v3Plan == plan {
			room.cancelV3PlanLocked("join plan acknowledgement timed out")
		}
	})
	return nil
}

func (room *relayRoom) handleJoinPlanReadyV3Locked(p *peer, msg clientMessage) error {
	b := room.v3Plan
	if b == nil || msg.Generation != b.generation {
		// Ignore a late acknowledgement of an already cancelled plan.
		return nil
	}
	if msg.Tick != b.effectiveTick || !containsPeerID(b.members, p.id) {
		return errors.New("invalid join plan acknowledgement")
	}
	b.ready[p.id] = true
	if len(b.ready) != len(b.members) {
		return nil
	}
	if b.timer != nil {
		b.timer.Stop()
	}
	room.v3Plan = nil
	room.v3PriorGeneration = room.rosterApplied
	room.v3EffectiveTick = b.effectiveTick
	room.rosterMembers = make(map[uint64]bool, len(b.members))
	for _, id := range b.members {
		room.rosterMembers[id] = true
	}
	room.rosterApplied = b.generation
	room.rosterStartTick = b.effectiveTick
	room.state.hashVotes = make(map[uint64]map[uint64]string)
	room.state.recheckVotes = make(map[uint64]map[uint64]string)
	for _, id := range b.members {
		if member := room.peers[id]; member != nil {
			// Preserve old command monotonicity until the future transition.
			if id == b.joiner {
				member.lastCommandTick = b.effectiveTick - 1
				member.lastSequence = 0
				member.awaitingJoin = false
			}
		}
	}
	room.electLocked()
	commit := serverMessage{Type: "roster_commit", Generation: b.generation, Tick: b.effectiveTick,
		Players: room.rosterPlayersLocked(b.members), Coordinator: room.state.coordinator}
	room.appendJournalLocked(commit)
	room.broadcastLocked(commit, 0)
	return nil
}

func (room *relayRoom) cancelV3PlanLocked(reason string) {
	b := room.v3Plan
	if b == nil {
		return
	}
	room.v3Plan = nil
	if b.timer != nil {
		b.timer.Stop()
	}
	for _, id := range b.members {
		room.sendToLocked(id, serverMessage{Type: "roster_plan_cancelled", Generation: b.generation, Message: reason})
	}
}

func (room *relayRoom) handleDepartureV3Locked(p *peer, wasActive bool) {
	if room.v3Plan != nil && containsPeerID(room.v3Plan.members, p.id) {
		room.cancelV3PlanLocked("participant disconnected during join plan")
	}
	if wasActive && room.state != nil {
		// An unexpected departure creates an unsatisfied lockstep input.
		// Explicit recovery is safer than fabricating neutral commands.
		room.state.paused = true
		room.broadcastLocked(serverMessage{Type: "session_paused", Paused: true, Message: "active player disconnected; checkpoint recovery required"}, p.id)
	}
}

// The two generations overlap in transport time but never in simulated time.
// Old inputs are valid strictly before effectiveTick; new inputs at or after it.
func (room *relayRoom) validInputGenerationLocked(generation, tick uint64) bool {
	if room.rosterProtocol != rosterProtocolV3 {
		return generation == room.rosterApplied && tick >= room.rosterStartTick
	}
	if room.v3EffectiveTick != 0 && tick < room.v3EffectiveTick {
		return generation == room.v3PriorGeneration
	}
	return generation == room.rosterApplied && tick >= room.rosterStartTick
}

// Rejoin a saved, currently empty global world without a roster vote.
// Its old actors remain in the saved state until the deterministic transition.
func (room *relayRoom) restoreSoloV3Locked(p *peer, checkpointTick uint64) error {
	if len(room.rosterMembers) != 0 || !p.awaitingJoin || room.v3Plan != nil {
		return errors.New("solo restore unavailable")
	}
	if checkpointTick == ^uint64(0) {
		return errors.New("tick overflow")
	}
	room.rosterGeneration++
	room.rosterApplied = room.rosterGeneration
	room.rosterStartTick = checkpointTick + 1
	room.rosterMembers[p.id] = true
	p.lastCommandTick = checkpointTick
	p.lastSequence = 0
	p.awaitingJoin = false
	room.electLocked()
	commit := serverMessage{Type: "roster_commit", Generation: room.rosterApplied,
		Tick: checkpointTick + 1, Players: room.rosterPlayersLocked([]uint64{p.id}),
		Coordinator: room.state.coordinator}
	room.appendJournalLocked(commit)
	room.sendToLocked(p.id, commit)
	return nil
}

// Cancel an unconfirmed join BEFORE established clients run out of old-roster
// ticks. This bounds the gameplay interruption even when a joiner goes offline.
func (room *relayRoom) guardJoinBoundaryV3Locked(tick uint64) {
	b := room.v3Plan
	if room.rosterProtocol != rosterProtocolV3 || b == nil {
		return
	}
	if tick >= b.effectiveTick-joinAbortMargin {
		room.cancelV3PlanLocked("join cancelled before its tick boundary; active game continues")
	}
}
