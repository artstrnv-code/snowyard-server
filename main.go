package main

// Snowyard Relay Server 0.3: session and state transport, no world simulation.
// The Java clients own all simulation, collisions, combat and world state.
import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const protocolVersion = 1
const maxPeers = 100
const maxNicknameBytes = 72
const maxPayloadBytes = 8 * 1024
const maxOutboundQueue = 128
const maxRooms = 256

type batchedCommand struct {
	Tick     uint64          `json:"tick"`
	Sequence uint64          `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
}

type clientMessage struct {
	Type          string           `json:"type"`
	Version       int              `json:"version,omitempty"`
	Nickname      string           `json:"nickname,omitempty"`
	Mode          string           `json:"mode,omitempty"`
	Room          string           `json:"room,omitempty"`
	Tick          uint64           `json:"tick,omitempty"`
	Sequence      uint64           `json:"sequence,omitempty"`
	Payload       json.RawMessage  `json:"payload,omitempty"`
	Hash          string           `json:"hash,omitempty"`
	Size          int              `json:"size,omitempty"`
	Digest        string           `json:"digest,omitempty"`
	Data          string           `json:"data,omitempty"`
	Index         int              `json:"index,omitempty"`
	SnapshotID    uint64           `json:"snapshotId,omitempty"`
	Cursor        uint64           `json:"cursor,omitempty"`
	BotID         uint64           `json:"botId,omitempty"`
	Commands      []batchedCommand `json:"commands,omitempty"`
	RosterVersion int              `json:"rosterVersion,omitempty"`
	Generation    uint64           `json:"generation,omitempty"`
}

type serverMessage struct {
	ServerVersion string           `json:"serverVersion,omitempty"`
	Type          string           `json:"type"`
	PlayerID      uint64           `json:"playerId,omitempty"`
	Nickname      string           `json:"nickname,omitempty"`
	Mode          string           `json:"mode,omitempty"`
	Room          string           `json:"room,omitempty"`
	Tick          uint64           `json:"tick,omitempty"`
	Sequence      uint64           `json:"sequence,omitempty"`
	Payload       json.RawMessage  `json:"payload,omitempty"`
	Hash          string           `json:"hash,omitempty"`
	Size          int              `json:"size,omitempty"`
	Digest        string           `json:"digest,omitempty"`
	Data          string           `json:"data,omitempty"`
	Index         int              `json:"index,omitempty"`
	Chunks        int              `json:"chunks,omitempty"`
	SnapshotID    uint64           `json:"snapshotId,omitempty"`
	Cursor        uint64           `json:"cursor,omitempty"`
	Entries       []journalEntry   `json:"entries,omitempty"`
	Coordinator   uint64           `json:"coordinator,omitempty"`
	Backup        uint64           `json:"backup,omitempty"`
	Paused        bool             `json:"paused,omitempty"`
	BotSlots      int              `json:"botSlots,omitempty"`
	Slots         int              `json:"slots,omitempty"`
	BotID         uint64           `json:"botId,omitempty"`
	Commands      []batchedCommand `json:"commands,omitempty"`
	HashInterval  uint64           `json:"hashInterval,omitempty"`
	RosterVersion int              `json:"rosterVersion,omitempty"`
	Generation    uint64           `json:"generation,omitempty"`
	Message       string           `json:"message,omitempty"`
	Players       []playerInfo     `json:"players,omitempty"`
	Version       int              `json:"version,omitempty"`
}

type playerInfo struct {
	ID       uint64 `json:"id"`
	Nickname string `json:"nickname"`
}

type peer struct {
	id              uint64
	nickname        string
	socket          *wsConn
	send            chan []byte
	room            *relayRoom
	rosterVersion   int
	lastCommandTick uint64
	lastSequence    uint64
	lastHashTick    uint64
	syncing         bool
	leaving         bool
	syncID          uint64
	replayCursor    uint64
	chunksSeen      []bool
	backupDownload  bool
	done            chan struct{}
	once            sync.Once
}

func (p *peer) stop() { p.once.Do(func() { close(p.done); p.socket.close() }) }

// Rooms are isolated. A room owns its membership, ordering and transient hashes.
// The hub mutex protects only room registration: gameplay never takes it.
type relayRoom struct {
	key                 string
	mode                string
	name                string
	mu                  sync.Mutex
	peers               map[uint64]*peer
	state               *sessionState
	hashInterval        uint64
	rosterEnabled       bool
	rosterProtocol      int
	rosterMembers       map[uint64]bool
	rosterGeneration    uint64
	rosterApplied       uint64
	rosterStartTick     uint64
	barrier             *rosterBarrier
	barrierTimer        *time.Timer
	rosterChangePending bool
	// Freeze normal commands only at the final catch-up batch.
	catchupOwner     uint64
	catchupWasPaused bool
}
type relay struct {
	mu               sync.Mutex
	nextID           atomic.Uint64
	total            atomic.Int64
	rooms            map[string]*relayRoom
	store            SnapshotStore
	worldInitMu      sync.Mutex
	nextMatch        atomic.Uint64
	snapshotInterval time.Duration
	hashInterval     uint64
}

func newRelay() *relay {
	dir := os.Getenv("SNOWYARD_SNAPSHOT_DIR")
	if dir == "" {
		dir = os.TempDir() + "/snowyard-relay"
	}
	local := newTempStore(dir)
	endpoint := os.Getenv("SNOWYARD_SHEETS_URL")
	token := os.Getenv("SNOWYARD_SHEETS_TOKEN")
	if endpoint == "" && token == "" {
		return newRelayWithStore(local)
	}
	if endpoint == "" || token == "" {
		log.Fatal("both SNOWYARD_SHEETS_URL and SNOWYARD_SHEETS_TOKEN must be configured")
	}
	remote, err := newSheetsAPI(endpoint, token)
	if err != nil {
		log.Fatalf("invalid world storage configuration: %v", err)
	}
	return newRelayWithStore(newHybridStore(local, remote))
}
func newRelayWithStore(store SnapshotStore) *relay {
	interval := 5 * time.Minute
	if raw := os.Getenv("SNOWYARD_SNAPSHOT_INTERVAL_SEC"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err == nil && n >= 30 && n <= 3600 {
			interval = time.Duration(n) * time.Second
		}
	}
	hashInterval := uint64(120)
	if raw := os.Getenv("SNOWYARD_HASH_INTERVAL_TICKS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err == nil && n >= 60 && n <= 3600 {
			hashInterval = uint64(n)
		}
	}
	return &relay{rooms: make(map[string]*relayRoom), store: store, snapshotInterval: interval, hashInterval: hashInterval}
}
func (r *relay) size() int { return int(r.total.Load()) }

// Legacy clients omit both fields. They remain in their original shared room.
// Future clients may choose an explicit mode/room (e.g. matchmaking-created ID).
func validRoomPart(s string) bool {
	if len(s) == 0 || len(s) > 48 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
func resolveRoom(mode, name string) (string, string, error) {
	if mode == "" && name == "" {
		return "legacy", "global", nil
	}
	if mode == "" || name == "" || !validRoomPart(mode) || !validRoomPart(name) {
		return "", "", errors.New("invalid mode or room")
	}
	// Reserved legacy namespace prevents an opt-in client from bypassing routing.
	if strings.EqualFold(mode, "legacy") {
		return "", "", errors.New("reserved mode")
	}
	return mode, name, nil
}
func (r *relay) add(p *peer, mode, name string) error {
	if p.rosterVersion != 0 && mode != "brawl" && mode != "battle_royale" {
		return errors.New("roster protocol requires an advanced mode")
	}
	// Load the persistent world outside the hub mutex, so Apps Script
	// latency cannot delay joins to other rooms. Serialize only Brawl
	// initialization, including recreation after the room becomes empty.
	var loaded *worldSnapshot
	if mode == "brawl" && name == "global" {
		r.worldInitMu.Lock()
		defer r.worldInitMu.Unlock()
		r.mu.Lock()
		_, exists := r.rooms["brawl/global"]
		r.mu.Unlock()
		if !exists {
			snapshot, err := r.store.Load()
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("cannot load saved world: %w", err)
			}
			loaded = snapshot
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.total.Load() >= maxPeers {
		return errors.New("relay capacity reached")
	}
	// Auto-matchmaking allocates private 10-slot Battle Royale rooms.
	if mode == "battle_royale" && name == "auto" {
		for _, candidate := range r.rooms {
			candidate.mu.Lock()
			available := candidate.royale() && !candidate.state.started && len(candidate.peers) < royaleSlots
			candidate.mu.Unlock()
			if available {
				name = candidate.name
				break
			}
		}
		if name == "auto" {
			name = fmt.Sprintf("match-%d", r.nextMatch.Add(1))
		}
	}
	key := mode + "/" + name
	room := r.rooms[key]
	if room == nil {
		if len(r.rooms) >= maxRooms {
			return errors.New("room capacity reached")
		}
		room = &relayRoom{key: key, mode: mode, name: name, peers: make(map[uint64]*peer), hashInterval: r.hashInterval, rosterEnabled: p.rosterVersion != 0, rosterProtocol: p.rosterVersion, rosterMembers: make(map[uint64]bool)}
		if room.advanced() {
			room.state = &sessionState{hashVotes: make(map[uint64]map[uint64]string), recheckVotes: make(map[uint64]map[uint64]string)}
		}
		if room.persistent() {
			room.state.snapshot = loaded
		}
		r.rooms[key] = room
	}
	room.mu.Lock()
	defer room.mu.Unlock()
	if room.rosterProtocol != p.rosterVersion {
		return errors.New("mixed roster protocol versions in one room")
	}
	if room.rosterEnabled && !room.advanced() {
		return errors.New("roster protocol requires an advanced room")
	}
	if r.total.Load() >= maxPeers {
		return errors.New("relay capacity reached")
	}
	if room.royale() && (room.state.started || len(room.peers) >= royaleSlots) {
		return errors.New("battle royale match is full or started")
	}
	roster := make([]playerInfo, 0, len(room.peers)+1)
	for _, other := range room.peers {
		roster = append(roster, playerInfo{other.id, other.nickname})
	}
	roster = append(roster, playerInfo{p.id, p.nickname})
	sort.Slice(roster, func(i, j int) bool { return roster[i].ID < roster[j].ID })
	checkpoint := uint64(0)
	if room.advanced() {
		checkpoint = room.hashInterval
	}
	wire, _ := json.Marshal(serverMessage{Type: "welcome", ServerVersion: "0.3.4a", PlayerID: p.id, Version: protocolVersion, Mode: mode, Room: name, Players: roster, HashInterval: checkpoint, RosterVersion: p.rosterVersion, Message: "relay v0.3.4a: fresh checkpoint roster and version handshake"})
	p.send <- wire
	p.room = room
	if room.persistent() && (room.state.snapshot != nil || len(room.peers) > 0) {
		p.syncing = true
	}
	room.peers[p.id] = p
	r.total.Add(1)
	if room.rosterEnabled {
		room.broadcastLocked(serverMessage{Type: "player_waiting", PlayerID: p.id, Nickname: p.nickname}, p.id)
	} else {
		room.broadcastLocked(serverMessage{Type: "player_joined", PlayerID: p.id, Nickname: p.nickname}, p.id)
	}
	if room.advanced() {
		room.electLocked()
	}
	if room.persistent() && p.syncing {
		room.sendManifestLocked(p)
	} else if room.rosterEnabled && !room.royale() {
		room.beginRosterLocked()
	}
	if room.persistent() && len(room.peers) == 1 && room.state.snapshot == nil && !room.rosterEnabled {
		room.sendToLocked(p.id, serverMessage{Type: "snapshot_requested", Message: "bootstrap initial world from client"})
	}
	if room.persistent() && len(room.peers) == 1 {
		r.armSnapshotTimerLocked(room)
	}
	if room.royale() {
		if len(room.peers) == 1 {
			room.state.startedAt = time.Now()
			room.state.timer = time.AfterFunc(royaleWait, func() { r.startRoyale(room) })
		}
		if len(room.peers) == royaleSlots {
			room.startRoyaleLocked()
		}
	}
	return nil
}
func (r *relay) remove(p *peer) {
	room := p.room
	if room == nil {
		return
	}
	room.mu.Lock()
	if room.peers[p.id] != p {
		room.mu.Unlock()
		return
	}
	wasActive := room.rosterMembers[p.id]
	if room.catchupOwner == p.id {
		room.catchupOwner = 0
		if room.state != nil && room.barrier == nil {
			room.state.paused = room.catchupWasPaused
			if !room.state.paused {
				room.broadcastLocked(serverMessage{Type: "session_resumed"}, 0)
			}
		}
	}
	delete(room.peers, p.id)
	if room.state != nil {
		if room.state.uploading != nil && room.state.uploading.Owner == p.id {
			room.state.uploading = nil
		}
		for _, votes := range room.state.hashVotes {
			delete(votes, p.id)
		}
		for _, votes := range room.state.recheckVotes {
			delete(votes, p.id)
		}
	}
	r.total.Add(-1)
	if room.rosterEnabled {
		room.broadcastLocked(serverMessage{Type: "player_disconnected", PlayerID: p.id}, p.id)
		delete(room.rosterMembers, p.id)
		if wasActive && room.rosterProtocol == rosterProtocolV2 {
			room.rosterChangePending = true
		}
		// Elect before creating the membership barrier so the coordinator
		// handoff's mandatory pause survives the roster commit.
		if room.advanced() {
			room.electLocked()
		}
		if room.rosterProtocol == rosterProtocolV2 && room.barrier != nil {
			room.abortRosterV2Locked("participant disconnected during roster verification")
		}
		if wasActive || room.rosterProtocol == rosterProtocolV2 {
			room.beginRosterLocked()
		} else if room.barrier != nil {
			room.beginRosterLocked()
		}
	} else {
		room.broadcastLocked(serverMessage{Type: "player_left", PlayerID: p.id}, p.id)
		if room.advanced() {
			room.electLocked()
		}
	}
	if room.state != nil && len(room.peers) == 0 {
		if room.state.timer != nil {
			room.state.timer.Stop()
		}
		if room.state.saveTimer != nil {
			room.state.saveTimer.Stop()
		}
	}
	room.mu.Unlock()
	// Lock order always hub -> room: safe against concurrent joins.
	r.mu.Lock()
	room.mu.Lock()
	if len(room.peers) == 0 && r.rooms[room.key] == room {
		delete(r.rooms, room.key)
	}
	room.mu.Unlock()
	r.mu.Unlock()
}
func (room *relayRoom) broadcastLocked(msg serverMessage, exclude uint64) {
	wire, _ := json.Marshal(msg) // One serialization per broadcast, not per player.
	gameplay := msg.Type == "command" || msg.Type == "command_batch" || msg.Type == "hash" || msg.Type == "bot_command"
	for id, p := range room.peers {
		if gameplay && p.syncing {
			continue
		}
		if id == exclude {
			continue
		}
		select {
		case p.send <- wire:
		default:
			go p.stop()
		}
	}
}
func (r *relay) handle(p *peer, msg clientMessage) error {
	room := p.room
	if room == nil {
		return errors.New("disconnected")
	}
	room.mu.Lock()
	defer room.mu.Unlock()
	if room.peers[p.id] != p {
		return errors.New("disconnected")
	}
	switch msg.Type {
	case "command":
		if msg.Tick == 0 || msg.Tick < p.lastCommandTick || (msg.Tick == p.lastCommandTick && msg.Sequence <= p.lastSequence) {
			return errors.New("non-monotonic command tick or sequence")
		}
		if len(msg.Payload) == 0 || len(msg.Payload) > maxPayloadBytes || !json.Valid(msg.Payload) {
			return errors.New("invalid command payload")
		}
		if p.syncing || p.leaving || !room.isActiveLocked(p) || (room.state != nil && room.state.paused) {
			return errors.New("session not accepting commands")
		}
		if room.rosterEnabled && (msg.Generation != room.rosterApplied || msg.Tick < room.rosterStartTick) {
			return errors.New("command roster generation or effective tick mismatch")
		}
		p.lastCommandTick, p.lastSequence = msg.Tick, msg.Sequence
		cmd := serverMessage{Type: "command", PlayerID: p.id, Tick: msg.Tick, Sequence: msg.Sequence, Payload: msg.Payload, Generation: room.rosterApplied}
		room.appendJournalLocked(cmd)
		room.broadcastLocked(cmd, p.id)
	case "command_batch":
		if p.syncing || p.leaving || !room.isActiveLocked(p) || (room.state != nil && room.state.paused) {
			return errors.New("session not accepting commands")
		}
		if room.rosterEnabled && msg.Generation != room.rosterApplied {
			return errors.New("batch roster generation mismatch")
		}
		if len(msg.Commands) == 0 || len(msg.Commands) > 32 {
			return errors.New("invalid command batch size")
		}
		lastTick, lastSeq := p.lastCommandTick, p.lastSequence
		for _, c := range msg.Commands {
			if c.Tick == 0 || (room.rosterEnabled && c.Tick < room.rosterStartTick) || c.Tick < lastTick || (c.Tick == lastTick && c.Sequence <= lastSeq) || len(c.Payload) == 0 || len(c.Payload) > maxPayloadBytes || !json.Valid(c.Payload) {
				return errors.New("invalid command batch")
			}
			lastTick, lastSeq = c.Tick, c.Sequence
		}
		p.lastCommandTick, p.lastSequence = lastTick, lastSeq
		for _, c := range msg.Commands {
			room.appendJournalLocked(serverMessage{Type: "command", PlayerID: p.id, Tick: c.Tick, Sequence: c.Sequence, Payload: c.Payload, Generation: room.rosterApplied})
		}
		room.broadcastLocked(serverMessage{Type: "command_batch", PlayerID: p.id, Commands: msg.Commands, Generation: room.rosterApplied}, p.id)
	case "leave_request":
		if !room.persistent() || room.state == nil {
			room.sendToLocked(p.id, serverMessage{Type: "leave_ready"})
			return nil
		}
		if len(room.peers) == 1 && room.state.coordinator == p.id {
			p.leaving = true
			room.sendToLocked(p.id, serverMessage{Type: "snapshot_requested", Message: "save final state before disconnect"})
		} else {
			room.sendToLocked(p.id, serverMessage{Type: "leave_ready"})
		}
	case "hash":
		period := uint64(8)
		if room.advanced() {
			period = room.hashInterval
		}
		if msg.Tick == 0 || msg.Tick%period != 0 || msg.Tick <= p.lastHashTick || len(msg.Hash) != 64 || !isHex(msg.Hash) {
			return errors.New("invalid hash checkpoint")
		}
		if p.syncing || (room.rosterEnabled && !room.isActiveLocked(p)) {
			return errors.New("player cannot vote before joining the active roster")
		}
		if room.rosterEnabled && (room.barrier != nil || msg.Generation != room.rosterApplied) {
			return errors.New("hash roster generation mismatch or pending change")
		}
		p.lastHashTick = msg.Tick
		// The server does not need to tally hashes: no consensus or repair is
		// implemented. Forward opaque checkpoints to the room, nothing more.
		room.broadcastLocked(serverMessage{Type: "hash", PlayerID: p.id, Tick: msg.Tick, Hash: msg.Hash, Generation: room.rosterApplied}, p.id)
		if room.advanced() {
			room.acceptHashLocked(p.id, msg.Tick, msg.Hash, false)
		}
	case "roster_ready":
		return room.handleRosterReadyLocked(p, msg)
	case "roster_join_ready":
		return room.handleRosterJoinReadyV2Locked(p, msg)
	case "coordinator_resume", "bot_command", "hash_recheck", "recovery_ready":
		return room.handleSessionLocked(p, msg)
	case "snapshot_begin", "snapshot_chunk", "snapshot_commit", "snapshot_info", "snapshot_get", "sync_poll", "sync_finish":
		return r.handleSnapshotLocked(room, p, msg)
	default:
		return errors.New("unknown message type")
	}
	return nil
}
func isHex(s string) bool {
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

func writer(p *peer) {
	defer p.stop()
	heartbeat := time.NewTicker(25 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-heartbeat.C:
			if err := p.socket.writeFrame(0x9, nil); err != nil {
				return
			}
		case wire := <-p.send:
			if err := p.socket.writeFrame(1, wire); err != nil {
				return
			}
		}
	}
}
func sendControl(p *peer, msg serverMessage) {
	wire, _ := json.Marshal(msg)
	select {
	case p.send <- wire:
	default:
		p.stop()
	}
}
func (r *relay) websocket(w http.ResponseWriter, req *http.Request) {
	socket, err := upgradeWebSocket(w, req)
	if err != nil {
		return
	}
	defer socket.close()
	// The first message must be a versioned hello; reject idle connections.
	data, err := socket.readMessageWithTimeout(15 * time.Second)
	if err != nil {
		return
	}
	var hello clientMessage
	if err = json.Unmarshal(data, &hello); err != nil || hello.Type != "hello" || hello.Version != protocolVersion || len(hello.Nickname) == 0 || len([]byte(hello.Nickname)) > maxNicknameBytes {
		_ = socket.writeFrame(1, []byte(`{"type":"error","message":"invalid hello or protocol version"}`))
		return
	}
	if hello.RosterVersion != 0 && hello.RosterVersion != rosterProtocolVersion && hello.RosterVersion != rosterProtocolV2 {
		_ = socket.writeFrame(1, []byte(`{"type":"error","message":"unsupported roster protocol"}`))
		return
	}
	p := &peer{id: r.nextID.Add(1), nickname: hello.Nickname, rosterVersion: hello.RosterVersion, socket: socket, send: make(chan []byte, maxOutboundQueue), done: make(chan struct{})}
	mode, roomName, err := resolveRoom(hello.Mode, hello.Room)
	if err == nil {
		err = r.add(p, mode, roomName)
	}
	if err != nil {
		_ = socket.writeFrame(1, []byte(`{"type":"error","message":"invalid or full room"}`))
		return
	}
	defer func() { r.remove(p); p.stop() }()
	go writer(p)
	for {
		message, err := socket.readMessage()
		if err != nil {
			return
		}
		var cmd clientMessage
		if err = json.Unmarshal(message, &cmd); err != nil {
			sendControl(p, serverMessage{Type: "error", Message: "invalid JSON"})
			continue
		}
		if err = r.handle(p, cmd); err != nil {
			sendControl(p, serverMessage{Type: "error", Message: err.Error()})
		}
	}
}
func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "10000"
	}
	if _, err := strconv.Atoi(port); err != nil {
		log.Fatalf("invalid PORT: %v", err)
	}
	hub := newRelay()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, "{\"status\":\"ok\",\"version\":%d,\"serverVersion\":\"0.3.4a\",\"players\":%d}", protocolVersion, hub.size())
	})
	mux.HandleFunc("/ws", hub.websocket)
	server := &http.Server{Addr: "0.0.0.0:" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second}
	log.Printf("Snowyard relay v0.3.4a listening on 0.0.0.0:%s", port)
	log.Fatal(server.ListenAndServe())
}
