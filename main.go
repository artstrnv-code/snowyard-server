package main

// Snowyard Relay Server 0.2: room-aware transport and coordination only.
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

type clientMessage struct {
	Type     string          `json:"type"`
	Version  int             `json:"version,omitempty"`
	Nickname string          `json:"nickname,omitempty"`
	Mode     string          `json:"mode,omitempty"`
	Room     string          `json:"room,omitempty"`
	Tick     uint64          `json:"tick,omitempty"`
	Sequence uint64          `json:"sequence,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Hash     string          `json:"hash,omitempty"`
}

type serverMessage struct {
	Type     string          `json:"type"`
	PlayerID uint64          `json:"playerId,omitempty"`
	Nickname string          `json:"nickname,omitempty"`
	Mode     string          `json:"mode,omitempty"`
	Room     string          `json:"room,omitempty"`
	Tick     uint64          `json:"tick,omitempty"`
	Sequence uint64          `json:"sequence,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Hash     string          `json:"hash,omitempty"`
	Message  string          `json:"message,omitempty"`
	Players  []playerInfo    `json:"players,omitempty"`
	Version  int             `json:"version,omitempty"`
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
	lastCommandTick uint64
	lastSequence    uint64
	lastHashTick    uint64
	done            chan struct{}
	once            sync.Once
}

func (p *peer) stop() { p.once.Do(func() { close(p.done); p.socket.close() }) }

// Rooms are isolated. A room owns its membership, ordering and transient hashes.
// The hub mutex protects only room registration: gameplay never takes it.
type relayRoom struct {
	key   string
	mode  string
	name  string
	mu    sync.Mutex
	peers map[uint64]*peer
}
type relay struct {
	mu     sync.Mutex
	nextID atomic.Uint64
	total  atomic.Int64
	rooms  map[string]*relayRoom
}

func newRelay() *relay     { return &relay{rooms: make(map[string]*relayRoom)} }
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
	key := mode + "/" + name
	r.mu.Lock()
	if r.total.Load() >= maxPeers {
		r.mu.Unlock()
		return errors.New("relay capacity reached")
	}
	room := r.rooms[key]
	if room == nil {
		if len(r.rooms) >= maxRooms {
			r.mu.Unlock()
			return errors.New("room capacity reached")
		}
		room = &relayRoom{key: key, mode: mode, name: name, peers: make(map[uint64]*peer)}
		r.rooms[key] = room
	}
	room.mu.Lock()
	if r.total.Load() >= maxPeers {
		room.mu.Unlock()
		r.mu.Unlock()
		return errors.New("relay capacity reached")
	}
	roster := make([]playerInfo, 0, len(room.peers)+1)
	for _, other := range room.peers {
		roster = append(roster, playerInfo{other.id, other.nickname})
	}
	roster = append(roster, playerInfo{p.id, p.nickname})
	sort.Slice(roster, func(i, j int) bool { return roster[i].ID < roster[j].ID })
	// Welcome is the FIRST queued message for a new peer, even under joins.
	wire, _ := json.Marshal(serverMessage{Type: "welcome", PlayerID: p.id, Version: protocolVersion, Mode: mode, Room: name, Players: roster, Message: "relay only: late join requires a future snapshot protocol"})
	p.send <- wire
	p.room = room
	room.peers[p.id] = p
	r.total.Add(1)
	room.broadcastLocked(serverMessage{Type: "player_joined", PlayerID: p.id, Nickname: p.nickname}, p.id)
	room.mu.Unlock()
	r.mu.Unlock()
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
	delete(room.peers, p.id)
	r.total.Add(-1)
	room.broadcastLocked(serverMessage{Type: "player_left", PlayerID: p.id}, p.id)
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
	for id, p := range room.peers {
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
		p.lastCommandTick, p.lastSequence = msg.Tick, msg.Sequence
		room.broadcastLocked(serverMessage{Type: "command", PlayerID: p.id, Tick: msg.Tick, Sequence: msg.Sequence, Payload: msg.Payload}, p.id)
	case "hash":
		if msg.Tick == 0 || msg.Tick%8 != 0 || msg.Tick <= p.lastHashTick || len(msg.Hash) != 64 || !isHex(msg.Hash) {
			return errors.New("invalid hash checkpoint")
		}
		p.lastHashTick = msg.Tick
		// The server does not need to tally hashes: no consensus or repair is
		// implemented. Forward opaque checkpoints to the room, nothing more.
		room.broadcastLocked(serverMessage{Type: "hash", PlayerID: p.id, Tick: msg.Tick, Hash: msg.Hash}, p.id)
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
	p := &peer{id: r.nextID.Add(1), nickname: hello.Nickname, socket: socket, send: make(chan []byte, maxOutboundQueue), done: make(chan struct{})}
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
		_, _ = fmt.Fprintf(w, "{\"status\":\"ok\",\"version\":%d,\"players\":%d}", protocolVersion, hub.size())
	})
	mux.HandleFunc("/ws", hub.websocket)
	server := &http.Server{Addr: "0.0.0.0:" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second}
	log.Printf("Snowyard relay v0.2 listening on 0.0.0.0:%s", port)
	log.Fatal(server.ListenAndServe())
}
