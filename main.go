package main

// Snowyard Relay Server 0.1: transport and coordination only.
// The Java clients own all simulation, collisions, combat and world state.
import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const protocolVersion = 1
const maxPeers = 100
const maxNicknameBytes = 72
const maxPayloadBytes = 8 * 1024
const maxOutboundQueue = 128

type clientMessage struct {
	Type     string          `json:"type"`
	Version  int             `json:"version,omitempty"`
	Nickname string          `json:"nickname,omitempty"`
	Tick     uint64          `json:"tick,omitempty"`
	Sequence uint64          `json:"sequence,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Hash     string          `json:"hash,omitempty"`
}

type serverMessage struct {
	Type     string          `json:"type"`
	PlayerID uint64          `json:"playerId,omitempty"`
	Nickname string          `json:"nickname,omitempty"`
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
	lastCommandTick uint64
	lastSequence    uint64
	lastHashTick    uint64
	done            chan struct{}
	once            sync.Once
}

func (p *peer) stop() { p.once.Do(func() { close(p.done); p.socket.close() }) }

// A single mutex serializes room membership and message ordering.
// The mutex is NEVER held during network I/O.
type relay struct {
	mu     sync.Mutex
	nextID atomic.Uint64
	peers  map[uint64]*peer
	hashes map[uint64]map[string]int
}

func newRelay() *relay {
	return &relay{peers: make(map[uint64]*peer), hashes: make(map[uint64]map[string]int)}
}
func (r *relay) size() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.peers) }
func (r *relay) add(p *peer) ([]playerInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.peers) >= maxPeers {
		return nil, errors.New("brawl capacity reached")
	}
	roster := make([]playerInfo, 0, len(r.peers)+1)
	for _, other := range r.peers {
		roster = append(roster, playerInfo{other.id, other.nickname})
	}
	roster = append(roster, playerInfo{p.id, p.nickname})
	r.peers[p.id] = p
	r.broadcastLocked(serverMessage{Type: "player_joined", PlayerID: p.id, Nickname: p.nickname}, p.id)
	return roster, nil
}
func (r *relay) remove(p *peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.peers[p.id] != p {
		return
	}
	delete(r.peers, p.id)
	r.broadcastLocked(serverMessage{Type: "player_left", PlayerID: p.id}, p.id)
	if len(r.peers) == 0 {
		r.hashes = make(map[uint64]map[string]int)
	}
}
func (r *relay) broadcastLocked(msg serverMessage, exclude uint64) {
	wire, _ := json.Marshal(msg)
	for id, p := range r.peers {
		if id != exclude {
			select {
			case p.send <- wire:
			default:
				go p.stop()
			}
		}
	}
}
func (r *relay) handle(p *peer, msg clientMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.peers[p.id] != p {
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
		// Payload is opaque to the server. Every receiver simulates it locally.
		p.lastCommandTick = msg.Tick
		p.lastSequence = msg.Sequence
		r.broadcastLocked(serverMessage{Type: "command", PlayerID: p.id, Tick: msg.Tick, Sequence: msg.Sequence, Payload: msg.Payload}, p.id)
	case "hash":
		if msg.Tick == 0 || msg.Tick%8 != 0 || msg.Tick <= p.lastHashTick || len(msg.Hash) != 64 || !isHex(msg.Hash) {
			return errors.New("invalid hash checkpoint")
		}
		p.lastHashTick = msg.Tick
		counts := r.hashes[msg.Tick]
		if counts == nil {
			counts = make(map[string]int)
			r.hashes[msg.Tick] = counts
		}
		counts[msg.Hash]++
		// Forward hashes; this release does NOT claim majority consensus or repair.
		r.broadcastLocked(serverMessage{Type: "hash", PlayerID: p.id, Tick: msg.Tick, Hash: msg.Hash}, p.id)
		// Bound transient hash history. No world data retained in this release.
		for tick := range r.hashes {
			if tick+128 < msg.Tick {
				delete(r.hashes, tick)
			}
		}
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
	data, err := socket.readMessage()
	if err != nil {
		return
	}
	var hello clientMessage
	if err = json.Unmarshal(data, &hello); err != nil || hello.Type != "hello" || hello.Version != protocolVersion || len(hello.Nickname) == 0 || len([]byte(hello.Nickname)) > maxNicknameBytes {
		_ = socket.writeFrame(1, []byte(`{"type":"error","message":"invalid hello or protocol version"}`))
		return
	}
	p := &peer{id: r.nextID.Add(1), nickname: hello.Nickname, socket: socket, send: make(chan []byte, maxOutboundQueue), done: make(chan struct{})}
	roster, err := r.add(p)
	if err != nil {
		_ = socket.writeFrame(1, []byte(`{"type":"error","message":"room full"}`))
		return
	}
	defer func() { r.remove(p); p.stop() }()
	go writer(p)
	sendControl(p, serverMessage{Type: "welcome", PlayerID: p.id, Version: protocolVersion, Players: roster, Message: "relay only: late join requires a future snapshot protocol"})
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
	log.Printf("Snowyard relay v0.1 listening on 0.0.0.0:%s", port)
	log.Fatal(server.ListenAndServe())
}
