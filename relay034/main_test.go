package main

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type testSocket struct {
	c net.Conn
	r *bufio.Reader
}

func connectTest(t *testing.T, host string) *testSocket {
	t.Helper()
	c, err := net.DialTimeout("tcp", host, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString(raw)
	_, err = fmt.Fprintf(c, "GET /ws HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n\r\n", host, key)
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(c)
	status, err := r.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "101") {
		t.Fatalf("upgrade: %s", status)
	}
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	return &testSocket{c, r}
}
func (s *testSocket) send(t *testing.T, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	mask := [4]byte{5, 17, 8, 22}
	header := []byte{0x81}
	if len(b) < 126 {
		header = append(header, 0x80|byte(len(b)))
	} else {
		header = append(header, 0xfe, byte(len(b)>>8), byte(len(b)))
	}
	header = append(header, mask[:]...)
	for i := range b {
		b[i] ^= mask[i%4]
	}
	if _, err = s.c.Write(append(header, b...)); err != nil {
		t.Fatal(err)
	}
}
func (s *testSocket) receive(t *testing.T) serverMessage {
	t.Helper()
	_ = s.c.SetReadDeadline(time.Now().Add(2 * time.Second))
	h := make([]byte, 2)
	if _, err := io.ReadFull(s.r, h); err != nil {
		t.Fatal(err)
	}
	n := int(h[1] & 0x7f)
	if n == 126 {
		var count uint16
		if err := binary.Read(s.r, binary.BigEndian, &count); err != nil {
			t.Fatal(err)
		}
		n = int(count)
	}
	if n > maxMessageSize {
		t.Fatalf("oversized server frame %d", n)
	}
	raw := make([]byte, n)
	if _, err := io.ReadFull(s.r, raw); err != nil {
		t.Fatal(err)
	}
	var v serverMessage
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}
func TestRelayTwoClients(t *testing.T) {
	hub := newRelay()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", hub.websocket)
	server := httptest.NewServer(mux)
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")
	a := connectTest(t, host)
	a.send(t, clientMessage{Type: "hello", Version: 1, Nickname: "A"})
	aw := a.receive(t)
	if aw.Type != "welcome" || aw.PlayerID == 0 {
		t.Fatalf("bad first welcome: %+v", aw)
	}
	b := connectTest(t, host)
	b.send(t, clientMessage{Type: "hello", Version: 1, Nickname: "B"})
	bw := b.receive(t)
	if bw.Type != "welcome" || len(bw.Players) != 2 {
		t.Fatalf("bad second welcome: %+v", bw)
	}
	joined := a.receive(t)
	if joined.Type != "player_joined" {
		t.Fatalf("expected joined, got %+v", joined)
	}
	a.send(t, clientMessage{Type: "command", Tick: 8, Sequence: 1, Payload: json.RawMessage(`{"move":"left"}`)})
	got := b.receive(t)
	if got.Type != "command" || got.PlayerID != aw.PlayerID || got.Tick != 8 || got.Sequence != 1 || string(got.Payload) != `{"move":"left"}` {
		t.Fatalf("bad relay: %+v", got)
	}
	a.send(t, clientMessage{Type: "hash", Tick: 8, Hash: strings.Repeat("a", 64)})
	got = b.receive(t)
	if got.Type != "hash" || got.Hash != strings.Repeat("a", 64) {
		t.Fatalf("bad hash relay: %+v", got)
	}
	a.send(t, clientMessage{Type: "command", Tick: 8, Sequence: 1, Payload: json.RawMessage(`{"move":"right"}`)})
	got = a.receive(t)
	if got.Type != "error" {
		t.Fatalf("duplicate command accepted: %+v", got)
	}
	_ = b.c.Close()
	left := a.receive(t)
	if left.Type != "player_left" || left.PlayerID != bw.PlayerID {
		t.Fatalf("bad disconnect: %+v", left)
	}
}
func TestInvalidHandshake(t *testing.T) {
	hub := newRelay()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", hub.websocket)
	server := httptest.NewServer(mux)
	defer server.Close()
	s := connectTest(t, strings.TrimPrefix(server.URL, "http://"))
	s.send(t, clientMessage{Type: "hello", Version: 999, Nickname: "A"})
	got := s.receive(t)
	if got.Type != "error" {
		t.Fatalf("bad protocol accepted: %+v", got)
	}
}

// Three players in one room must all receive the same command; a fourth player
// in another room must not receive gameplay or membership events from it.
func receiveGameplayEvent(t *testing.T, s *testSocket) serverMessage {
	t.Helper()
	for i := 0; i < 8; i++ {
		v := s.receive(t)
		if v.Type != "coordinator_changed" {
			return v
		}
	}
	t.Fatal("unexpected repeated coordinator events")
	return serverMessage{}
}
func TestMultiPlayerRoomIsolation(t *testing.T) {
	hub := newRelay()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", hub.websocket)
	server := httptest.NewServer(mux)
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")
	connect := func(nick, mode, room string) (*testSocket, serverMessage) {
		s := connectTest(t, host)
		s.send(t, clientMessage{Type: "hello", Version: 1, Nickname: nick, Mode: mode, Room: room})
		w := receiveGameplayEvent(t, s)
		if w.Type != "welcome" {
			t.Fatalf("%s welcome: %+v", nick, w)
		}
		return s, w
	}
	a, aw := connect("A", "brawl", "game1")
	b, bw := connect("B", "brawl", "game1")
	if ev := receiveGameplayEvent(t, a); ev.Type != "player_joined" || ev.PlayerID != bw.PlayerID {
		t.Fatalf("join B: %+v", ev)
	}
	c, cw := connect("C", "brawl", "game1")
	if len(cw.Players) != 3 {
		t.Fatalf("expected 3 player roster, got %+v", cw.Players)
	}
	for _, s := range []*testSocket{a, b} {
		if ev := receiveGameplayEvent(t, s); ev.Type != "player_joined" || ev.PlayerID != cw.PlayerID {
			t.Fatalf("join C: %+v", ev)
		}
	}
	d, dw := connect("D", "brawl", "game2")
	if len(dw.Players) != 1 {
		t.Fatalf("cross-room roster: %+v", dw.Players)
	}
	a.send(t, clientMessage{Type: "command", Tick: 1, Sequence: 1, Payload: json.RawMessage(`{"move":1}`)})
	for _, s := range []*testSocket{b, c} {
		v := receiveGameplayEvent(t, s)
		if v.Type != "command" || v.PlayerID != aw.PlayerID || v.Tick != 1 {
			t.Fatalf("bad room command: %+v", v)
		}
	}
	if ev := d.receive(t); ev.Type != "coordinator_changed" {
		t.Fatalf("expected coordinator notice: %+v", ev)
	}
	_ = d.c.SetReadDeadline(time.Now().Add(80 * time.Millisecond))
	header := make([]byte, 2)
	if _, err := io.ReadFull(d.r, header); err == nil {
		t.Fatal("unrelated room received a command")
	}
	if hub.size() != 4 {
		t.Fatalf("expected 4 players, got %d", hub.size())
	}
}

func TestLegacyAndExplicitRoomsAreSeparate(t *testing.T) {
	hub := newRelay()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", hub.websocket)
	server := httptest.NewServer(mux)
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")
	legacy := connectTest(t, host)
	legacy.send(t, clientMessage{Type: "hello", Version: 1, Nickname: "Old client"})
	lw := legacy.receive(t)
	modern := connectTest(t, host)
	modern.send(t, clientMessage{Type: "hello", Version: 1, Nickname: "New client", Mode: "brawl", Room: "global"})
	mw := modern.receive(t)
	if len(lw.Players) != 1 || len(mw.Players) != 1 || mw.Mode != "brawl" || mw.Room != "global" {
		t.Fatalf("wrong room membership: legacy=%+v modern=%+v", lw, mw)
	}
}

func TestResolveRoomValidation(t *testing.T) {
	tests := []struct {
		mode, room string
		ok         bool
	}{
		{"", "", true}, {"brawl", "global", true}, {"battle_royale", "match-42", true},
		{"", "global", false}, {"brawl", "", false}, {"legacy", "global", false},
		{"brawl", "../bad", false}, {"brawl", "space bad", false},
		{"brawl", "", false},
	}
	for _, tt := range tests {
		_, _, err := resolveRoom(tt.mode, tt.room)
		if (err == nil) != tt.ok {
			t.Fatalf("resolveRoom(%q,%q) err=%v", tt.mode, tt.room, err)
		}
	}
}

func TestConcurrentRoomOperations(t *testing.T) {
	hub := newRelay()
	var wg sync.WaitGroup
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := &peer{id: hub.nextID.Add(1), nickname: "load", send: make(chan []byte, maxOutboundQueue)}
			room := fmt.Sprintf("match%d", i%8)
			if err := hub.add(p, "brawl", room); err != nil {
				t.Errorf("add: %v", err)
				return
			}
			hub.remove(p)
		}(i)
	}
	wg.Wait()
	if hub.size() != 0 {
		t.Fatalf("peers leaked: %d", hub.size())
	}
	hub.mu.Lock()
	n := len(hub.rooms)
	hub.mu.Unlock()
	if n != 0 {
		t.Fatalf("rooms leaked: %d", n)
	}
}

// In-memory benchmark of the critical fan-out path. It deliberately excludes
// socket/network latency and does not claim production capacity on Render.
func BenchmarkRoomBroadcast16(b *testing.B) {
	room := &relayRoom{peers: make(map[uint64]*peer)}
	for i := 0; i < 16; i++ {
		id := uint64(i + 1)
		room.peers[id] = &peer{id: id, send: make(chan []byte, 1)}
	}
	msg := serverMessage{Type: "command", PlayerID: 1, Tick: 10, Sequence: 1, Payload: json.RawMessage(`{"x":1,"y":2}`)}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		room.broadcastLocked(msg, 1)
		for id, p := range room.peers {
			if id != 1 {
				<-p.send
			}
		}
	}
}
