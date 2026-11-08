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
