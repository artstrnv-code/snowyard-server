package main

import (
	"bufio"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// Verify that a matching, masked Pong clears the pending probe and updates RTT.
// The following text frame makes readMessage finish; Pong itself is a control frame.
func TestHeartbeatPongAcknowledgement0312(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	ws := &wsConn{conn: server, reader: bufio.NewReader(server)}
	now := time.Now().UnixNano()
	ws.pendingPingNs.Store(now)
	ws.lastPongNs.Store(now - int64(time.Second))
	done := make(chan error, 1)
	go func() { _, err := ws.readMessageWithTimeout(2 * time.Second); done <- err }()
	writeMasked := func(op byte, payload []byte) {
		t.Helper()
		var frame []byte
		frame = append(frame, 0x80|op, 0x80|byte(len(payload)), 1, 2, 3, 4)
		for i, b := range payload {
			frame = append(frame, b^byte(i%4+1))
		}
		if _, err := client.Write(frame); err != nil {
			t.Fatal(err)
		}
	}
	pong := make([]byte, 8)
	binary.BigEndian.PutUint64(pong, uint64(now))
	writeMasked(0xA, pong)
	writeMasked(0x1, []byte("x"))
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := ws.pendingPingNs.Load(); got != 0 {
		t.Fatalf("pending ping not cleared: %d", got)
	}
	if got := ws.pingRTTMs.Load(); got < 0 {
		t.Fatalf("RTT invalid: %d", got)
	}
	if got := ws.lastReadNs.Load(); got == 0 {
		t.Fatal("frame read timestamp not updated")
	}
}

func TestHeartbeatRejectsStalePong0312(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	ws := &wsConn{conn: server, reader: bufio.NewReader(server)}
	now := time.Now().UnixNano()
	ws.pendingPingNs.Store(now)
	done := make(chan error, 1)
	go func() { _, err := ws.readMessageWithTimeout(2 * time.Second); done <- err }()
	write := func(op byte, payload []byte) {
		frame := []byte{0x80 | op, 0x80 | byte(len(payload)), 0, 0, 0, 0}
		frame = append(frame, payload...)
		if _, err := client.Write(frame); err != nil {
			t.Error(err)
		}
	}
	pong := make([]byte, 8)
	binary.BigEndian.PutUint64(pong, uint64(now-100))
	write(0xA, pong)
	write(0x1, []byte("x"))
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := ws.pendingPingNs.Load(); got != now {
		t.Fatalf("stale pong acknowledged: %d", got)
	}
}
