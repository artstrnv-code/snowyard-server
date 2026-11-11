package main

// Minimal RFC 6455 WebSocket transport for the Snowyard desktop relay.
// Text and binary messages. Supports masked client frames, fragmentation, ping/pong,
// close, limits and deadlines. No third-party dependencies.
import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const maxMessageSize = 32 * 1024
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

var errWSClosed = errors.New("websocket closed")

type wsConn struct {
	conn          net.Conn
	reader        *bufio.Reader
	mu            sync.Mutex // Only one writer may use the connection at a time.
	lastReadNs    atomic.Int64
	lastWriteNs   atomic.Int64
	lastPongNs    atomic.Int64
	pendingPingNs atomic.Int64
	pingRTTMs     atomic.Int64
}

func upgradeWebSocket(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	if r.Method != http.MethodGet || !headerHasToken(r.Header, "Connection", "Upgrade") || !headerHasToken(r.Header, "Upgrade", "websocket") || r.Header.Get("Sec-WebSocket-Version") != "13" {
		http.Error(w, "WebSocket upgrade required (RFC 6455)", http.StatusBadRequest)
		return nil, errors.New("invalid upgrade request")
	}
	// Browser requests must be same-origin. Desktop clients normally omit Origin.
	if origin := r.Header.Get("Origin"); origin != "" && !sameOrigin(origin, r.Host) {
		http.Error(w, "origin denied", http.StatusForbidden)
		return nil, errors.New("origin denied")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	raw, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(raw) != 16 {
		http.Error(w, "invalid WebSocket key", http.StatusBadRequest)
		return nil, errors.New("invalid websocket key")
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijacking unavailable", http.StatusInternalServerError)
		return nil, errors.New("no hijacker")
	}
	c, buf, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}
	hash := sha1.Sum([]byte(key + wsGUID))
	accept := base64.StdEncoding.EncodeToString(hash[:])
	_, err = fmt.Fprintf(buf, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", accept)
	if err == nil {
		err = buf.Flush()
	}
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	socket := &wsConn{conn: c, reader: buf.Reader}
	now := time.Now().UnixNano()
	socket.lastReadNs.Store(now)
	socket.lastWriteNs.Store(now)
	socket.lastPongNs.Store(now)
	socket.pingRTTMs.Store(-1)
	return socket, nil
}

func sameOrigin(origin, host string) bool {
	// Only browser Origins are checked; no client-provided hostname is trusted for authentication.
	// Host may be the public Render hostname, forwarded by Render.
	return origin == "https://"+host || origin == "http://"+host
}
func headerHasToken(h http.Header, name, target string) bool {
	for _, v := range h.Values(name) {
		for _, tok := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(tok), target) {
				return true
			}
		}
	}
	return false
}

func (w *wsConn) readMessage() ([]byte, error) {
	return w.readMessageWithTimeout(75 * time.Second)
}

// New connections must identify themselves promptly so idle upgraded sockets
// do not consume the free server's file descriptors for a full minute.
func (w *wsConn) readMessageWithTimeout(timeout time.Duration) ([]byte, error) {
	var output []byte
	var fragmented bool
	for {
		_ = w.conn.SetReadDeadline(time.Now().Add(timeout))
		header := make([]byte, 2)
		if _, err := io.ReadFull(w.reader, header); err != nil {
			return nil, err
		}
		if header[0]&0x70 != 0 {
			return nil, errors.New("unsupported websocket extension")
		}
		fin := header[0]&0x80 != 0
		op := header[0] & 0x0f
		if header[1]&0x80 == 0 {
			return nil, errors.New("unmasked client frame")
		}
		n := uint64(header[1] & 0x7f)
		switch n {
		case 126:
			var x uint16
			if err := binary.Read(w.reader, binary.BigEndian, &x); err != nil {
				return nil, err
			}
			n = uint64(x)
		case 127:
			if err := binary.Read(w.reader, binary.BigEndian, &n); err != nil {
				return nil, err
			}
			if n>>63 != 0 {
				return nil, errors.New("invalid payload length")
			}
		}
		if n > maxMessageSize || uint64(len(output))+n > maxMessageSize {
			return nil, errors.New("message too large")
		}
		if op >= 8 && (!fin || n > 125) {
			return nil, errors.New("invalid control frame")
		}
		mask := make([]byte, 4)
		if _, err := io.ReadFull(w.reader, mask); err != nil {
			return nil, err
		}
		payload := make([]byte, int(n))
		if _, err := io.ReadFull(w.reader, payload); err != nil {
			return nil, err
		}
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
		w.lastReadNs.Store(time.Now().UnixNano())
		switch op {
		case 0x8:
			_ = w.writeFrame(0x8, payload)
			return nil, errWSClosed
		case 0x9:
			if err := w.writeFrame(0xA, payload); err != nil {
				return nil, err
			}
			continue
		case 0xA:
			// Pong echoes our exact 8-byte timestamp. A delayed response to
			// an older ping must not acknowledge a newer outstanding ping.
			if len(payload) == 8 {
				sent := int64(binary.BigEndian.Uint64(payload))
				if sent > 0 && w.pendingPingNs.CompareAndSwap(sent, 0) {
					now := time.Now().UnixNano()
					w.lastPongNs.Store(now)
					w.pingRTTMs.Store((now - sent) / int64(time.Millisecond))
				}
			}
			continue
		case 0x1, 0x2:
			if fragmented {
				return nil, errors.New("unexpected text frame")
			}
			output = append(output, payload...)
			if fin {
				return output, nil
			}
			fragmented = true
		case 0x0:
			if !fragmented {
				return nil, errors.New("unexpected continuation")
			}
			output = append(output, payload...)
			if fin {
				return output, nil
			}
		default:
			return nil, errors.New("unsupported websocket data opcode")
		}
	}
}

func (w *wsConn) writeFrame(op byte, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	hdr := []byte{0x80 | op}
	switch {
	case len(data) < 126:
		hdr = append(hdr, byte(len(data)))
	case len(data) <= 65535:
		hdr = append(hdr, 126, byte(len(data)>>8), byte(len(data)))
	default:
		hdr = append(hdr, 127, 0, 0, 0, 0, byte(len(data)>>24), byte(len(data)>>16), byte(len(data)>>8), byte(len(data)))
	}
	// A single write avoids a split header/payload across kernel calls.
	frame := make([]byte, len(hdr)+len(data))
	copy(frame, hdr)
	copy(frame[len(hdr):], data)
	for len(frame) > 0 {
		n, err := w.conn.Write(frame)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		frame = frame[n:]
	}
	w.lastWriteNs.Store(time.Now().UnixNano())
	return nil
}

// Issue a single probe only when no earlier Ping is outstanding.
func (w *wsConn) sendHeartbeat() error {
	sent := time.Now().UnixNano()
	if !w.pendingPingNs.CompareAndSwap(0, sent) {
		return nil
	}
	var payload [8]byte
	binary.BigEndian.PutUint64(payload[:], uint64(sent))
	if err := w.writeFrame(0x9, payload[:]); err != nil {
		w.pendingPingNs.CompareAndSwap(sent, 0)
		return err
	}
	return nil
}
func (w *wsConn) healthSummary(now time.Time) string {
	age := func(ns int64) int64 {
		if ns <= 0 {
			return -1
		}
		return (now.UnixNano() - ns) / int64(time.Millisecond)
	}
	return fmt.Sprintf("rxAgeMs=%d txAgeMs=%d pongAgeMs=%d pingOutstandingMs=%d rttMs=%d",
		age(w.lastReadNs.Load()), age(w.lastWriteNs.Load()), age(w.lastPongNs.Load()),
		age(w.pendingPingNs.Load()), w.pingRTTMs.Load())
}

// A normal WebSocket close frame is best effort; broken connections are closed directly.
func (w *wsConn) close() { _ = w.conn.Close() }
