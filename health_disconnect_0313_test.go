package main

import (
	"strings"
	"testing"
)

// Distinguish transport loss from queue overload without publishing peer data.
func TestDisconnectHealthCounters0313(t *testing.T) {
	var counts disconnectCounters
	samples := []string{
		"frame read: EOF",
		"frame write: write tcp: broken pipe",
		"pong timeout: 50s",
		"outbound queue full during control message",
		"frame read: connection reset by peer",
		"normal shutdown",
	}
	for _, reason := range samples {
		counts.record(reason)
	}
	got := counts.snapshot()
	want := map[string]uint64{
		"total": 6, "read_eof": 1, "write_error": 1,
		"pong_timeout": 1, "queue_full": 1, "read_error": 1, "other": 1,
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("%s = %d, want %d", key, got[key], value)
		}
	}
	if counts.lastAtUnix.Load() <= 0 {
		t.Fatal("no timestamp for last disconnect")
	}
	for key := range got {
		if strings.Contains(key, "room") || strings.Contains(key, "nickname") {
			t.Fatalf("unexpected identifiable field: %s", key)
		}
	}
}

// stopWithReason's sync.Once makes it safe for writer and reader to fail together.
func TestDisconnectRecordedOnce0313(t *testing.T) {
	var counts disconnectCounters
	p := &peer{onDisconnect: counts.record}
	p.stopWithReason("frame read: EOF")
	p.stopWithReason("frame write: broken pipe")
	if got := counts.total.Load(); got != 1 {
		t.Fatalf("counted %d disconnects for one peer", got)
	}
}
