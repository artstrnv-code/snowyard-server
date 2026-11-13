package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestSYB2BatchBridge(t *testing.T) {
	sample := []byte{0xB2, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77}
	var wire bytes.Buffer
	wire.WriteByte(clientBatchMarker)
	putVar(&wire, 7)
	wire.WriteByte(2)
	for _, v := range []uint64{254, 255} {
		putVar(&wire, v)
		putVar(&wire, v-253)
		putVar(&wire, uint64(len(sample)))
		wire.Write(sample)
	}
	msg, err := decodeClientBatch(wire.Bytes())
	if err != nil || len(msg.Commands) != 2 || msg.Generation != 7 || msg.Commands[1].Tick != 255 {
		t.Fatalf("decode: %+v %v", msg, err)
	}
	outbound, err := encodeServerBatch(serverMessage{Type: "command_batch", PlayerID: 9, Generation: msg.Generation, Commands: msg.Commands})
	if err != nil || outbound[0] != serverBatchMarker {
		t.Fatalf("encode: %v", err)
	}
	// The hot path must keep bytes intact without allocating JSON/Base64.
	if len(msg.Commands[0].Payload) != 0 || !bytes.Equal(msg.Commands[0].BinaryPayload, sample) {
		t.Fatal("binary input was converted or corrupted")
	}
	// Replay compatibility remains available on demand.
	var parsed struct {
		B string `json:"b"`
	}
	if err := json.Unmarshal(wrapBinaryCommand(msg.Commands[0].BinaryPayload), &parsed); err != nil {
		t.Fatal(err)
	}
	recovered, err := base64.StdEncoding.DecodeString(parsed.B)
	if err != nil || !bytes.Equal(recovered, sample) {
		t.Fatal("payload corrupted by replay bridge")
	}
}
func TestSYB2RejectsMalformed(t *testing.T) {
	cases := [][]byte{{}, {clientBatchMarker}, {clientBatchMarker, 1, 33}, {clientBatchMarker, 1, 1, 1, 1, 9, 0xB2}}
	for _, payload := range cases {
		if _, err := decodeClientBatch(payload); err == nil {
			t.Fatalf("accepted malformed %x", payload)
		}
	}
}

// SYB3 travels through the relay unchanged, including its compact throw body.
func TestSYB3BatchBridge(t *testing.T) {
	sample := []byte{0xB5, 0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34, 0x56, 0x78, 0x90, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	var wire bytes.Buffer
	wire.WriteByte(clientBatchMarker)
	putVar(&wire, 2)
	wire.WriteByte(1)
	putVar(&wire, 17)
	putVar(&wire, 42)
	putVar(&wire, uint64(len(sample)))
	wire.Write(sample)
	msg, err := decodeClientBatch(wire.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	back, err := encodeServerBatch(serverMessage{Type: "command_batch", PlayerID: 1, Generation: msg.Generation, Commands: msg.Commands})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(back[len(back)-len(sample):], sample) {
		t.Fatal("SYB3 payload changed during relay")
	}
}
