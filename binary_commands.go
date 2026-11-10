package main

// SYB2 binary WebSocket envelope. The relay validates the tick envelope but
// deliberately never simulates or re-encodes a client's deterministic input.
// The journal remains JSON-compatible for checkpoint replay.
import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

const clientBatchMarker byte = 0xB3
const serverBatchMarker byte = 0xB4

func putVar(out *bytes.Buffer, v uint64) {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], v)
	out.Write(buf[:n])
}
func getVar(r *bytes.Reader) (uint64, error) {
	v, e := binary.ReadUvarint(r)
	if e != nil {
		return 0, e
	}
	return v, nil
}
func decodeClientBatch(data []byte) (clientMessage, error) {
	bad := clientMessage{}
	if len(data) < 3 || data[0] != clientBatchMarker {
		return bad, errors.New("invalid binary command marker")
	}
	r := bytes.NewReader(data[1:])
	gen, err := getVar(r)
	if err != nil {
		return bad, err
	}
	count, err := r.ReadByte()
	if err != nil || count == 0 || count > 32 {
		return bad, errors.New("invalid binary command count")
	}
	msg := clientMessage{Type: "command_batch", Generation: gen, Commands: make([]batchedCommand, 0, int(count))}
	for i := 0; i < int(count); i++ {
		tick, e := getVar(r)
		if e != nil {
			return bad, e
		}
		seq, e := getVar(r)
		if e != nil {
			return bad, e
		}
		size, e := getVar(r)
		if e != nil {
			return bad, e
		}
		if size == 0 || size > 6144 || uint64(r.Len()) < size {
			return bad, errors.New("invalid binary tick size")
		}
		payload := make([]byte, int(size))
		if _, e = r.Read(payload); e != nil {
			return bad, e
		}
		if payload[0] != 0xB2 && !(len(payload) >= 4 && bytes.Equal(payload[:4], []byte("SYJ1"))) {
			return bad, errors.New("invalid tick encoding")
		}
		body, err := json.Marshal(struct {
			B string `json:"b"`
		}{base64.StdEncoding.EncodeToString(payload)})
		if err != nil {
			return bad, err
		}
		msg.Commands = append(msg.Commands, batchedCommand{Tick: tick, Sequence: seq, Payload: body})
	}
	if r.Len() != 0 {
		return bad, errors.New("trailing binary command data")
	}
	return msg, nil
}
func encodeServerBatch(msg serverMessage) ([]byte, error) {
	if msg.Type != "command_batch" || len(msg.Commands) == 0 || len(msg.Commands) > 32 {
		return nil, errors.New("invalid server batch")
	}
	var out bytes.Buffer
	out.WriteByte(serverBatchMarker)
	putVar(&out, msg.PlayerID)
	putVar(&out, msg.Generation)
	out.WriteByte(byte(len(msg.Commands)))
	for _, c := range msg.Commands {
		var p struct {
			B string `json:"b"`
		}
		if err := json.Unmarshal(c.Payload, &p); err != nil {
			return nil, err
		}
		data, err := base64.StdEncoding.DecodeString(p.B)
		if err != nil {
			return nil, err
		}
		if len(data) == 0 || len(data) > 6144 {
			return nil, fmt.Errorf("invalid compact input length: %d", len(data))
		}
		putVar(&out, c.Tick)
		putVar(&out, c.Sequence)
		putVar(&out, uint64(len(data)))
		out.Write(data)
	}
	return out.Bytes(), nil
}
