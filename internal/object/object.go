package object

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
)

// Kind is the type tag stored in every object header.
type Kind string

const (
	KindBlob   Kind = "blob"
	KindTree   Kind = "tree"
	KindCommit Kind = "commit"
)

// ID is the SHA-256 of an object's canonical encoding.
type ID [sha256.Size]byte

func (id ID) String() string { return hex.EncodeToString(id[:]) }

// Short returns the first 12 hex characters, for human-readabl output.
func (id ID) Short() string { return hex.EncodeToString(id[:12]) }

func (id ID) IsZero() bool { return id == ID{} }

var ErrMalformed = errors.New("object: malformed encoding")

// ParseID accept a full 64-character hex ID.
func ParseID(s string) (ID, error) {
	var id ID
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, fmt.Errorf("object: bad id %q: %w", s, err)
	}
	if len(b) != sha256.Size {
		return id, fmt.Errorf("object: bad id length %q: %d", s, len(b))
	}
	copy(id[:], b)
	return id, nil
}

// Encode produces the canonical byte sequence that is hashed and stored.
func Encode(k Kind, payload []byte) []byte {
	header := string(k) + " " + strconv.Itoa(len(payload)) + "\x00"
	buf := make([]byte, 0, len(header)+len(payload))
	buf = append(buf, header...)
	buf = append(buf, payload...)
	return buf
}

// Hash returns the ID of an already-encoded object.
func Hash(encoded []byte) ID { return sha256.Sum256(encoded) }

// Decode splits a canonical encoding back into kind and payload.
func Decode(encoded []byte) (Kind, []byte, error) {
	nul := bytes.IndexByte(encoded, 0)
	if nul < 0 {
		return "", nil, ErrMalformed
	}
	head := string(encoded[:nul])
	sp := bytes.IndexByte([]byte(head), ' ')
	if sp < 0 {
		return "", nil, ErrMalformed
	}
	kind := Kind(head[:sp])
	n, err := strconv.Atoi(head[sp+1:])
	if err != nil {
		return "", nil, ErrMalformed
	}
	payload := encoded[nul+1:]
	if n != len(payload) {
		return "", nil, fmt.Errorf("%w: header says %d bytes, got %d", ErrMalformed, n, len(payload))
	}
	switch kind {
	case KindBlob, KindTree, KindCommit:
	default:
		return "", nil, fmt.Errorf("%w: unknown kind %q", ErrMalformed, kind)
	}
	return kind, payload, nil
}
