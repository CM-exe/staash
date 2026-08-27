package object

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// EntryKind distinguishes a subtree from a value.
type EntryKind byte

const (
	EntryBlob EntryKind = 1
	EntryTree EntryKind = 2
)

// Entry is one row of a tree object.
type Entry struct {
	Name string
	Kind EntryKind
	ID   ID
}

// Tree is an immutable, sorted list of entries.
//
// Invariant: entries are sorted by Name and names are unique. Canonical
// ordering is what makes the encoding deterministic, and deterministic
// encoding is what makes the content addressing useful: two identical keyspaces
// must produce the same tree ID.
type Tree struct {
	Entries []Entry
}

// NewTree copies and sorts entries.
func NewTree(entries []Entry) *Tree {
	cp := make([]Entry, len(entries))
	copy(cp, entries)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Name < cp[j].Name })
	return &Tree{Entries: cp}
}

// TreeFromMap builds a tree from a name -> entry map.
func TreeFromMap(m map[string]Entry) *Tree {
	entries := make([]Entry, 0, len(m))
	for _, e := range m {
		entries = append(entries, e)
	}
	return NewTree(entries)
}

// Map returns the entries keyed by name.
func (t *Tree) Map() map[string]Entry {
	m := make(map[string]Entry, len(t.Entries))
	for _, e := range t.Entries {
		m[e.Name] = e
	}
	return m
}

// Encode serialises the tree:
//
//	repeated: kind(1) nameLen(uvarint) name(namelen) id(32)
func (t *Tree) Encode() []byte {
	buf := make([]byte, 0, len(t.Entries)*48)
	var scratch [binary.MaxVarintLen64]byte
	for _, e := range t.Entries {
		buf = append(buf, byte(e.Kind))
		n := binary.PutUvarint(scratch[:], uint64(len(e.Name)))
		buf = append(buf, scratch[:n]...)
		buf = append(buf, e.Name...)
		buf = append(buf, e.ID[:]...)
	}
	return buf
}

// DecodeTree is the inverse of Encode.
func DecodeTree(payload []byte) (*Tree, error) {
	var entries []Entry
	for len(payload) > 0 {
		kind := EntryKind(payload[0])
		if kind != EntryBlob && kind != EntryTree {
			return nil, fmt.Errorf("%w: bad entry kind %q", ErrMalformed, kind)
		}
		payload = payload[1:]
		nameLen, n := binary.Uvarint(payload)
		if n <= 0 {
			return nil, ErrMalformed
		}
		payload = payload[n:]
		if uint64(len(payload)) < nameLen+uint64(len(ID{})) {
			return nil, ErrMalformed
		}
		name := string(payload[:nameLen])
		payload = payload[nameLen:]
		var id ID
		copy(id[:], payload[:len(id)])
		payload = payload[len(id):]
		entries = append(entries, Entry{Name: name, Kind: kind, ID: id})
	}
	return &Tree{Entries: entries}, nil
}
