package object

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	payload := []byte("hello world")
	enc := Encode(KindBlob, payload)
	kind, got, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	if kind != KindBlob || !bytes.Equal(got, payload) {
		t.Fatalf("round trip failed: got kind=%q payload=%q", kind, got)
	}
}

func TestSameContentSameId(t *testing.T) {
	a := Hash(Encode(KindBlob, []byte("x")))
	b := Hash(Encode(KindBlob, []byte("x")))
	c := Hash(Encode(KindTree, []byte("x")))
	if a != b {
		t.Fatalf("same content, different ids: %s vs %s", a.Short(), b.Short())
	}
	if a == c {
		t.Fatalf("same content but different kind, same id: %s vs %s", a.Short(), c.Short())
	}
}

func TestStorePutGetDedup(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id1, err := s.Put(KindBlob, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	id2, err := s.Put(KindBlob, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("dedup failed: %s vs %s", id1.Short(), id2.Short())
	}
	payload, err := s.GetKind(id1, KindBlob)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "value" {
		t.Fatalf("unexpected payload: %q", payload)
	}
	if _, err := s.GetKind(id1, KindTree); err == nil {
		t.Fatalf("expected error for wrong kind")
	}
}

func TestStoreDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.Put(KindBlob, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	h := id.String()
	path := filepath.Join(dir, h[:2], h[2:])
	if err := os.WriteFile(path, []byte("blob 5\x00wrong"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Get(id); err == nil {
		t.Fatal("expected corruption to be detected")
	}
}
