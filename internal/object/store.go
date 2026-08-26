package object

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/CM-exe/staash/internal/fsutil"
)

// ErrNotFound is returned by Store.Get for an unknown ID.
var ErrNotFound = errors.New("object: not found")

// Store is a filesystem-backed content-addressed store.
//
// Layout:
//
// <root>/ab/cdef...   the object whose ID starts with "ab"
// <root>/tmp/         staging area for in-progress writes
//
// Invariant (relied on everywhere else): once a file exists under <root>/ab/,
// its contents are complete and never change. Writers stage into <root>/tmp
// and rename into place, and rename is atomic within a filesystem.
type Store struct {
	root string
}

func NewStore(root string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(root, "tmp"), 0o755); err != nil {
		return nil, err
	}
	return &Store{root: root}, nil
}
func (s *Store) path(id ID) string {
	h := id.String()
	return filepath.Join(s.root, h[:2], h[2:])
}
func (s *Store) Has(id ID) bool {
	_, err := os.Stat(s.path(id))
	return err == nil
}

// Put stores payload under kind and returns its ID. Writing an object that
// already exists is a no-op: identical content always has an identical ID,
// which gives us deduplication for free.
func (s *Store) Put(kind Kind, payload []byte) (ID, error) {
	encoded := Encode(kind, payload)
	id := Hash(encoded)
	if s.Has(id) {
		return id, nil
	}
	tmp, err := os.CreateTemp(filepath.Join(s.root, "tmp"), "obj-*")
	if err != nil {
		return id, err
	}
	tmpName := tmp.Name()
	defer func() {
		tmp.Close()
		os.Remove(tmpName)
	}()
	if _, err := tmp.Write(encoded); err != nil {
		return id, err
	}
	if err := tmp.Sync(); err != nil {
		return id, err
	}
	if err := tmp.Close(); err != nil {
		return id, err
	}
	dst := s.path(id)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return id, err
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return id, err
	}
	return id, fsutil.SyncDir(filepath.Dir(dst))
}

// Get reads an object and verifies that its content still hashes to its name.
// The check is cheap compared to the cost of silently serving corrupted data.
func (s *Store) Get(id ID) (Kind, []byte, error) {
	raw, err := os.ReadFile(s.path(id))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil, fmt.Errorf("%w: %s", ErrNotFound, id.Short())
		}
		return "", nil, err
	}
	if got := Hash(raw); got != id {
		return "", nil, fmt.Errorf("object: corrupted %s (content hashes to %s)", id.Short(), got.Short())
	}
	return Decode(raw)
}

// GetKind is Get plus an assertion on the object type.
func (s *Store) GetKind(id ID, want Kind) ([]byte, error) {
	k, payload, err := s.Get(id)
	if err != nil {
		return nil, err
	}
	if k != want {
		return nil, fmt.Errorf("object: %s is a %s, wanted %s", id.Short(), k, want)
	}
	return payload, nil
}

// CleanTmp removes staging files left behind by a crash. Safe to call at
// startup: a temp file is never referenced by anything.
func (s *Store) CleanTmp() error {
	entries, err := os.ReadDir(filepath.Join(s.root, "tmp"))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.Remove(filepath.Join(s.root, "tmp", e.Name())); err != nil {
			return err
		}
	}
	return nil
}
