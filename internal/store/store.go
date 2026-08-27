package store

import (
	"sort"
	"sync"
)

// Op identifies the king of a Mutation.
type Op byte

const (
	OpSet Op = 1
	OpDel Op = 2
)

// Mutation is a single absolute change to the keyspace. It is absolute, not
// relative: applying the same mutation twice produces the same result. The WAL
// relies on that property during replay.
type Mutation struct {
	Op    Op
	Key   string
	Value string // empty for OpDel
}

/** Store is a concurency-safe map[string]string
 *
 * Invariant : every exported method either takes s.mu (Mutex attribute)
 * for reading or writing. No caller ever receives a reference to the internal map.
 */
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func New() *Store {
	return &Store{data: make(map[string]string)}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Del removes key and reports whether it existed.
func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	return ok
}

func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.data[key]
	return ok
}

// Keys returns every key in sorted order. Sorting is not free, but it makes
// the command output deterministic, whichc makes tests much easier to write.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *Store) ApplyBatch(muts []Mutation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range muts {
		switch m.Op {
		case OpSet:
			s.data[m.Key] = m.Value
		case OpDel:
			delete(s.data, m.Key)
		}
	}
}

// Snapshot returns a copy of the whole keyspace.
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// Replace swaps the entire keyspace. Used by CHECKOUT and by startup recovery.
func (s *Store) Replace(data map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = data
}
