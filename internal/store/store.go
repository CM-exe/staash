package store

import (
	"sort"
	"sync"
)

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
