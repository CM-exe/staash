package store

import (
	"fmt"
	"sync"
	"testing"
)

func TestStoreBasics(t *testing.T) {
	s := New()

	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected miss on empty store")
	}

	s.Set("a", "1")
	if v, ok := s.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a) = %q, %v; want 1, true", v, ok)
	}

	s.Set("a", "2")
	if v, _ := s.Get("a"); v != "2" {
		t.Fatalf("Get(a) = %q; want 2", v)
	}

	if !s.Del("a") {
		t.Fatal("expected Del(a) to return true")
	}

	if s.Del("a") {
		t.Fatal("expected Del(a) to return false")
	}

	if s.Exists("a") {
		t.Fatal("expected Exists(a) to return false")
	}
}

func TestKeysAreStored(t *testing.T) {
	s := New()
	for _, k := range []string{"c", "a", "b"} {
		s.Set(k, k)
	}

	got := s.Keys()
	want := []string{"a", "b", "c"}

	if len(got) != len(want) {
		t.Fatalf("Keys() = %v; want %v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Keys() = %v; want %v", got, want)
		}
	}
}

// Run with -race: without the mutex this test reliably reports a data race
// and often panics with "concurrent map writes"
func TestConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				k := fmt.Sprintf("k%d", j)
				s.Set(k, fmt.Sprintf("%d-%d", i, j))
				s.Get(k)
				s.Keys()
			}
		}(i)
	}
	wg.Wait()
	if s.Len() != 500 {
		t.Fatalf("Len() = %d; want 500", s.Len())
	}
}
