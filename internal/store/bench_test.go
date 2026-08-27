package store

import (
	"testing"
)

func BenchmarkSet(b *testing.B) {
	s := New()
	b.ReportAllocs()
	for b.Loop() {
		s.Set("key", "value")
	}
}

func BenchmarkGet(b *testing.B) {
	s := New()
	s.Set("key", "value")
	b.ReportAllocs()
	for b.Loop() {
		s.Get("key")
	}
}

// RunParallel spreads iterations across GOMAXPROCS goroutines, which is the
// only way to see whether RWMutex read parallelism is actually helping.
func BenchmarkGetParallel(b *testing.B) {
	s := New()
	s.Set("key", "value")
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get("key")
		}
	})
}
