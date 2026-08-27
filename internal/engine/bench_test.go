package engine

import (
	"fmt"
	"testing"

	"github.com/CM-exe/staash/internal/store"
)

func benchEngine(b *testing.B, sync bool) *Engine {
	b.Helper()
	e, err := Open(Options{Dir: b.TempDir(), SyncWAL: sync})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { e.Close() })
	return e
}

func BenchmarkEngineSetNoSync(b *testing.B) {
	e := benchEngine(b, false)
	b.ReportAllocs()

	for b.Loop() {
		if err := e.Set("key", "value"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCommit measures the cost of a commit that touches one key in a
// database of `size` keys. If tree sharing works, the numbers should be
// roughly flat as size grows.
func BenchmarkCommit(b *testing.B) {
	for _, size := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("db=%d", size), func(b *testing.B) {
			e := benchEngine(b, false)
			muts := make([]store.Mutation, size)
			for i := range muts {
				muts[i] = store.Mutation{Op: store.OpSet, Key: fmt.Sprintf("key%06d", i), Value: fmt.Sprintf("v%d", i)}
			}
			if err := e.Apply(muts); err != nil {
				b.Fatal(err)
			}
			if _, err := e.Commit("seed"); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := e.Set("key000000", fmt.Sprintf("v%d", i)); err != nil {
					b.Fatal(err)
				}
				if _, err := e.Commit("bench"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkEngineSetSync(b *testing.B) {
	e := benchEngine(b, true)
	b.ReportAllocs()

	for b.Loop() {
		if err := e.Set("key", "value"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEngineGet(b *testing.B) {
	e := benchEngine(b, false)
	_ = e.Set("key", "value")
	b.ReportAllocs()

	for b.Loop() {
		e.Get("key")
	}
}
func BenchmarkEngineDel(b *testing.B) {
	e := benchEngine(b, false)
	b.ReportAllocs()

	for b.Loop() {
		if _, err := e.Del("key"); err != nil {
			b.Fatal(err)
		}
	}
}

// One WAL record and one lock acquisition for 100 keys: this is the
// measurement that justifies transactions as a performance feature.
func BenchmarkEngineBatch100(b *testing.B) {
	e := benchEngine(b, false)
	muts := make([]store.Mutation, 100)
	for i := range muts {
		muts[i] = store.Mutation{Op: store.OpSet, Key: fmt.Sprintf("k%d", i), Value: "v"}
	}
	b.ReportAllocs()

	for b.Loop() {
		if err := e.Apply(muts); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkMaterialize(b *testing.B) {
	e := benchEngine(b, false)
	muts := make([]store.Mutation, 5000)
	for i := range muts {
		muts[i] = store.Mutation{Op: store.OpSet, Key: fmt.Sprintf("key%06d", i), Value: "value"}
	}
	if err := e.Apply(muts); err != nil {
		b.Fatal(err)
	}
	id, err := e.Commit("seed")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()

	for b.Loop() {
		if _, err := e.materialize(id); err != nil {
			b.Fatal(err)
		}
	}
}
