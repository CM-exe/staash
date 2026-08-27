package wal

import (
	"path/filepath"
	"testing"

	"github.com/CM-exe/staash/internal/store"
)

func BenchmarkAppendSync(b *testing.B) {
	w, _, err := Open(filepath.Join(b.TempDir(), "wal.log"), true)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	muts := []store.Mutation{{Op: store.OpSet, Key: "key", Value: "value"}}

	for b.Loop() {
		if err := w.Append(muts); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendNoSync(b *testing.B) {
	w, _, err := Open(filepath.Join(b.TempDir(), "wal.log"), false)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	muts := []store.Mutation{{Op: store.OpSet, Key: "key", Value: "value"}}

	for b.Loop() {
		if err := w.Append(muts); err != nil {
			b.Fatal(err)
		}
	}
}
