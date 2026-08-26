package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/CM-exe/staash/internal/store"
)

func openTemp(t *testing.T, dir string, sync bool) (*WAL, [][]store.Mutation) {
	t.Helper()
	w, batches, err := Open(filepath.Join(dir, "wal.log"), sync)
	if err != nil {
		t.Fatal(err)
	}
	return w, batches
}

func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	w, batches := openTemp(t, dir, true)
	if len(batches) != 0 {
		t.Fatalf("fresh log should be empty, got %d batches", len(batches))
	}
	if err := w.Append([]store.Mutation{{Op: store.OpSet, Key: "a", Value: "1"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]store.Mutation{
		{Op: store.OpSet, Key: "b", Value: "2"},
		{Op: store.OpDel, Key: "a"},
	}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2, batches := openTemp(t, dir, true)
	defer w2.Close()
	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}
	if len(batches[1]) != 2 || batches[1][1].Op != store.OpDel || batches[1][1].Key != "a" {
		t.Fatalf("expected second batch to be Del(a), got %+v", batches[1])
	}
}

func TestResetEmptiesLog(t *testing.T) {
	dir := t.TempDir()
	w, _ := openTemp(t, dir, true)
	_ = w.Append([]store.Mutation{{Op: store.OpSet, Key: "a", Value: "1"}})
	if w.Size() == 0 {
		t.Fatal("expected non-zero size")
	}
	if err := w.Reset(); err != nil {
		t.Fatal(err)
	}
	w.Close()

	_, batches := openTemp(t, dir, true)
	if len(batches) != 0 {
		t.Fatalf("expected empty log after reset, got %d batches", len(batches))
	}
}

// A crach in the middle of Append leaves a partial record. Recovery must drop
// it and keep everything before it.
func TestTornFinalRecordIsDropped(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, _, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Append([]store.Mutation{{Op: store.OpSet, Key: "good", Value: "1"}})
	_ = w.Append([]store.Mutation{{Op: store.OpSet, Key: "torn", Value: "2"}})
	good := w.Size()
	w.Close()

	// Chop the last record in half.
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, full[:len(full)-4], 0o644); err != nil {
		t.Fatal(err)
	}

	w2, batches, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if len(batches) != 1 || batches[0][0].Key != "good" {
		t.Fatalf("expected only the first batch to be recovered, got %+v", batches)
	}
	if w2.Size() >= good {
		t.Fatalf("log was not truncated: size %d", w2.Size())
	}

	// the log must still be usable after recovery.
	if err := w2.Append([]store.Mutation{{Op: store.OpSet, Key: "after", Value: "3"}}); err != nil {
		t.Fatal(err)
	}
}

func TestChecksumCatchesBitFlip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, _, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Append([]store.Mutation{{Op: store.OpSet, Key: "key", Value: "value"}})
	w.Close()
	raw, _ := os.ReadFile(path)
	raw[len(raw)-1] ^= 0xFF // flip bits in the payload
	_ = os.WriteFile(path, raw, 0o644)
	_, batches, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 0 {
		t.Fatalf("corrupt record should have been rejected, got %+v", batches)
	}
}
