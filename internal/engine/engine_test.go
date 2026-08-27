package engine

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/CM-exe/staash/internal/object"
)

func openTestEngine(t *testing.T, dir string) *Engine {
	t.Helper()
	e, err := Open(Options{Dir: dir, SyncWAL: false})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}
func mustSet(t *testing.T, e *Engine, k, v string) {
	t.Helper()
	if err := e.Set(k, v); err != nil {
		t.Fatal(err)
	}
}
func mustCommit(t *testing.T, e *Engine, msg string) object.ID {
	t.Helper()
	id, err := e.Commit(msg)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func mustGet(t *testing.T, e *Engine, k string) string {
	t.Helper()
	v, ok := e.Get(k)
	if !ok {
		t.Fatalf("key %q missing", k)
	}
	return v
}

func TestCommitAndReopen(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory sync is not supported on Windows")
	}
	dir := t.TempDir()
	e := openTestEngine(t, dir)
	mustSet(t, e, "name", "alice")
	mustSet(t, e, "city", "berlin")
	id := mustCommit(t, e, "initial")
	if e.DirtyCount() != 0 {
		t.Fatal("commit should clear the dirty set")
	}
	e.Close()
	e2 := openTestEngine(t, dir)
	if got := mustGet(t, e2, "name"); got != "alice" {
		t.Fatalf("name = %q", got)
	}
	_, headID, ok, err := e2.HeadInfo()
	if err != nil || !ok || headID != id {
		t.Fatalf("HeadInfo = %v %v %v", headID, ok, err)
	}
}
func TestHistoryReconstruction(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory sync is not supported on Windows")
	}
	e := openTestEngine(t, t.TempDir())
	mustSet(t, e, "k", "v1")
	first := mustCommit(t, e, "v1")
	mustSet(t, e, "k", "v2")
	mustCommit(t, e, "v2")
	old, err := e.materialize(first)
	if err != nil {
		t.Fatal(err)
	}
	if old["k"] != "v1" {
		t.Fatalf("historical state = %v", old)
	}
	if got := mustGet(t, e, "k"); got != "v2" {
		t.Fatalf("current = %q", got)
	}
	entries, err := e.Log(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Commit.Message != "v2" {
		t.Fatalf("log = %+v", entries)
	}
}

// Unchanged shards must be reused, otherwise commits would cost O(database).
func TestTreeSharesUnchangedShards(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory sync is not supported on Windows")
	}
	e := openTestEngine(t, t.TempDir())
	for i := 0; i < 200; i++ {
		mustSet(t, e, fmt.Sprintf("key%03d", i), "v")
	}
	c1 := mustCommit(t, e, "bulk")
	mustSet(t, e, "key000", "changed")
	c2 := mustCommit(t, e, "one key")
	t1, _ := e.loadCommit(c1)
	t2, _ := e.loadCommit(c2)
	root1, _ := e.loadTree(t1.Tree)
	root2, _ := e.loadTree(t2.Tree)
	same := 0
	m2 := root2.Map()
	for name, entry := range root1.Map() {
		if other, ok := m2[name]; ok && other.ID == entry.ID {
			same++
		}
	}
	if same < len(root1.Entries)-1 {
		t.Fatalf("only %d/%d shards reused", same, len(root1.Entries))
	}
}
