package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

func TestBranchCheckout(t *testing.T) {
	dir := t.TempDir()
	e := openTestEngine(t, dir)
	mustSet(t, e, "shared", "base")
	mustCommit(t, e, "base")
	if err := e.Branch("feature"); err != nil {
		t.Fatal(err)
	}
	if err := e.Checkout("feature"); err != nil {
		t.Fatal(err)
	}
	mustSet(t, e, "feature-only", "1")
	mustCommit(t, e, "on feature")
	if err := e.Checkout("main"); err != nil {
		t.Fatal(err)
	}
	if e.Exists("feature-only") {
		t.Fatal("feature key leaked into main")
	}
	if err := e.Checkout("feature"); err != nil {
		t.Fatal(err)
	}
	if !e.Exists("feature-only") {
		t.Fatal("feature key lost")
	}
	// Checkout with uncommitted work must be refused.
	mustSet(t, e, "scratch", "x")
	if err := e.Checkout("main"); err == nil {
		t.Fatal("expected checkout to be refused while dirty")
	}
}

func TestThreeWayMergeNoConflict(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	mustSet(t, e, "name", "alice")
	mustCommit(t, e, "base")
	_ = e.Branch("feature")
	mustSet(t, e, "city", "berlin") // on main
	mustCommit(t, e, "main change")
	_ = e.Checkout("feature")
	mustSet(t, e, "email", "a@example.com")
	mustCommit(t, e, "feature change")
	_ = e.Checkout("main")
	res, err := e.Merge("feature")
	if err != nil {
		t.Fatal(err)
	}
	if res.Kind != "merge" {
		t.Fatalf("kind = %s", res.Kind)
	}
	for k, want := range map[string]string{"name": "alice", "city": "berlin", "email": "a@example.com"} {
		if got := mustGet(t, e, k); got != want {
			t.Fatalf("%s = %q want %q", k, got, want)
		}
	}
	c, err := e.Show(res.Commit)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Parents) != 2 {
		t.Fatalf("merge commit has %d parents", len(c.Parents))
	}
}

func TestMergeConflict(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	mustSet(t, e, "name", "alice")
	mustCommit(t, e, "base")
	_ = e.Branch("feature")
	mustSet(t, e, "name", "bob")
	mustCommit(t, e, "main renames")
	_ = e.Checkout("feature")
	mustSet(t, e, "name", "charlie")
	mustCommit(t, e, "feature renames")
	_ = e.Checkout("main")
	_, err := e.Merge("feature")
	var conflict *ErrMergeConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("err = %v, want conflict", err)
	}
	if len(conflict.Keys) != 1 || conflict.Keys[0] != "name" {
		t.Fatalf("conflict keys = %v", conflict.Keys)
	}
	// The failed merge must leave the database untouched.
	if got := mustGet(t, e, "name"); got != "bob" {
		t.Fatalf("name = %q after aborted merge", got)
	}
}

func TestMergeIdenticalChangeIsNotAConflict(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	mustSet(t, e, "k", "base")
	mustCommit(t, e, "base")
	_ = e.Branch("feature")
	mustSet(t, e, "k", "same")
	mustCommit(t, e, "main")
	_ = e.Checkout("feature")
	mustSet(t, e, "k", "same")
	mustCommit(t, e, "feature")
	_ = e.Checkout("main")
	if _, err := e.Merge("feature"); err != nil {
		t.Fatalf("identical changes should merge cleanly: %v", err)
	}
}

func TestMergeDeletion(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	mustSet(t, e, "gone", "1")
	mustSet(t, e, "kept", "1")
	mustCommit(t, e, "base")
	_ = e.Branch("feature")
	_ = e.Checkout("feature")
	if _, err := e.Del("gone"); err != nil {
		t.Fatal(err)
	}
	mustCommit(t, e, "delete on feature")
	_ = e.Checkout("main")
	mustSet(t, e, "other", "1")
	mustCommit(t, e, "main change")
	if _, err := e.Merge("feature"); err != nil {
		t.Fatal(err)
	}
	if e.Exists("gone") {
		t.Fatal("deletion was not merged")
	}
	if !e.Exists("kept") || !e.Exists("other") {
		t.Fatal("merge dropped unrelated keys")
	}
}

func TestFastForwardMerge(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	mustSet(t, e, "a", "1")
	mustCommit(t, e, "base")
	_ = e.Branch("feature")
	_ = e.Checkout("feature")
	mustSet(t, e, "b", "2")
	mustCommit(t, e, "feature work")
	_ = e.Checkout("main")
	res, err := e.Merge("feature")
	if err != nil {
		t.Fatal(err)
	}
	if res.Kind != "fast-forward" {
		t.Fatalf("kind = %s", res.Kind)
	}
	if got := mustGet(t, e, "b"); got != "2" {
		t.Fatalf("b = %q", got)
	}
}

// Uncommitted writes must survive a restart via the WAL.
func TestUncommittedWritesRecovered(t *testing.T) {
	dir := t.TempDir()
	e := openTestEngine(t, dir)
	mustSet(t, e, "committed", "yes")
	mustCommit(t, e, "c1")
	mustSet(t, e, "pending", "yes")
	if _, err := e.Del("committed"); err != nil {
		t.Fatal(err)
	}
	e.Close() // simulate a crash: no commit, WAL holds two mutations
	e2 := openTestEngine(t, dir)
	if v, ok := e2.Get("pending"); !ok || v != "yes" {
		t.Fatalf("pending lost: %q %v", v, ok)
	}
	if e2.Exists("committed") {
		t.Fatal("delete was not replayed")
	}
	if e2.DirtyCount() != 2 {
		t.Fatalf("dirty = %d, want 2", e2.DirtyCount())
	}
}

// A crash between "branch ref updated" and "WAL truncated" must be harmless.
func TestCrashBetweenCommitAndWALReset(t *testing.T) {
	dir := t.TempDir()
	e := openTestEngine(t, dir)
	mustSet(t, e, "k", "v")
	mustCommit(t, e, "c1")
	e.Close()
	// Re-create the situation by hand: append a record that the (already
	// committed) state already contains.
	e2 := openTestEngine(t, dir)
	mustSet(t, e2, "k", "v")
	e2.Close()
	e3 := openTestEngine(t, dir)
	if got := mustGet(t, e3, "k"); got != "v" {
		t.Fatalf("k = %q", got)
	}
}

func TestStrayTempObjectsAreCleaned(t *testing.T) {
	dir := t.TempDir()
	e := openTestEngine(t, dir)
	mustSet(t, e, "a", "1")
	mustCommit(t, e, "c")
	e.Close()
	stray := filepath.Join(dir, "objects", "tmp", "obj-crash")
	if err := os.WriteFile(stray, []byte("half written"), 0o644); err != nil {
		t.Fatal(err)
	}
	e2 := openTestEngine(t, dir)
	if _, err := os.Stat(stray); !os.IsNotExist(err) {
		t.Fatal("stray temp file survived recovery")
	}
	if got := mustGet(t, e2, "a"); got != "1" {
		t.Fatalf("a = %q", got)
	}
}
