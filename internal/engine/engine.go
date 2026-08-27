package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/CM-exe/staash/internal/object"
	"github.com/CM-exe/staash/internal/refs"
	"github.com/CM-exe/staash/internal/store"
	"github.com/CM-exe/staash/internal/wal"
)

const DefaultBranch = "main"

var (
	ErrNothingToCommit = errors.New("nothing to commit")
	ErrDirty           = errors.New("uncommitted changes present")
	ErrNoCommits       = errors.New("no commits yet")
	ErrBranchExists    = errors.New("branch already exists")
	ErrNoSuchBranch    = errors.New("no such branch")
)

// Options controls engine behaviour.
type Options struct {
	Dir string // data directory
	// SyncWAL makes every write durable before the client is acknowedged/
	SyncWAL bool
	// Now is injectable for deterministic tests.
	Now func() time.Time
}

// Engine is the database.
//
// Locking model (deliberately coarse): mu serialises every *mutating* and
// every version-control operation. Point reads bypass mu and go straight to
// the store, which has its own RWMutex. This is simple and obviously correct;
// it is also a scalability ceiling.
type Engine struct {
	mu      sync.Mutex
	opts    Options
	store   *store.Store
	objects *object.Store
	refs    *refs.Store
	log     *wal.WAL

	// dirty is the set of keys mutated since the last commit. It mirrors the
	// content of the WAL and is what makes commits incremental.
	dirty map[string]struct{}
}

// LogEntry pairs a commit with its ID.
type LogEntry struct {
	ID     object.ID
	Commit *object.Commit
}

// Open creates or reopens a database in opts.Dir and performs recovery.
func Open(opts Options) (*Engine, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	objects, err := object.NewStore(filepath.Join(opts.Dir, "objects"))
	if err != nil {
		return nil, err
	}
	if err := objects.CleanTmp(); err != nil {
		return nil, err
	}
	refStore, err := refs.NewStore(opts.Dir)
	if err != nil {
		return nil, err
	}
	if !refStore.HeadExists() {
		if err := refStore.SetHead(DefaultBranch); err != nil {
			return nil, err
		}
	}
	e := &Engine{
		opts:    opts,
		store:   store.New(),
		objects: objects,
		refs:    refStore,
		dirty:   make(map[string]struct{}),
	}
	// Step 1: rebuild the last committed state from the object store.
	head, err := refStore.Head()
	if err != nil {
		return nil, err
	}
	commitID, ok, err := refStore.ReadBranch(head)
	if err != nil {
		return nil, err
	}
	if ok {
		data, err := e.materialize(commitID)
		if err != nil {
			return nil, err
		}
		e.store.Replace(data)
	}
	// Step 2: reapply uncommitted mutations from the WAL on top.
	log, batches, err := wal.Open(filepath.Join(opts.Dir, "wal.log"), opts.SyncWAL)
	if err != nil {
		return nil, err
	}
	e.log = log
	for _, batch := range batches {
		e.store.ApplyBatch(batch)
		for _, m := range batch {
			e.dirty[m.Key] = struct{}{}
		}
	}
	return e, nil
}

func (e *Engine) Close() error { return e.log.Close() }

func (e *Engine) Get(key string) (string, bool) { return e.store.Get(key) }
func (e *Engine) Exists(key string) bool        { return e.store.Exists(key) }
func (e *Engine) Keys() []string                { return e.store.Keys() }
func (e *Engine) Len() int                      { return e.store.Len() }

// Apply durably records a batch of mutations and then applies it in memory.
//
// Ordering rule (the write-ahead rule): the log is written first. If we
// crashed between the two lines, recovery would replay the batch; if we wrote
// memory first and crashed, the acknowledged change would be lost.
func (e *Engine) Apply(muts []store.Mutation) error {
	if len(muts) == 0 {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.log.Append(muts); err != nil {
		return err
	}
	e.store.ApplyBatch(muts)
	for _, m := range muts {
		e.dirty[m.Key] = struct{}{}
	}
	return nil
}

// DirtyCount is the number of keys changed since the last commit.
func (e *Engine) DirtyCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.dirty)
}

func (e *Engine) Set(key, value string) error {
	return e.Apply([]store.Mutation{{Op: store.OpSet, Key: key, Value: value}})
}

func (e *Engine) Del(key string) (bool, error) {
	existed := e.store.Exists(key)
	if err := e.Apply([]store.Mutation{{Op: store.OpDel, Key: key}}); err != nil {
		return false, err
	}
	return existed, nil
}

// Commit turns the current in-memory state into an immutable commit on the
// current branch.
//
// Durability order, and why:
//  1. write blobs/trees/commit  (fsynced; unreferenced garbage if we crash)
//  2. update the branch ref     (atomic rename; this is the "commit point")
//  3. reset the WAL             (safe: state is now recoverable from objects)
//
// A crash between 2 and 3 replays the WAL on top of the new commit. Because
// WAL records are absolute, replay is idempotent and the result is identical.
func (e *Engine) Commit(message string) (object.ID, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	branch, err := e.refs.Head()
	if err != nil {
		return object.ID{}, err
	}
	parent, hasParent, err := e.refs.ReadBranch(branch)
	if err != nil {
		return object.ID{}, err
	}
	if len(e.dirty) == 0 && hasParent {
		return object.ID{}, ErrNothingToCommit
	}
	var parentTree object.ID
	var parents []object.ID
	if hasParent {
		pc, err := e.loadCommit(parent)
		if err != nil {
			return object.ID{}, err
		}
		parentTree = pc.Tree
		parents = []object.ID{parent}
	}
	treeID, err := e.writeTree(parentTree, e.store.Snapshot(), e.dirty)
	if err != nil {
		return object.ID{}, err
	}
	return e.finishCommit(branch, treeID, parents, message)
}

// Log walks the history reachable from the current branch, newest first.
// Walking the whole DAG (rather than only first parents) means merge commits
// show both sides of the history.
func (e *Engine) Log(limit int) ([]LogEntry, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	branch, err := e.refs.Head()
	if err != nil {
		return nil, err
	}
	headID, ok, err := e.refs.ReadBranch(branch)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNoCommits
	}
	seen := map[object.ID]bool{}
	var out []LogEntry
	queue := []object.ID{headID}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		c, err := e.loadCommit(id)
		if err != nil {
			return nil, err
		}
		out = append(out, LogEntry{ID: id, Commit: c})
		queue = append(queue, c.Parents...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Commit.Time.Equal(out[j].Commit.Time) {
			return out[i].ID.String() > out[j].ID.String()
		}
		return out[i].Commit.Time.After(out[j].Commit.Time)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Show returns a single commit by ID.
func (e *Engine) Show(id object.ID) (*object.Commit, error) {
	return e.loadCommit(id)
}

// HeadInfo reports the current branch and the commit it points at.
func (e *Engine) HeadInfo() (branch string, id object.ID, hasCommit bool, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	branch, err = e.refs.Head()
	if err != nil {
		return "", object.ID{}, false, err
	}
	id, hasCommit, err = e.refs.ReadBranch(branch)
	return branch, id, hasCommit, err
}

// Branch creates a new branch pointing at the current commit. Creating a
// branch writes 65 bytes and copies no data: history is immutable and shared.
func (e *Engine) Branch(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists, err := e.refs.ReadBranch(name); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("%w: %s", ErrBranchExists, name)
	}
	cur, err := e.refs.Head()
	if err != nil {
		return err
	}
	id, ok, err := e.refs.ReadBranch(cur)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNoCommits
	}
	return e.refs.SetBranch(name, id)
}

func (e *Engine) Branches() ([]string, string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	names, err := e.refs.ListBranches()
	if err != nil {
		return nil, "", err
	}
	cur, err := e.refs.Head()
	if err != nil {
		return nil, "", err
	}
	return names, cur, nil
}

// Checkout switches branches and replaces the in-memory state.
//
// Uncommitted changes are refused rather than carried across or silently
// discarded: with no staging area there is no way to tell the two intents
// apart, and losing writes silently is the worse failure.
func (e *Engine) Checkout(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.dirty) > 0 {
		return fmt.Errorf("%w (%d keys); COMMIT or ROLLBACK first", ErrDirty, len(e.dirty))
	}
	id, ok, err := e.refs.ReadBranch(name)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: %s", ErrNoSuchBranch, name)
	}
	data, err := e.materialize(id)
	if err != nil {
		return err
	}
	if err := e.refs.SetHead(name); err != nil {
		return err
	}
	e.store.Replace(data)
	return nil
}

// finishCommit writes the commit object, moves the branch and clears the WAL.
// Callers must hold e.mu.
func (e *Engine) finishCommit(branch string, treeID object.ID, parents []object.ID, message string) (object.ID, error) {
	c := &object.Commit{
		Tree:    treeID,
		Parents: parents,
		Time:    e.opts.Now(),
		Message: message,
	}
	id, err := e.objects.Put(object.KindCommit, c.Encode())
	if err != nil {
		return object.ID{}, err
	}
	if err := e.refs.SetBranch(branch, id); err != nil {
		return object.ID{}, err
	}
	if err := e.log.Reset(); err != nil {
		return object.ID{}, err
	}
	e.dirty = make(map[string]struct{})
	return id, nil
}

// shardOf maps a key to one of 256 buckets. Hashing (rather than using the
// key's own first byte) keeps buckets evenly sized even when keys share a
// prefix such as "user:". The cost is that trees are no longer in key order,
// so ordered range scans would need a different layout.
func shardOf(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:1])
}

// writeTree produces the root tree for snapshot, reusing every shard of
// parentTree that none of the dirty keys touch. That reuse is why a commit
// costs O(changed keys), not O(database size), and why branches are cheap.
func (e *Engine) writeTree(parentTree object.ID, snapshot map[string]string, dirty map[string]struct{}) (object.ID, error) {
	root := map[string]object.Entry{}
	if !parentTree.IsZero() {
		t, err := e.loadTree(parentTree)
		if err != nil {
			return object.ID{}, err
		}
		root = t.Map()
	}
	byShard := map[string][]string{}
	for k := range dirty {
		s := shardOf(k)
		byShard[s] = append(byShard[s], k)
	}
	for shard, keys := range byShard {
		leaf := map[string]object.Entry{}
		if e0, ok := root[shard]; ok {
			t, err := e.loadTree(e0.ID)
			if err != nil {
				return object.ID{}, err
			}
			leaf = t.Map()
		}
		for _, k := range keys {
			v, present := snapshot[k]
			if !present {
				delete(leaf, k)
				continue
			}
			blobID, err := e.objects.Put(object.KindBlob, []byte(v))
			if err != nil {
				return object.ID{}, err
			}
			leaf[k] = object.Entry{Name: k, Kind: object.EntryBlob, ID: blobID}
		}
		if len(leaf) == 0 {
			delete(root, shard)
			continue
		}
		id, err := e.objects.Put(object.KindTree, object.TreeFromMap(leaf).Encode())
		if err != nil {
			return object.ID{}, err
		}
		root[shard] = object.Entry{Name: shard, Kind: object.EntryTree, ID: id}
	}
	return e.objects.Put(object.KindTree, object.TreeFromMap(root).Encode())
}

func (e *Engine) loadTree(id object.ID) (*object.Tree, error) {
	payload, err := e.objects.GetKind(id, object.KindTree)
	if err != nil {
		return nil, err
	}
	return object.DecodeTree(payload)
}

// treeKeys flattens a root tree into key -> blob ID.
func (e *Engine) treeKeys(rootTree object.ID) (map[string]object.ID, error) {
	out := map[string]object.ID{}
	if rootTree.IsZero() {
		return out, nil
	}
	root, err := e.loadTree(rootTree)
	if err != nil {
		return nil, err
	}
	for _, shard := range root.Entries {
		leaf, err := e.loadTree(shard.ID)
		if err != nil {
			return nil, err
		}
		for _, entry := range leaf.Entries {
			out[entry.Name] = entry.ID
		}
	}
	return out, nil
}

func (e *Engine) loadCommit(id object.ID) (*object.Commit, error) {
	payload, err := e.objects.GetKind(id, object.KindCommit)
	if err != nil {
		return nil, err
	}
	return object.DecodeCommit(payload)
}

// materialize reconstructs the full keyspace of a commit.
func (e *Engine) materialize(commitID object.ID) (map[string]string, error) {
	c, err := e.loadCommit(commitID)
	if err != nil {
		return nil, err
	}
	ids, err := e.treeKeys(c.Tree)
	if err != nil {
		return nil, err
	}
	data := make(map[string]string, len(ids))
	for k, id := range ids {
		payload, err := e.objects.GetKind(id, object.KindBlob)
		if err != nil {
			return nil, err
		}
		data[k] = string(payload)
	}
	return data, nil
}
