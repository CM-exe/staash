package engine

import (
	"errors"
	"fmt"
	"sort"

	"github.com/CM-exe/staash/internal/object"
)

// ErrMergeConflict is returned when both sides changed the same key
// differently. The merge is aborted; nothing is written.
type ErrMergeConflict struct {
	Keys []string
}

func (e *ErrMergeConflict) Error() string {
	return fmt.Sprintf("merge conflict on %d key(s): %v", len(e.Keys), e.Keys)
}

var ErrUnrelatedHistories = errors.New("no common ancestor")

// MergeResult describes what Merge did.
type MergeResult struct {
	Kind   string // "up-to-date" | "fast-forward" | "merge"
	Commit object.ID
}

func (e *Engine) Merge(name string) (MergeResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.dirty) > 0 {
		return MergeResult{}, fmt.Errorf("%w (%d keys); COMMIT or ROLLBACK first", ErrDirty, len(e.dirty))
	}
	branch, err := e.refs.Head()
	if err != nil {
		return MergeResult{}, err
	}
	if name == branch {
		return MergeResult{}, errors.New("cannot merge a branch into itself")
	}
	oursID, ok, err := e.refs.ReadBranch(branch)
	if err != nil {
		return MergeResult{}, err
	}
	if !ok {
		return MergeResult{}, ErrNoCommits
	}
	theirsID, ok, err := e.refs.ReadBranch(name)
	if err != nil {
		return MergeResult{}, err
	}
	if !ok {
		return MergeResult{}, fmt.Errorf("%w: %s", ErrNoSuchBranch, name)
	}
	baseID, err := e.mergeBase(oursID, theirsID)
	if err != nil {
		return MergeResult{}, err
	}

	// Case 1: their branch is already in our history.
	if baseID == theirsID {
		return MergeResult{Kind: "up-to-date", Commit: oursID}, nil
	}
	// Case 2: we have added nothing since the base -> fast-forward.
	if baseID == oursID {
		data, err := e.materialize(theirsID)
		if err != nil {
			return MergeResult{}, err
		}
		if err := e.refs.SetBranch(branch, theirsID); err != nil {
			return MergeResult{}, err
		}
		e.store.Replace(data)
		return MergeResult{Kind: "fast-forward", Commit: theirsID}, nil
	}
	// Case 3: real three-way merge.
	baseKeys, err := e.commitKeys(baseID)
	if err != nil {
		return MergeResult{}, err
	}
	ourKeys, err := e.commitKeys(oursID)
	if err != nil {
		return MergeResult{}, err
	}
	theirKeys, err := e.commitKeys(theirsID)
	if err != nil {
		return MergeResult{}, err
	}
	all := map[string]struct{}{}
	for k := range baseKeys {
		all[k] = struct{}{}
	}
	for k := range ourKeys {
		all[k] = struct{}{}
	}
	for k := range theirKeys {
		all[k] = struct{}{}
	}

	var conflicts []string
	// changes we must apply on top of *our* tree, as key -> blob (zero = delete)
	apply := map[string]object.ID{}
	for k := range all {
		base, inBase := baseKeys[k]
		ours, inOurs := ourKeys[k]
		theirs, inTheirs := theirKeys[k]
		oursChanged := inOurs != inBase || ours != base
		theirsChanged := inTheirs != inBase || theirs != base
		switch {
		case !theirsChanged:
			// keep ours; nothing to apply
		case !oursChanged:
			apply[k] = theirs // zero ID when they deleted it
			if !inTheirs {
				apply[k] = object.ID{}
			}
		case inOurs == inTheirs && ours == theirs:
			// both sides made the same change
		default:
			conflicts = append(conflicts, k)
		}
	}
	if len(conflicts) > 0 {
		sort.Strings(conflicts)
		return MergeResult{}, &ErrMergeConflict{Keys: conflicts}
	}

	// Build the merged keyspace from our current state plus their changes.
	merged := e.store.Snapshot()
	changed := map[string]struct{}{}
	for k, blob := range apply {
		changed[k] = struct{}{}
		if blob.IsZero() {
			delete(merged, k)
			continue
		}
		payload, err := e.objects.GetKind(blob, object.KindBlob)
		if err != nil {
			return MergeResult{}, err
		}
		merged[k] = string(payload)
	}
	ourCommit, err := e.loadCommit(oursID)
	if err != nil {
		return MergeResult{}, err
	}
	treeID, err := e.writeTree(ourCommit.Tree, merged, changed)
	if err != nil {
		return MergeResult{}, err
	}
	msg := fmt.Sprintf("merge branch %q into %q", name, branch)
	id, err := e.finishCommit(branch, treeID, []object.ID{oursID, theirsID}, msg)
	if err != nil {
		return MergeResult{}, err
	}
	e.store.Replace(merged)
	return MergeResult{Kind: "merge", Commit: id}, nil
}

func (e *Engine) commitKeys(id object.ID) (map[string]object.ID, error) {
	c, err := e.loadCommit(id)
	if err != nil {
		return nil, err
	}
	return e.treeKeys(c.Tree)
}

// mergeBase finds a common ancestor of a and b.
//
// The algorithm is a breadth-first walk from b through the set of ancestors of
// a. For the shapes this database can produce (linear history plus merges) it
// finds the nearest common ancestor. It is *not* Git's full merge-base
// algorithm: with criss-cross merges several equally good bases exist and this
// returns whichever the BFS reaches first.
func (e *Engine) mergeBase(a, b object.ID) (object.ID, error) {
	ancestors := map[object.ID]bool{}
	queue := []object.ID{a}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if ancestors[id] {
			continue
		}
		ancestors[id] = true
		c, err := e.loadCommit(id)
		if err != nil {
			return object.ID{}, err
		}
		queue = append(queue, c.Parents...)
	}
	seen := map[object.ID]bool{}
	queue = []object.ID{b}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		if ancestors[id] {
			return id, nil
		}
		c, err := e.loadCommit(id)
		if err != nil {
			return object.ID{}, err
		}
		queue = append(queue, c.Parents...)
	}
	return object.ID{}, ErrUnrelatedHistories
}
