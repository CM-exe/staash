package engine

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/CM-exe/staash/internal/store"
	"github.com/CM-exe/staash/internal/wal"
)

// Options controls engine behaviour.
type Options struct {
	Dir     string
	SyncWAL bool
}

// Engine owns the store and the log. mu serialises writes so that a batch is
// appened to the log and applied to memory as one indivisible operation.
type Engine struct {
	mu    sync.Mutex
	opts  Options
	store *store.Store
	log   *wal.WAL
}

// Open creates or reopens the database in opts.Dir and replays the log.
func Open(opts Options) (*Engine, error) {
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	log, batches, err := wal.Open(filepath.Join(opts.Dir, "wal.log"), opts.SyncWAL)
	if err != nil {
		return nil, err
	}
	e := &Engine{opts: opts, store: store.New(), log: log}
	for _, batch := range batches {
		e.store.ApplyBatch(batch)
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
	return nil
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
