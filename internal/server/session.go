package server

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/CM-exe/staash/internal/engine"
	"github.com/CM-exe/staash/internal/object"
	"github.com/CM-exe/staash/internal/protocol"
	"github.com/CM-exe/staash/internal/store"
	"github.com/CM-exe/staash/internal/ui"
)

// session is the per-connection state. Everything that is not shared between
// clients lives here; today that is only the open transaction.
type session struct {
	eng *engine.Engine
	tx  *txn
}

// txn buffers writes until EXEC.
//
// overlay maps key -> value, where a nil pointer means "deleted". order keeps
// insertion order so the emitted batch is deterministic (nice for tests and
// for reading the WAL by hand).
type txn struct {
	overlay map[string]*string
	order   []string
}

func newSession(e *engine.Engine) *session { return &session{eng: e} }

func (t *txn) set(key, value string) {
	if _, ok := t.overlay[key]; !ok {
		t.order = append(t.order, key)
	}
	v := value
	t.overlay[key] = &v
}

func (t *txn) del(key string) {
	if _, ok := t.overlay[key]; !ok {
		t.order = append(t.order, key)
	}
	t.overlay[key] = nil
}

func (t *txn) mutations() []store.Mutation {
	muts := make([]store.Mutation, 0, len(t.order))
	for _, k := range t.order {
		v := t.overlay[k]
		if v == nil {
			muts = append(muts, store.Mutation{Op: store.OpDel, Key: k})
		} else {
			muts = append(muts, store.Mutation{Op: store.OpSet, Key: k, Value: *v})
		}
	}
	return muts
}

// read implements read-your-own-writes inside a transaction.
func (s *session) read(key string) (string, bool) {
	if s.tx != nil {
		if v, ok := s.tx.overlay[key]; ok {
			if v == nil {
				return "", false
			}
			return *v, true
		}
	}
	return s.eng.Get(key)
}

// keys merges committed keys with the transaction overlay.
func (s *session) keys() []string {
	base := s.eng.Keys()
	if s.tx == nil {
		return base
	}
	set := make(map[string]struct{}, len(base))
	for _, k := range base {
		set[k] = struct{}{}
	}
	for k, v := range s.tx.overlay {
		if v == nil {
			delete(set, k)
		} else {
			set[k] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// dispatch executes one command. The returned error is a *transport* error:
// if it is non-nil the connection is unusable and must be closed. Command
// errors are written to the client as -ERR replies and return nil.
func (s *session) dispatch(w *protocol.Writer, cmd protocol.Command) (quit bool, err error) {
	n := len(cmd.Args)
	argErr := func() error { return w.Error("wrong number of arguments for " + cmd.Name + " command") }

	// Version-control commands change the whole keyspace, so they are refused
	// while a transaction is buffered rather than given ill-defined semantics.
	switch cmd.Name {
	case "COMMIT", "CHECKOUT", "MERGE", "BRANCH":
		if s.tx != nil {
			return false, w.Error(cmd.Name + " is not allowed inside a transaction")
		}
	}

	switch cmd.Name {
	case "PING":
		if n == 1 {
			return false, w.Bulk(cmd.Args[0])
		}
		return false, w.Simple("PONG")
	case "QUIT":
		return true, w.BYE()
	case "SET":
		if n != 2 {
			return false, argErr()
		}
		if s.tx != nil {
			s.tx.set(cmd.Args[0], cmd.Args[1])
			return false, w.Simple("QUEUED")
		}
		if err := s.eng.Set(cmd.Args[0], cmd.Args[1]); err != nil {
			return false, w.Error(err.Error())
		}
		return false, w.OK()
	case "GET":
		if n != 1 {
			return false, argErr()
		}
		v, ok := s.read(cmd.Args[0])
		if !ok {
			return false, w.Nil()
		}
		return false, w.Bulk(v)
	case "DEL":
		if n != 1 {
			return false, argErr()
		}
		if s.tx != nil {
			s.tx.del(cmd.Args[0])
			return false, w.Simple("QUEUED")
		}
		existed, err := s.eng.Del(cmd.Args[0])
		if err != nil {
			return false, w.Error(err.Error())
		}
		return false, w.Int(boolToInt(existed))
	case "EXISTS":
		if n != 1 {
			return false, argErr()
		}
		_, ok := s.read(cmd.Args[0])
		return false, w.Int(boolToInt(ok))
	case "KEYS":
		if n != 0 {
			return false, argErr()
		}
		return false, w.StringArray(s.keys())
	case "DBSIZE":
		return false, w.Int(int64(len(s.keys())))
	case "COMMIT":
		if n != 1 {
			return false, argErr()
		}
		id, err := s.eng.Commit(cmd.Args[0])
		if err != nil {
			return false, w.Error(err.Error())
		}
		return false, w.Simple(id.String())
	case "LOG":
		limit := 20
		if n == 1 {
			v, err := strconv.Atoi(cmd.Args[0])
			if err != nil || v < 0 {
				return false, w.Error("LOG: limit must be a positive integer")
			}
			limit = v
		}
		entries, err := s.eng.Log(limit)
		if err != nil {
			return false, w.Error(err.Error())
		}
		lines := make([]string, 0, len(entries))
		for _, e := range entries {
			lines = append(lines, fmt.Sprintf("%s %s %s",
				e.ID.Short(), e.Commit.Time.UTC().Format(time.RFC3339), e.Commit.Message))
		}
		return false, w.StringArray(lines)
	case "SHOW":
		var id object.ID
		if n == 1 {
			parsed, err := object.ParseID(cmd.Args[0])
			if err != nil {
				return false, w.Error(err.Error())
			}
			id = parsed
		} else {
			_, headID, ok, err := s.eng.HeadInfo()
			if err != nil {
				return false, w.Error(err.Error())
			}
			if !ok {
				return false, w.Error(engine.ErrNoCommits.Error())
			}
			id = headID
		}
		c, err := s.eng.Show(id)
		if err != nil {
			return false, w.Error(err.Error())
		}
		var b strings.Builder
		fmt.Fprintf(&b, "commit %s\ntree %s\n", id, c.Tree)
		for _, p := range c.Parents {
			fmt.Fprintf(&b, "parent %s\n", p)
		}
		fmt.Fprintf(&b, "time %s\n\n%s", c.Time.UTC().Format(time.RFC3339Nano), c.Message)
		return false, w.Bulk(b.String())
	case "HEAD":
		branch, id, ok, err := s.eng.HeadInfo()
		if err != nil {
			return false, w.Error(err.Error())
		}
		if !ok {
			return false, w.Bulk(branch + " (no commits)")
		}
		return false, w.Bulk(branch + " " + id.String())
	case "STATUS":
		branch, _, _, err := s.eng.HeadInfo()
		if err != nil {
			return false, w.Error(err.Error())
		}
		return false, w.Bulk(fmt.Sprintf("branch %s, %d uncommitted key(s), %d key(s) total",
			branch, s.eng.DirtyCount(), s.eng.Len()))
	case "BRANCH":
		if n != 1 {
			return false, argErr()
		}
		if err := s.eng.Branch(cmd.Args[0]); err != nil {
			return false, w.Error(err.Error())
		}
		return false, w.OK()
	case "BRANCHES":
		names, cur, err := s.eng.Branches()
		if err != nil {
			return false, w.Error(err.Error())
		}
		out := make([]string, 0, len(names))
		for _, name := range names {
			if name == cur {
				out = append(out, "* "+name)
			} else {
				out = append(out, "  "+name)
			}
		}
		return false, w.StringArray(out)
	case "CHECKOUT":
		if n != 1 {
			return false, argErr()
		}
		if err := s.eng.Checkout(cmd.Args[0]); err != nil {
			return false, w.Error(err.Error())
		}
		return false, w.OK()
	case "BEGIN":
		if s.tx != nil {
			return false, w.Error("transaction already open")
		}
		s.tx = &txn{overlay: map[string]*string{}}
		return false, w.OK()
	case "EXEC":
		if s.tx == nil {
			return false, w.Error("EXEC without BEGIN")
		}
		muts := s.tx.mutations()
		s.tx = nil
		if err := s.eng.Apply(muts); err != nil {
			return false, w.Error(err.Error())
		}
		return false, w.Int(int64(len(muts)))
	case "ROLLBACK", "DISCARD":
		if s.tx == nil {
			return false, w.Error("ROLLBACK without BEGIN")
		}
		s.tx = nil
		return false, w.OK()
	case "HELP":
		ui.DisplayBanner(w, ui.AppInfo{
			Name:       "Staash",
			Version:    "0.1.0",
			Author:     "CM-exe",
			Repository: "github.com/CM-exe/staash",
			License:    "MIT",
			LastUpdate: "2026-08-26",
		})
		w.Banner("Commands:")
		w.Banner("  PING")
		w.Banner("  QUIT")
		w.Banner("-------------DB---------------------")
		w.Banner("  SET <key> <value>")
		w.Banner("  GET <key>")
		w.Banner("  DEL <key>")
		w.Banner("  EXISTS <key>")
		w.Banner("  KEYS")
		w.Banner("  DBSIZE")
		w.Banner("-------------VERSION----------------")
		w.Banner("  COMMIT <message>")
		w.Banner("  LOG [limit]")
		w.Banner("  SHOW [commit-id]")
		w.Banner("  HEAD")
		w.Banner("  STATUS")
		w.Banner("  BRANCH <name>")
		w.Banner("  BRANCHES")
		w.Banner("  CHECKOUT <name>")
		w.Banner("-------------TRANSACTION------------")
		w.Banner("  BEGIN")
		w.Banner("  EXEC")
		w.Banner("  ROLLBACK / DISCARD")
		w.Banner("-------------HELP-------------------")
		w.Banner("  HELP")
		return false, nil
	default:
		return false, w.Error("unknown command: " + cmd.Name)
	}
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
