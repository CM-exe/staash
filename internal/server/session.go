package server

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/CM-exe/staash/internal/engine"
	"github.com/CM-exe/staash/internal/object"
	"github.com/CM-exe/staash/internal/protocol"
	"github.com/CM-exe/staash/internal/ui"
)

type session struct {
	eng *engine.Engine
}

func newSession(e *engine.Engine) *session { return &session{eng: e} }

// dispatch executes one command. The returned error is a *transport* error:
// if it is non-nil the connection is unusable and must be closed. Command
// errors are written to the client as -ERR replies and return nil.
func (s *session) dispatch(w *protocol.Writer, cmd protocol.Command) (quit bool, err error) {
	n := len(cmd.Args)
	argErr := func() error { return w.Error("wrong number of arguments for " + cmd.Name + " command") }

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
		if err := s.eng.Set(cmd.Args[0], cmd.Args[1]); err != nil {
			return false, w.Error(err.Error())
		}
		return false, w.OK()
	case "GET":
		if n != 1 {
			return false, argErr()
		}
		v, ok := s.eng.Get(cmd.Args[0])
		if !ok {
			return false, w.Nil()
		}
		return false, w.Bulk(v)
	case "DEL":
		if n != 1 {
			return false, argErr()
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
		return false, w.Int(boolToInt(s.eng.Exists(cmd.Args[0])))
	case "KEYS":
		if n != 0 {
			return false, argErr()
		}
		return false, w.StringArray(s.eng.Keys())
	case "DBSIZE":
		return false, w.Int(int64(s.eng.Len()))
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
		w.Banner("  HELP")
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
