package server

import (
	"github.com/CM-exe/staash/internal/protocol"
	"github.com/CM-exe/staash/internal/store"
	"github.com/CM-exe/staash/internal/ui"
)

type session struct {
	st *store.Store
}

func newSession(st *store.Store) *session { return &session{st: st} }

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
		s.st.Set(cmd.Args[0], cmd.Args[1])
		return false, w.OK()
	case "GET":
		if n != 1 {
			return false, argErr()
		}
		v, ok := s.st.Get(cmd.Args[0])
		if !ok {
			return false, w.Nil()
		}
		return false, w.Bulk(v)
	case "DEL":
		if n != 1 {
			return false, argErr()
		}
		return false, w.Int(boolToInt(s.st.Del(cmd.Args[0])))
	case "EXISTS":
		if n != 1 {
			return false, argErr()
		}
		return false, w.Int(boolToInt(s.st.Exists(cmd.Args[0])))
	case "KEYS":
		if n != 0 {
			return false, argErr()
		}
		return false, w.StringArray(s.st.Keys())
	case "DBSIZE":
		return false, w.Int(int64(s.st.Len()))
	case "HELP":
		ui.DisplayBanner(w, ui.AppInfo{
			Name:       "Staash",
			Version:    "0.1.0",
			Author:     "CM-exe",
			Repository: "github.com/CM-exe/staash",
			License:    "MIT",
			LastUpdate: "2026-08-26",
		})
		w.Bulk("Commands:")
		w.Bulk("  PING")
		w.Bulk("  QUIT")
		w.Bulk("  SET <key> <value>")
		w.Bulk("  GET <key>")
		w.Bulk("  DEL <key>")
		w.Bulk("  EXISTS <key>")
		w.Bulk("  KEYS")
		w.Bulk("  DBSIZE")
		w.Bulk("  HELP")
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
