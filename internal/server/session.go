package server

import (
	"github.com/CM-exe/staash/internal/engine"
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
