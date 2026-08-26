package server

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/CM-exe/staash/internal/store"
)

type Config struct {
	Addr        string
	IdleTimeout time.Duration
}

type Server struct {
	cfg Config
	st  *store.Store
	ln  net.Listener

	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	closing bool
	wg      sync.WaitGroup
}

func New(st *store.Store, cfg Config) *Server {
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:6380"
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 5 * time.Minute
	}
	return &Server{
		cfg:   cfg,
		st:    st,
		conns: make(map[net.Conn]struct{}),
	}
}

// Listen binds the socket. Splitting it from Serve lets tests use port 0 and
// then read back the real address before any client connects.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.ln = ln
	return nil
}

// Addr is the bound address; only valid after Listen succeeds.
func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Serve runs the accept loop until Close is called.
func (s *Server) Serve() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			s.mu.Lock()
			closing := s.closing
			s.mu.Unlock()
			if closing {
				return nil
			}
			// Transient accept errors (EMFILE, ECONNABORTED, etc) should not kill the server.
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return fmt.Errorf("accept: %w", err)
		}
		s.trackConn(conn, true)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.trackConn(conn, false)
			defer conn.Close()
			s.handleConn(conn)
		}()
	}
}

func (s *Server) trackConn(c net.Conn, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if add {
		s.conns[c] = struct{}{}
	} else {
		delete(s.conns, c)
	}
}

// Close stops accepting, drops live connections and waits for their goroutines
// to finish. Closing a connection makes its blocked Read return immediately,
// which is how we unblock handlers without a per-connection channel.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	var err error
	if s.ln != nil {
		err = s.ln.Close()
	}
	for _, c := range conns {
		c.Close()
	}
	s.wg.Wait()
	return err
}

func (s *Server) handleConn(conn net.Conn) {
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	for {
		if err := conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout)); err != nil {
			return
		}
		line, err := r.ReadString('\n')
		if err != nil {
			return // EOF, timeout, or the client vanished.
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if err := conn.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return
		}
		if quit != s.execute(w, fields); quit {
			w.Flush()
			return
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

// execute is deliberately crude; Phase 3 replaces it.
func (s *Server) execute(w *bufio.Writer, fields []string) (quit bool) {
	cmd := strings.ToUpper(fields[0])
	args := fields[1:]
	reply := func(format string, a ...any) {
		fmt.Fprintf(w, format+"\n", a...)
	}
	switch cmd {
	case "PING":
		reply("PONG")
	case "QUIT":
		reply("BYE")
		return true
	case "SET":
		if len(args) != 2 {
			reply("ERR wrong number of arguments for SET command")
			break
		}
		s.st.Set(args[0], args[1])
		reply("OK")
	case "GET":
		if len(args) != 1 {
			reply("ERR wrong number of arguments for GET command")
			break
		}
		if v, ok := s.st.Get(args[0]); ok {
			reply("%s", v)
		} else {
			reply("(nil)")
		}
	case "DEL":
		if len(args) != 1 {
			reply("ERR wrong number of arguments for DEL command")
			break
		}
		if s.st.Del(args[0]) {
			reply("(1)")
		} else {
			reply("(0)")
		}
	case "EXISTS":
		if len(args) != 1 {
			reply("ERR wrong number of arguments for EXISTS command")
			break
		}
		if s.st.Exists(args[0]) {
			reply("(1)")
		} else {
			reply("(0)")
		}
	case "KEYS":
		keys := s.st.Keys()
		reply("%d", len(keys))
		for _, k := range keys {
			reply("%s", k)
		}
	default:
		reply("ERR unknown command '%s'", cmd)
	}
	return false
}
