package server

import (
	"bufio"
	"errors"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/CM-exe/staash/internal/engine"
	"github.com/CM-exe/staash/internal/protocol"
)

type Config struct {
	Addr         string
	MaxLine      int           // maximum request line in bytes
	IdleTimeout  time.Duration // close connections that say nothing for this long
	WriteTimeout time.Duration // fail writes that take longer than this
	Logger       *log.Logger
}

func (c *Config) withDefaults() {
	if c.Addr == "" {
		c.Addr = "127.0.0.1:6380"
	}
	if c.MaxLine == 0 {
		c.MaxLine = 64 << 10 // 64 KiB
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 5 * time.Minute
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = log.Default()
	}
}

type Server struct {
	cfg Config
	eng *engine.Engine
	ln  net.Listener

	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	closing bool
	wg      sync.WaitGroup
}

func New(e *engine.Engine, cfg Config) *Server {
	cfg.withDefaults()
	return &Server{
		cfg:   cfg,
		eng:   e,
		conns: make(map[net.Conn]struct{}),
	}
}

// Listen binds the socket. Splitting it from Serve lets tests use port 0 and
// then read back the real address before any client connects.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
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
			return err
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
	reader := bufio.NewReaderSize(conn, 4096)
	bw := bufio.NewWriter(conn)
	w := protocol.NewWriter(bw)
	sess := newSession(s.eng)

	for {
		if err := conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout)); err != nil {
			return
		}
		line, err := readLine(reader, s.cfg.MaxLine)
		if err != nil {
			if errors.Is(err, errLineTooLong) {
				_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
				_ = w.Error("request line exceeds limit of 64 KiB")
				_ = w.Flush()
			}
			return // EOF, timeout, or a client we no longer trust to be in sync
		}
		if strings.TrimSpace(line) == "" {
			continue
		}

		cmd, perr := protocol.Parse(line)
		if err := conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout)); err != nil {
			return
		}
		if perr != nil {
			if err := w.Error(perr.Error()); err != nil {
				return
			}
		} else {
			quit, err := sess.dispatch(w, cmd)
			if err != nil {
				return
			}
			if quit {
				_ = w.Flush()
				return
			}
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

var errLineTooLong = errors.New("request line exceeds limit of 64 KiB")

// readLine reads up to and including '\n, enforcing a maximum length so a
// malicious or buggy client cannot make the server allocate without bound.
func readLine(r *bufio.Reader, max int) (string, error) {
	var buf []byte
	for {
		chunk, err := r.ReadSlice('\n')
		buf = append(buf, chunk...)
		if err == bufio.ErrBufferFull {
			if len(buf) > max {
				return "", errLineTooLong
			}
			continue
		}
		if err != nil {
			return "", err
		}
		if len(buf) > max {
			return "", errLineTooLong
		}
		return strings.TrimRight(string(buf), "\r\n"), nil
	}
}
