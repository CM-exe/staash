package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CM-exe/staash/internal/engine"
)

type client struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func startServer(t *testing.T) (*Server, string) {
	t.Helper()
	eng, err := engine.Open(engine.Options{Dir: t.TempDir(), SyncWAL: false})
	if err != nil {
		t.Fatal(err)
	}
	srv := New(eng, Config{Addr: "127.0.0.1:0", IdleTimeout: 5 * time.Second})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() {
		srv.Close()
		eng.Close()
	})
	return srv, srv.Addr()
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &client{t: t, conn: conn, r: bufio.NewReader(conn)}
}

// do sends a command and returns the raw reply, already unwrapped for bulk
// strings and arrays.
func (c *client) do(format string, args ...any) string {
	c.t.Helper()
	line := fmt.Sprintf(format, args...)
	if _, err := c.conn.Write([]byte(line + "\r\n")); err != nil {
		c.t.Fatal(err)
	}
	return c.readReply()
}

func (c *client) readReply() string {
	c.t.Helper()
	line, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return ""
	}
	switch line[0] {
	case '+', '-', ':':
		return line
	case '$':
		var n int
		fmt.Sscanf(line[1:], "%d", &n)
		if n < 0 {
			return "(nil)"
		}
		buf := make([]byte, n+2)
		if _, err := readFull(c.r, buf); err != nil {
			c.t.Fatal(err)
		}
		return string(buf[:n])
	case '*':
		var n int
		fmt.Sscanf(line[1:], "%d", &n)
		items := make([]string, 0, n)
		for i := 0; i < n; i++ {
			items = append(items, c.readReply())
		}
		return "[" + strings.Join(items, ",") + "]"
	}
	c.t.Fatalf("unexpected reply %q", line)
	return ""
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func TestTransactionIsolationAndRollback(t *testing.T) {
	_, addr := startServer(t)
	a := dial(t, addr)
	b := dial(t, addr)
	a.do("SET k v0")
	a.do("BEGIN")
	if got := a.do("SET k v1"); got != "+QUEUED" {
		t.Fatalf("queued = %q", got)
	}
	a.do("SET other x")
	// Own writes are visible to the transaction...
	if got := a.do("GET k"); got != "v1" {
		t.Fatalf("read-your-writes = %q", got)
	}
	// ...but not to anyone else.
	if got := b.do("GET k"); got != "v0" {
		t.Fatalf("leaked uncommitted write: %q", got)
	}
	if got := a.do("EXEC"); got != ":2" {
		t.Fatalf("EXEC = %q", got)
	}
	if got := b.do("GET k"); got != "v1" {
		t.Fatalf("after EXEC = %q", got)
	}

	a.do("BEGIN")
	a.do("SET k rolled-back")
	if got := a.do("ROLLBACK"); got != "+OK" {
		t.Fatalf("ROLLBACK = %q", got)
	}
	if got := a.do("GET k"); got != "v1" {
		t.Fatalf("rollback failed: %q", got)
	}
	if got := a.do("EXEC"); !strings.HasPrefix(got, "-ERR EXEC without BEGIN") {
		t.Fatalf("EXEC outside txn = %q", got)
	}
	a.do("BEGIN")
	if got := a.do(`COMMIT "nope"`); !strings.HasPrefix(got, "-ERR COMMIT is not allowed") {
		t.Fatalf("commit in txn = %q", got)
	}
	a.do("ROLLBACK")
}

func TestBasicCommands(t *testing.T) {
	_, addr := startServer(t)
	c := dial(t, addr)
	if got := c.do("PING"); got != "+PONG" {
		t.Fatalf("PING = %q", got)
	}
	if got := c.do("SET name alice"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	if got := c.do("GET name"); got != "alice" {
		t.Fatalf("GET = %q", got)
	}
	if got := c.do("GET missing"); got != "(nil)" {
		t.Fatalf("GET missing = %q", got)
	}
	if got := c.do(`SET msg "hello world"`); got != "+OK" {
		t.Fatalf("quoted SET = %q", got)
	}
	if got := c.do("GET msg"); got != "hello world" {
		t.Fatalf("quoted GET = %q", got)
	}
	if got := c.do("EXISTS name"); got != ":1" {
		t.Fatalf("EXISTS = %q", got)
	}
	if got := c.do("DEL name"); got != ":1" {
		t.Fatalf("DEL = %q", got)
	}
	if got := c.do("DEL name"); got != ":0" {
		t.Fatalf("second DEL = %q", got)
	}
	if got := c.do("KEYS"); got != "[msg]" {
		t.Fatalf("KEYS = %q", got)
	}
	if got := c.do("BOGUS"); !strings.HasPrefix(got, "-ERR unknown command") {
		t.Fatalf("BOGUS = %q", got)
	}
	if got := c.do("SET onlykey"); !strings.HasPrefix(got, "-ERR wrong number") {
		t.Fatalf("arity = %q", got)
	}
	if got := c.do(`SET k "unterminated`); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("bad quoting = %q", got)
	}
	// The connection must still work after errors.
	if got := c.do("PING"); got != "+PONG" {
		t.Fatalf("PING after errors = %q", got)
	}
}

func TestVersionControlOverTheWire(t *testing.T) {
	_, addr := startServer(t)
	c := dial(t, addr)
	c.do("SET name alice")
	if got := c.do(`COMMIT "first"`); !strings.HasPrefix(got, "+") || len(got) != 65 {
		t.Fatalf("COMMIT = %q", got)
	}
	if got := c.do("BRANCH feature"); got != "+OK" {
		t.Fatalf("BRANCH = %q", got)
	}
	c.do("CHECKOUT feature")
	c.do("SET name bob")
	c.do(`COMMIT "rename"`)
	c.do("CHECKOUT main")
	if got := c.do("GET name"); got != "alice" {
		t.Fatalf("main should still have alice, got %q", got)
	}
	if got := c.do("MERGE feature"); !strings.HasPrefix(got, "+fast-forward") {
		t.Fatalf("MERGE = %q", got)
	}
	if got := c.do("GET name"); got != "bob" {
		t.Fatalf("after merge = %q", got)
	}
	if got := c.do("LOG"); !strings.Contains(got, "rename") {
		t.Fatalf("LOG = %q", got)
	}
	if got := c.do("SHOW"); !strings.Contains(got, "commit ") {
		t.Fatalf("SHOW = %q", got)
	}
}

func TestConcurrentClients(t *testing.T) {
	_, addr := startServer(t)
	const clients, ops = 8, 200
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			c := &client{t: t, conn: conn, r: bufio.NewReader(conn)}
			for j := 0; j < ops; j++ {
				key := fmt.Sprintf("c%d:k%d", id, j)
				if got := c.do("SET %s %d", key, j); got != "+OK" {
					t.Errorf("SET = %q", got)
					return
				}
				if got := c.do("GET %s", key); got != fmt.Sprint(j) {
					t.Errorf("GET %s = %q", key, got)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestRequestTooLong(t *testing.T) {
	eng, err := engine.Open(engine.Options{Dir: t.TempDir(), SyncWAL: false})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	srv := New(eng, Config{Addr: "127.0.0.1:0", MaxLine: 128})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	defer srv.Close()
	c := dial(t, srv.Addr())
	if got := c.do("SET k %s", strings.Repeat("v", 1024)); !strings.HasPrefix(got, "-ERR request line exceeds limit of 64 KiB") {
		t.Fatalf("got %q", got)
	}
}

func TestGracefulShutdownUnblocksClients(t *testing.T) {
	srv, addr := startServer(t)
	c := dial(t, addr)
	c.do("PING")
	done := make(chan struct{})
	go func() {
		srv.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return; a handler goroutine is stuck")
	}
}
