# Building a Versioned Key-Value Database in Go

This tutorial builds `staash`: a small TCP key-value database that stores its history the way Git does. You will start with an empty directory and finish with a program that speaks a network protocol, survives crashes, and can branch, commit, check out and merge its own data.

Every code block below is real, compiling Go. The finished project is about 2,300 lines of implementation and 1,600 lines of tests.

**How to read this.** Work through the phases in order; each one ends with a *Checkpoint* telling you exactly what should build, run and pass. If you get stuck, the checkpoint is the contract: get back to it before moving on.

**Prerequisites.** Go 1.21 or newer (`go version`), a Unix-like system (the durability code uses `fsync` on directories, which is a no-op on Windows), and a terminal. No external dependencies — the whole thing uses only the standard library.

---

## Phase 0 — Architecture and design

### What we are building

A single-process database server with two personalities fused into one:

| Redis-like half | Git-like half |
| --- | --- |
| `GET` / `SET` / `DEL` over TCP | content-addressed object store |
| in-memory keyspace, microsecond reads | immutable commits forming a DAG |
| write-ahead log for durability | branches, `CHECKOUT`, three-way `MERGE` |
| multiple concurrent clients | history you can reconstruct at any point |

The unifying idea is simple enough to state in one sentence:

> **The live keyspace is a mutable in-memory map; the committed keyspace is an immutable tree of content-addressed objects on disk; the write-ahead log is the bridge between them.**

Everything else in this tutorial is a consequence of that sentence.

### What we are deliberately *not* building

Being explicit about non-goals keeps the design honest.

- **Not a Redis clone.** No expiry, no data types beyond strings, no pub/sub, no clustering.
- **Not a Git clone.** No working tree, no staging area, no packfiles, no delta compression, no rename detection, no textual line-level merges.
- **Not distributed.** One process, one data directory. No replication, no consensus.
- **Not multi-user.** No authentication, no ACLs. Bind to localhost.
- **Not a memory-bounded database.** The entire keyspace lives in RAM. If your data does not fit in memory, this design does not apply to you.
- **Not serializable.** Our transactions are atomic and durable, but not serializable. Phase 9 states precisely what they do and do not guarantee.

### Terminology

Pin these down now; the rest of the tutorial uses them precisely.

| Term | Meaning in this project |
| --- | --- |
| **key / value** | a `string` → `string` pair in the live keyspace |
| **mutation** | one absolute change: "set K to V" or "delete K" |
| **batch** | a group of mutations applied atomically |
| **WAL** | write-ahead log; an append-only file of batches not yet committed |
| **object** | an immutable byte string on disk, named by the hash of its contents |
| **blob** | an object holding one value |
| **tree** | an object holding a sorted list of `name → object ID` entries |
| **snapshot** | the root tree of one complete keyspace state |
| **commit** | an object holding a root tree ID, parent commit IDs, a timestamp and a message |
| **ref** | a mutable file containing one commit ID (`refs/heads/main`) |
| **HEAD** | a file naming the branch we are currently on |
| **dirty** | the set of keys mutated since the last commit |

### Components and data flow

```
   client         client         client
      |              |              |
      +--------------+--------------+
                     |  TCP, one goroutine per connection
                     v
            +------------------+
            |  server/session  |  parse a line, dispatch a command
            +------------------+
                     |
                     v
            +------------------+
            |      engine      |  the only component that knows the
            +------------------+  whole picture; owns write ordering
              |    |         |
     reads    |    | writes  | commits / branches / merges
              v    v         v
        +--------+  +-----+  +----------------+   +--------+
        | store  |  | WAL |  |  object store  |<->|  refs  |
        | (RAM)  |  |(disk)| |  (disk, immut.)|   | (disk) |
        +--------+  +-----+  +----------------+   +--------+
```

Read path: `session → engine → store`. That is it — a read never touches the disk, because the entire committed state was loaded into memory at startup.

Write path: `session → engine → WAL (append + fsync) → store`. The log is written **before** memory. If the process dies between the two, recovery replays the record and reaches the same state. If we did it the other way round, an acknowledged write could vanish.

Commit path: `engine → object store (blobs, trees, commit) → refs → WAL reset`.

### The commit graph

```
            refs/heads/main
                   |
                   v
   C1  <-------  C2  <-------  C3            HEAD -> main
   |             |             |
   root          root          root
   |             |             |
 Tree A        Tree B        Tree C          <- root trees (256 shards max)
 /    \        /    \        /    \
Sh"3a" Sh"c1" Sh"3a" Sh"c1'" Sh"3a" Sh"c1'"  <- shard trees; note Sh"3a" is
   |     |       |      |      |     |          *the same object* in all three
 blobs blobs   blobs  blobs  blobs blobs
```

Arrows point from child to parent, which is the direction the data actually goes: a commit names its parent, never the reverse. That is what makes commits immutable — nothing has to be rewritten when history grows.

The shared `Sh"3a"` node is the whole trick. Commits store the *changed* shards and reuse the pointers to the rest, so a commit costs time proportional to what changed, not to how much data exists.

### Invariants

State these once; the code will rely on them constantly.

1. **Objects are immutable.** Once a file exists in `objects/`, its bytes never change. Writers stage into `objects/tmp/` and `rename(2)` into place.
2. **Object names are content hashes.** `id = SHA-256("<kind> <length>\0<payload>")`. Identical content is stored once.
3. **Tree encodings are canonical.** Entries are sorted by name, so two identical keyspaces always produce byte-identical trees and therefore the same ID.
4. **WAL records are absolute, never relative.** Replaying a record twice is the same as replaying it once. This is what makes recovery idempotent.
5. **A branch ref update is the commit point.** Objects written before it are harmless garbage if we crash; the rename of the ref file is the moment history changes.
6. **The WAL holds only uncommitted work.** It is truncated after every successful commit, so it never grows without bound while the database is being committed.
7. **In-memory state == committed state + WAL.** This is the recovery equation, and it is the reason `CHECKOUT` refuses to run while the WAL is non-empty.

### Project structure and package boundaries

```
staash/
├── go.mod
├── cmd/
│   └── staash/
│       └── main.go          flags, signals, wiring
└── internal/
    ├── fsutil/              atomic file write, directory fsync
    ├── store/               in-memory map + mutation type
    ├── wal/                 append-only log of batches
    ├── object/              hashing, blobs, trees, commits, on-disk object store
    ├── refs/                HEAD and refs/heads/*
    ├── protocol/            request parsing, reply encoding
    ├── engine/              the database: ties everything together
    └── server/              TCP listener, connections, command dispatch
```

The dependency graph is acyclic and shallow:

```
server ──> engine ──> store
   |          |  ├──> wal ──> store
   └──> protocol |  ├──> object ──> fsutil
                 |  └──> refs ──> object, fsutil
                 v
```

Some notes on the choices, because package layout is a design decision, not a formality:

- **`internal/` for everything.** Nothing here is a reusable library, and marking it `internal` says so to the compiler and to the reader. There is no `pkg/` directory in this project — adding one would advertise a public API we are not prepared to support.
- **No `commit/`, `branch/` or `transaction/` package.** A commit *is* an object, so `object.Commit` lives with the other object types. A branch is a file with a commit ID in it, so it lives in `refs`. Transactions turned out to be per-connection state with no persistent form at all, so they live in `server/session.go`. Three packages that would each have contained one struct is worse than three files.
- **`wal` imports `store`.** The log records `store.Mutation` values. The alternative — a fourth package existing only to hold a three-field struct — buys nothing. When two packages genuinely share a vocabulary type, letting the lower-level one import it is fine.
- **`engine` is deliberately the fat package.** Write ordering, tree building and merging all need to see the store, the log, the objects and the refs at once. Splitting them would mean either an import cycle or a pile of interfaces invented to break one. We will revisit this in Phase 14.
- **No interfaces yet.** There is exactly one store implementation, one WAL implementation and one object store implementation. An interface with a single implementation is a guess about the future; we will add one only when a second implementation actually shows up (Phase 12 discusses when that becomes justified).

### Set up the repository

```bash
mkdir staash && cd staash
git init
go mod init github.com/example/staash
```

> **Note.** Replace `github.com/example/staash` with your own module path. Every import in this tutorial starts with that prefix; if you choose a different one, substitute it everywhere.

Create the directory skeleton:

```bash
mkdir -p cmd/staash
mkdir -p internal/store internal/wal internal/object internal/refs
mkdir -p internal/protocol internal/engine internal/server internal/fsutil
```

Add a `.gitignore`:

```gitignore
# .gitignore
/staash
/data/
*.test
*.prof
```

```bash
git add . && git commit -m "chore: initialise go module and project layout"
```

---

## Phase 1 — A concurrent in-memory store

### Why a plain map is not enough

Go maps are not safe for concurrent use. Two goroutines writing the same map at the same time is not "usually fine" or "occasionally wrong" — the runtime detects it and kills the process:

```
fatal error: concurrent map writes
```

This is not a panic you can recover from. Even concurrent *read-while-write* is undefined: the runtime may be resizing the bucket array under you, and the reader can follow a pointer that is being rewritten.

Two standard fixes:

| Option | Pros | Cons |
| --- | --- | --- |
| `sync.RWMutex` around a normal map | simple, predictable, readers run in parallel | one lock for the whole keyspace |
| `sync.Map` | lock-free for some patterns | worse for write-heavy loads, no ordered iteration, no atomic multi-key updates |

**We choose the `RWMutex`.** `sync.Map` is tuned for caches where entries are written once and read many times by disjoint goroutines; our workload is a mix, and — more decisively — we will need *multi-key atomic updates* for transactions in Phase 9. You cannot get that from `sync.Map` at all. A single mutex also makes the correctness argument short enough to state in one sentence, which is worth a lot.

The cost: all writes serialise on one lock. Phase 13 measures it, and Phase 14 explains how sharding would fix it.

### `internal/store/store.go`

```go
// Package store implements the in-memory key/value map that answers reads.
package store

import (
	"sort"
	"sync"
)

// Store is a concurrency-safe map[string]string.
//
// Invariant: every exported method either takes s.mu for reading or for
// writing. No caller ever receives a reference to the internal map.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func New() *Store {
	return &Store{data: make(map[string]string)}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Del removes key and reports whether it existed.
func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[key]
	delete(s.data, key)
	return ok
}

func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.data[key]
	return ok
}

// Keys returns every key in sorted order. Sorting is not free, but it makes
// the command output deterministic, which makes tests much easier to write.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
```

Four design points worth naming:

1. **`Get` returns `(string, bool)`, not `(string, error)`.** A missing key is not an error; it is a normal answer. Reserve `error` for things that went wrong, so callers do not have to distinguish `ErrNotFound` from real failures.
2. **`Del` returns whether the key existed.** Redis clients expect this, and it costs one extra map lookup. Note that `delete` on a missing key is already a no-op in Go — the lookup exists only for the return value.
3. **`Keys` copies.** Returning a slice that aliases internal state would let a caller read it after the lock is released. Every method here returns values, never references into `s.data`. When we add `Snapshot` in Phase 6 it will copy for the same reason.
4. **`defer` on every unlock.** Slightly slower than a manual `Unlock`, but it survives future edits that add an early `return`. We will revisit exactly one of these in Phase 13 if profiling says so — and it will not.

### `internal/store/store_test.go`

```go
package store

import (
	"fmt"
	"sync"
	"testing"
)

func TestStoreBasics(t *testing.T) {
	s := New()

	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected miss on empty store")
	}
	s.Set("a", "1")
	if v, ok := s.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a) = %q,%v", v, ok)
	}
	s.Set("a", "2")
	if v, _ := s.Get("a"); v != "2" {
		t.Fatalf("overwrite failed: %q", v)
	}
	if !s.Del("a") {
		t.Fatal("Del should report the key existed")
	}
	if s.Del("a") {
		t.Fatal("second Del should report false")
	}
	if s.Exists("a") {
		t.Fatal("key should be gone")
	}
}

func TestKeysAreSorted(t *testing.T) {
	s := New()
	for _, k := range []string{"c", "a", "b"} {
		s.Set(k, k)
	}
	got := s.Keys()
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

// Run with -race: without the mutex this test reliably reports a data race
// and often panics with "concurrent map writes".
func TestConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				k := fmt.Sprintf("k%d", j)
				s.Set(k, fmt.Sprintf("%d-%d", i, j))
				s.Get(k)
				s.Keys()
			}
		}(i)
	}
	wg.Wait()
	if s.Len() != 500 {
		t.Fatalf("len = %d, want 500", s.Len())
	}
}
```

Try the experiment: comment out the `Lock`/`Unlock` pair in `Set` and run `go test -race ./internal/store/`. You will get a race report naming both goroutines and both stack traces. Put it back. This is the single most useful Go tool you will use in this project.

### Checkpoint 1

```bash
go test ./internal/store/
go test -race ./internal/store/
```

Both should pass. Nothing runs yet — there is no `main` — but the foundation exists.

```bash
git add . && git commit -m "feat: add concurrent in-memory key-value store"
```

---

## Phase 2 — A TCP server

Now we put the store on a socket. This phase is about *networking mechanics*; the protocol stays crude on purpose and gets replaced in Phase 3.

### The concepts

**`net.Listener`** is a bound socket. `net.Listen("tcp", addr)` returns one; `Accept()` blocks until a client connects and returns a `net.Conn`.

**`net.Conn`** is a bidirectional byte stream implementing `io.Reader` and `io.Writer`. Two things about it trip up everyone the first time:

- **TCP has no message boundaries.** `Read` returns whatever bytes have arrived — possibly half a command, possibly three commands, possibly one byte. `Write` may also write fewer bytes than you gave it. This is why we need *framing* (Phase 3) and why we always wrap connections in `bufio`.
- **Reads block forever by default.** A client that connects and says nothing holds a goroutine and a file descriptor indefinitely. `SetReadDeadline` fixes that.

**Goroutine per connection** is the standard Go server shape. Goroutines start at about 2 KB of stack, so thousands of idle connections are affordable. The alternative — an event loop with `epoll` — is what the Go runtime is already doing for you underneath.

**Connection lifecycle:**

```
Accept ──> register conn ──> go handleConn
                                  |
                          +-------v--------+
                          | set read dline |<-----+
                          | read one line  |      |
                          | parse+execute  |      | loop
                          | write reply    |      |
                          | flush          |------+
                          +-------+--------+
                                  | EOF, timeout, QUIT, or write error
                                  v
                          deregister, Close, wg.Done
```

**Graceful shutdown** is the subtle part. Closing the listener stops new connections but does nothing to goroutines blocked in `Read`. The trick: keep a set of live connections, and `Close()` them. A blocked `Read` on a closed connection returns immediately with an error, the handler falls out of its loop, and a `sync.WaitGroup` tells us when they are all gone.

### `internal/server/server.go` (first version)

```go
package server

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/example/staash/internal/store"
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
		cfg.IdleTimeout = 10 * time.Minute
	}
	return &Server{cfg: cfg, st: st, conns: make(map[net.Conn]struct{})}
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

// Addr is the bound address; only valid after Listen.
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
			// Transient accept errors (EMFILE, ECONNABORTED) should not kill
			// the server.
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
```

> **Warning.** `s.conns` is touched by the accept goroutine, by every handler goroutine, and by whoever calls `Close`. It needs `s.mu`. Note also that `Close` copies the connection set *while holding the lock* and then closes the copies *after releasing it* — closing a connection wakes a handler that immediately calls `trackConn(..., false)`, which wants the same lock. Holding it across the `Close` calls would deadlock.

Now the connection handler and a throwaway command executor:

```go
func (s *Server) handleConn(conn net.Conn) {
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	for {
		if err := conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout)); err != nil {
			return
		}
		line, err := r.ReadString('\n')
		if err != nil {
			return // EOF, timeout, or the client vanished
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if err := conn.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return
		}
		if quit := s.execute(w, fields); quit {
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
		fmt.Fprintf(w, format+"\r\n", a...)
	}
	switch cmd {
	case "PING":
		reply("PONG")
	case "QUIT":
		reply("OK")
		return true
	case "SET":
		if len(args) != 2 {
			reply("ERR wrong number of arguments for SET")
			break
		}
		s.st.Set(args[0], args[1])
		reply("OK")
	case "GET":
		if len(args) != 1 {
			reply("ERR wrong number of arguments for GET")
			break
		}
		if v, ok := s.st.Get(args[0]); ok {
			reply("%s", v)
		} else {
			reply("(nil)")
		}
	case "DEL":
		if len(args) != 1 {
			reply("ERR wrong number of arguments for DEL")
			break
		}
		if s.st.Del(args[0]) {
			reply("1")
		} else {
			reply("0")
		}
	case "EXISTS":
		if len(args) != 1 {
			reply("ERR wrong number of arguments for EXISTS")
			break
		}
		if s.st.Exists(args[0]) {
			reply("1")
		} else {
			reply("0")
		}
	case "KEYS":
		keys := s.st.Keys()
		reply("%d", len(keys))
		for _, k := range keys {
			reply("%s", k)
		}
	default:
		reply("ERR unknown command %s", cmd)
	}
	return false
}
```

Three notes on the buffered writer:

- **`bufio.Writer` solves the partial-write problem.** `bufio.Writer.Write` never returns a short write without an error, and it batches small replies into one syscall.
- **Flush once per command, not per fragment.** A reply is only on the wire after `Flush`. Forgetting it is the classic "my server hangs" bug.
- **A flush error ends the connection.** If we cannot write, the client is gone or the socket is broken; there is nothing useful left to do.

### `cmd/staash/main.go` (first version)

```go
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/example/staash/internal/server"
	"github.com/example/staash/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "TCP address to listen on")
	flag.Parse()

	st := store.New()
	srv := server.New(st, server.Config{Addr: *addr})
	if err := srv.Listen(); err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("listening on %s", srv.Addr())

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil {
			log.Printf("serve: %v", err)
		}
	case sig := <-sigCh:
		log.Printf("received %s, shutting down", sig)
		srv.Close()
	}
}
```

The `select` over "serve failed" and "signal arrived" is the whole shutdown story: whichever happens first wins, and `srv.Close()` blocks until every handler goroutine has returned.

### Try it

```bash
go run ./cmd/staash
```

In another terminal:

```bash
$ nc 127.0.0.1 6380
PING
PONG
SET name alice
OK
GET name
alice
GET nope
(nil)
KEYS
1
name
QUIT
OK
```

Press `Ctrl+C` in the server terminal; it should log the signal and exit immediately rather than hanging.

### What can go wrong here

| Failure | Behaviour |
| --- | --- |
| Client disconnects mid-command | `ReadString` returns `io.EOF`; handler returns; goroutine exits |
| Client connects and goes silent | read deadline fires after `IdleTimeout`; connection closed |
| Client sends 10 GB with no newline | **`ReadString` grows a buffer without bound.** Fixed in Phase 3 |
| Value contains a space | `strings.Fields` splits it into two args → wrong. Fixed in Phase 3 |
| Value contains a newline | reply is unparseable by the client. Fixed in Phase 3 |
| Server is killed | all data lost — it is still purely in memory. Fixed in Phase 4 |

Two of those are protocol bugs and one is a memory-exhaustion vector, which is exactly the agenda for the next phase.

### Checkpoint 2

- `go build ./...` succeeds and `go run ./cmd/staash` serves `nc`.
- `Ctrl+C` shuts down cleanly.

```bash
git add . && git commit -m "feat: serve the store over TCP with graceful shutdown"
```

---

## Phase 3 — A real protocol

### Why protocols need framing

TCP delivers a byte stream. `SET msg hello` and `SET msg hel` + `lo` are indistinguishable at the transport layer. A protocol's job is to tell the reader where one message ends and the next begins. There are exactly three ways to do it:

| Framing | Example | Trade-off |
| --- | --- | --- |
| **Delimiter** | line-based, `\n` terminated | trivial to write and to debug by hand; the delimiter cannot appear in the data |
| **Length prefix** | `$5\r\nhello\r\n` | binary safe, no escaping, no scanning; unreadable without a tool |
| **Self-describing** | JSON, protobuf | flexible; heavier to parse, and JSON still needs framing on a stream |

Redis solves this asymmetrically, and so will we:

- **Requests** are lines. Humans type them, `nc` and `telnet` work, and the argument syntax can be as simple as we like.
- **Replies** are length-prefixed. Values may contain anything, including newlines, and clients never have to guess.

That asymmetry is not a compromise — it matches the actual constraint on each side. Requests come from humans and small clients; replies carry arbitrary user data.

### The reply format (RESP, simplified)

| Prefix | Meaning | Example |
| --- | --- | --- |
| `+` | simple string | `+OK\r\n` |
| `-` | error | `-ERR unknown command FOO\r\n` |
| `:` | integer | `:1\r\n` |
| `$` | bulk string, length-prefixed | `$5\r\nhello\r\n` |
| `$-1` | nil | `$-1\r\n` |
| `*` | array of *n* following replies | `*2\r\n$1\r\na\r\n$1\r\nb\r\n` |

A client reads one line, switches on the first byte, and for `$` and `*` reads exactly as many bytes or elements as announced. No ambiguity, no escaping.

### The request format

```
line     := token (SP+ token)*
token    := bare | quoted
bare     := run of non-space, non-quote characters
quoted   := '"' ( '\' any | any-but-quote )* '"'
```

Quoting exists so `SET greeting "hello world"` works. Escaping exists so quoted values can contain quotes. That is all — no nesting, no types, no binary-safe request path. If you need to store bytes that cannot survive a text line, that is a genuine limitation of this protocol, listed under future work in Phase 14.

> **Note.** We are deliberately *not* implementing RESP request arrays (`*3\r\n$3\r\nSET\r\n...`). It would make the request path binary safe, but it would also make hand-testing impossible and add a second parser before we have a single user. If you want it later, the shape of the change is clear: swap `readLine` + `Parse` for a RESP reader, keep everything downstream identical.

### `internal/protocol/protocol.go`

```go
// Package protocol implements the wire format: inline text requests and
// RESP-inspired replies.
package protocol

import (
	"errors"
	"strings"
	"unicode"
)

// Command is a parsed request line.
type Command struct {
	Name string   // upper-cased verb, e.g. "SET"
	Args []string // arguments, quotes removed
}

var (
	ErrUnterminatedQuote = errors.New("unbalanced quotes in request")
	ErrEmptyCommand      = errors.New("empty command")
)

// Parse splits one request line into a command.
func Parse(line string) (Command, error) {
	tokens, err := tokenize(line)
	if err != nil {
		return Command{}, err
	}
	if len(tokens) == 0 || tokens[0] == "" {
		// `""` tokenizes to a single empty token; an empty verb is not a
		// command. Found by the fuzz test in Phase 12.
		return Command{}, ErrEmptyCommand
	}
	var args []string
	if len(tokens) > 1 {
		args = tokens[1:]
	}
	return Command{Name: strings.ToUpper(tokens[0]), Args: args}, nil
}

func tokenize(line string) ([]string, error) {
	var (
		tokens []string
		cur    strings.Builder
		inTok  bool
	)
	runes := []rune(line)
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		switch {
		case unicode.IsSpace(c):
			if inTok {
				tokens = append(tokens, cur.String())
				cur.Reset()
				inTok = false
			}
		case c == '"':
			inTok = true
			i++
			closed := false
			for ; i < len(runes); i++ {
				if runes[i] == '\\' && i+1 < len(runes) {
					i++
					cur.WriteRune(unescape(runes[i]))
					continue
				}
				if runes[i] == '"' {
					closed = true
					break
				}
				cur.WriteRune(runes[i])
			}
			if !closed {
				return nil, ErrUnterminatedQuote
			}
		default:
			inTok = true
			cur.WriteRune(c)
		}
	}
	if inTok {
		tokens = append(tokens, cur.String())
	}
	return tokens, nil
}

func unescape(c rune) rune {
	switch c {
	case 'n':
		return '\n'
	case 't':
		return '\t'
	case 'r':
		return '\r'
	default:
		return c
	}
}
```

The `inTok` flag is what distinguishes an empty token that was explicitly written as `""` from no token at all — `COMMIT ""` must produce one empty argument, not zero arguments.

> **Note.** The `tokens[0] == ""` guard was not in the first draft of this code. The fuzz test in Phase 12 found that `""` parsed into a command whose name was the empty string, which then fell through to `unknown command ` in the dispatcher. It is a harmless bug, but it is a real one, and it is the kind of thing fuzzing finds in thirty seconds and code review does not.

### `internal/protocol/reply.go`

```go
package protocol

import (
	"bufio"
	"strconv"
	"strings"
)

// Writer encodes replies. Length-prefixed bulk strings are what make the
// protocol binary safe: the reader never has to scan for a delimiter that
// might appear in the data.
type Writer struct {
	w *bufio.Writer
}

func NewWriter(w *bufio.Writer) *Writer { return &Writer{w: w} }

func (w *Writer) Simple(s string) error {
	_, err := w.w.WriteString("+" + s + "\r\n")
	return err
}

func (w *Writer) OK() error { return w.Simple("OK") }

// Error writes an error reply. Newlines are stripped because a simple-format
// reply is terminated by CRLF.
func (w *Writer) Error(msg string) error {
	msg = strings.NewReplacer("\r", " ", "\n", " ").Replace(msg)
	_, err := w.w.WriteString("-ERR " + msg + "\r\n")
	return err
}

func (w *Writer) Int(n int64) error {
	_, err := w.w.WriteString(":" + strconv.FormatInt(n, 10) + "\r\n")
	return err
}

func (w *Writer) Bulk(s string) error {
	if _, err := w.w.WriteString("$" + strconv.Itoa(len(s)) + "\r\n"); err != nil {
		return err
	}
	if _, err := w.w.WriteString(s); err != nil {
		return err
	}
	_, err := w.w.WriteString("\r\n")
	return err
}

func (w *Writer) Nil() error {
	_, err := w.w.WriteString("$-1\r\n")
	return err
}

// ArrayHeader announces n following elements.
func (w *Writer) ArrayHeader(n int) error {
	_, err := w.w.WriteString("*" + strconv.Itoa(n) + "\r\n")
	return err
}

// StringArray is the common case: an array of bulk strings.
func (w *Writer) StringArray(items []string) error {
	if err := w.ArrayHeader(len(items)); err != nil {
		return err
	}
	for _, it := range items {
		if err := w.Bulk(it); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) Flush() error { return w.w.Flush() }
```

`Error` is the one place that sanitises its input, because an error message often contains user data (`unknown command <whatever they typed>`) and a simple-format reply is delimited by CRLF. A user who sends a command name containing `\r\n` would otherwise be able to inject a fake second reply into the stream — a small-scale version of HTTP response splitting.

### Bounding the request size

`bufio.Reader.ReadString` grows its buffer until it finds the delimiter. A client that sends gigabytes with no newline will make the server allocate gigabytes. Replace it with a bounded reader. Add to `internal/server/server.go`:

```go
var errLineTooLong = errors.New("request line exceeds limit")

// readLine reads up to and including '\n', enforcing a maximum length so a
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
```

`ReadSlice` returns `bufio.ErrBufferFull` instead of growing, handing back whatever fits in the fixed 4 KB buffer. We accumulate chunks ourselves and bail out once the total crosses the limit. `TrimRight` accepts both `\n` and `\r\n`, so clients on either convention work.

When the limit is hit we send one error and **close the connection**. We cannot keep going: the rest of that oversized line is still in the socket and would be parsed as garbage commands. Resynchronising a text protocol after a framing error is not worth the complexity when the client is already misbehaving.

### Rewriting the server to use the protocol package

Update the imports and `Config` in `internal/server/server.go`:

```go
import (
	"bufio"
	"errors"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/example/staash/internal/protocol"
	"github.com/example/staash/internal/store"
)

// Config holds tunables with sane zero-value fallbacks.
type Config struct {
	Addr         string
	MaxLine      int           // maximum request line in bytes
	IdleTimeout  time.Duration // close connections that say nothing for this long
	WriteTimeout time.Duration
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
		c.IdleTimeout = 10 * time.Minute
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = log.Default()
	}
}
```

Change `New` to call it:

```go
func New(st *store.Store, cfg Config) *Server {
	cfg.withDefaults()
	return &Server{cfg: cfg, st: st, conns: make(map[net.Conn]struct{})}
}
```

And replace `handleConn` entirely:

```go
func (s *Server) handleConn(conn net.Conn) {
	reader := bufio.NewReaderSize(conn, 4096)
	bw := bufio.NewWriter(conn)
	w := protocol.NewWriter(bw)
	sess := newSession(s.st)

	for {
		if err := conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout)); err != nil {
			return
		}
		line, err := readLine(reader, s.cfg.MaxLine)
		if err != nil {
			if errors.Is(err, errLineTooLong) {
				_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
				_ = w.Error("request too long")
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
```

Finally delete `execute` and create the session file.

### `internal/server/session.go` (first version)

A *session* is the per-connection state. Right now that is nothing at all, but Phase 9 will put an open transaction here, and having the seam already in place means that change touches one file.

```go
package server

import (
	"github.com/example/staash/internal/protocol"
	"github.com/example/staash/internal/store"
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
	argErr := func() error { return w.Error("wrong number of arguments for " + cmd.Name) }

	switch cmd.Name {
	case "PING":
		if n == 1 {
			return false, w.Bulk(cmd.Args[0])
		}
		return false, w.Simple("PONG")

	case "QUIT":
		return true, w.OK()

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

	default:
		return false, w.Error("unknown command " + cmd.Name)
	}
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
```

**The two kinds of error.** This distinction runs through the whole server and is worth internalising: a *command* error is data the client asked for and gets as `-ERR ...`; a *transport* error means the socket is broken and there is no point writing anything else. Conflating them either kills connections over typos or writes into a dead socket forever.

### `internal/protocol/protocol_test.go`

```go
package protocol

import (
	"bufio"
	"reflect"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		in      string
		want    Command
		wantErr bool
	}{
		{in: "PING", want: Command{Name: "PING"}},
		{in: "set a 1", want: Command{Name: "SET", Args: []string{"a", "1"}}},
		{in: "  SET   a    1  ", want: Command{Name: "SET", Args: []string{"a", "1"}}},
		{in: `SET msg "hello world"`, want: Command{Name: "SET", Args: []string{"msg", "hello world"}}},
		{in: `SET msg "she said \"hi\""`, want: Command{Name: "SET", Args: []string{"msg", `she said "hi"`}}},
		{in: `COMMIT ""`, want: Command{Name: "COMMIT", Args: []string{""}}},
		{in: `SET a "unclosed`, wantErr: true},
		{in: "   ", wantErr: true},
	}
	for _, tc := range tests {
		got, err := Parse(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("Parse(%q) = %+v, want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%q): %v", tc.in, err)
			continue
		}
		if got.Name != tc.want.Name || !reflect.DeepEqual(got.Args, tc.want.Args) {
			t.Errorf("Parse(%q) = %+v, want %+v", tc.in, got, tc.want)
		}
	}
}

func TestWriter(t *testing.T) {
	var sb strings.Builder
	bw := bufio.NewWriter(&sb)
	w := NewWriter(bw)
	_ = w.OK()
	_ = w.Int(42)
	_ = w.Bulk("hi")
	_ = w.Nil()
	_ = w.Error("bad\nthing")
	_ = w.StringArray([]string{"a", "b"})
	_ = w.Flush()

	want := "+OK\r\n:42\r\n$2\r\nhi\r\n$-1\r\n-ERR bad thing\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n"
	if sb.String() != want {
		t.Fatalf("got %q\nwant %q", sb.String(), want)
	}
}
```

> **Note.** `reflect.DeepEqual(nil, []string{})` is `false`. That is why `Parse` returns a nil `Args` slice rather than an empty one for zero-argument commands — an arbitrary choice, but one the tests depend on, so it is worth making deliberately.

### Checkpoint 3

```bash
go build ./... && go test ./...
go run ./cmd/staash
```

```bash
$ nc 127.0.0.1 6380
SET greeting "hello world"
+OK
GET greeting
$11
hello world
GET nope
$-1
KEYS
*1
$8
greeting
```

Values with spaces now survive the round trip, and replies are unambiguous.

```bash
git add . && git commit -m "feat: add text protocol with RESP-style replies"
```

---

## Phase 4 — Persistence and the write-ahead log

Everything so far is lost on restart. This phase fixes that, and it is the longest phase in the tutorial because durability is where the interesting failure modes live.

### Why not just save the map?

The obvious approach — serialise the whole map to a file after every write:

```go
// Do not do this.
func (s *Store) Set(k, v string) {
	s.mu.Lock()
	s.data[k] = v
	blob, _ := json.Marshal(s.data)
	os.WriteFile("db.json", blob, 0644)
	s.mu.Unlock()
}
```

Four problems, in increasing order of severity:

1. **O(n) per write.** A one-byte change to a 1 GB database writes 1 GB. Throughput collapses as the database grows.
2. **Write amplification.** Even at moderate sizes you are pushing megabytes through the page cache per `SET`.
3. **It is not atomic.** `os.WriteFile` truncates the file and then writes. A crash halfway leaves a truncated JSON file: not the old state, not the new state, *no* state. You have destroyed the database in the act of saving it.
4. **It is not durable.** Without `fsync`, `WriteFile` returning means the kernel has the bytes, not the disk. A power cut loses them.

Problem 3 is fixable (write to a temp file and rename — that is `fsutil.WriteFileAtomic`, coming in Phase 5). Problems 1 and 2 are fundamental to the approach.

### The write-ahead log

The standard answer, used by every serious database:

> Before changing memory, append a description of the change to a file. On restart, replay the file.

Appending is O(size of the change) and sequential, which is the one access pattern that is fast on every storage device ever made. The log never needs to be read except during recovery.

**Our variant has one twist.** In most databases the WAL grows until a checkpoint flushes the in-memory state to the main data files. In our design, *commits are the checkpoint*. After a successful commit the entire state is recoverable from immutable objects, so the WAL is truncated to zero. The log therefore holds exactly "the mutations since the last commit" — usually a handful of records.

This gives us the recovery equation from Phase 0:

```
state after restart  =  materialize(HEAD commit)  +  replay(WAL)
```

### Record format

```
+----------------+----------------+------------------+
| payload length | CRC32C of      | payload          |
| uint32 LE      | payload uint32 | (length bytes)   |
+----------------+----------------+------------------+
        4               4              variable
```

Payload = one batch of mutations:

```
uvarint count
repeated count times:
    op (1 byte: 1=SET, 2=DEL)
    uvarint keylen   key bytes
    uvarint vallen   value bytes  (empty for DEL)
```

Design decisions in that layout:

- **Length first, so a reader knows how much to read** without scanning for a delimiter — and binary data cannot contain a "delimiter" anyway.
- **CRC32C over the payload**, not the header. If the header itself is torn we will see a nonsensical length and stop; if the payload is torn or bit-rotted the checksum catches it. Castagnoli (`crc32.Castagnoli`) has a hardware instruction on x86-64 and ARM64, so it is nearly free.
- **A batch per record, not a mutation per record.** This is what makes transactions atomic on disk: either the whole record verifies and the whole batch is applied, or none of it is.
- **Varints for lengths**, because most keys and values are short and a fixed uint32 would triple the size of a small record.
- **No timestamps, no sequence numbers, no transaction IDs.** We do not need them: the log is replayed from the beginning, in order, exactly once. Adding fields "just in case" is how formats rot.

### `internal/store` — add the mutation type

The WAL records mutations, so define them where they belong. Add to the top of `internal/store/store.go`, just after the imports:

```go
// Op identifies the kind of a Mutation.
type Op byte

const (
	OpSet Op = 1
	OpDel Op = 2
)

// Mutation is a single absolute change to the keyspace. It is absolute, not
// relative: applying the same mutation twice produces the same result. The WAL
// relies on that property during replay.
type Mutation struct {
	Op    Op
	Key   string
	Value string // empty for OpDel
}
```

And add a method at the bottom:

```go
// ApplyBatch applies every mutation under a single write lock, so no reader
// can observe a half-applied batch. This is what makes transactions atomic
// with respect to other clients.
func (s *Store) ApplyBatch(muts []Mutation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range muts {
		switch m.Op {
		case OpSet:
			s.data[m.Key] = m.Value
		case OpDel:
			delete(s.data, m.Key)
		}
	}
}
```

> **Invariant 4, made concrete.** `Mutation` says "key K now has value V", not "increment K". That is why replaying a record twice is harmless, and it is the reason Phase 11 can be relaxed about crashing between the ref update and the log truncation.

Add the test:

```go
func TestApplyBatchIsAtomic(t *testing.T) {
	s := New()
	s.Set("x", "old")
	s.ApplyBatch([]Mutation{
		{Op: OpSet, Key: "x", Value: "new"},
		{Op: OpSet, Key: "y", Value: "y"},
		{Op: OpDel, Key: "z"},
	})
	if v, _ := s.Get("x"); v != "new" {
		t.Fatalf("x = %q", v)
	}
	if !s.Exists("y") {
		t.Fatal("y missing")
	}
}
```

### `internal/wal/wal.go`

```go
// Package wal implements an append-only write-ahead log of store mutations.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/example/staash/internal/store"
)

const headerSize = 8

// MaxRecordSize bounds how much memory a single corrupt length field can make
// us allocate.
const MaxRecordSize = 64 << 20 // 64 MiB

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt marks a record that is present but does not verify.
var ErrCorrupt = errors.New("wal: corrupt record")

// WAL is an open log file. It is not safe for concurrent use; the engine
// serialises access.
type WAL struct {
	f      *os.File
	path   string
	offset int64
	sync   bool
}
```

Note `MaxRecordSize`. Without it, a corrupted length field reading `0xFFFFFFFF` would make recovery try to allocate 4 GB. Every parser that reads a length off disk needs a bound like this.

Now `Open`, which does recovery as part of opening:

```go
// Open opens (creating if needed) the log at path, replays it, truncates any
// trailing garbage, and positions the file at the end.
//
// syncOnAppend controls durability: true means fsync after every batch (slow,
// safe against power loss), false means rely on the OS page cache (fast, safe
// only against process crashes).
func Open(path string, syncOnAppend bool) (*WAL, [][]store.Mutation, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	batches, good, err := replay(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if size != good {
		// Trailing bytes are an incomplete or corrupt record: a write that was
		// interrupted by a crash. Dropping them is correct because the client
		// never received an acknowledgement for that batch.
		if err := f.Truncate(good); err != nil {
			f.Close()
			return nil, nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, nil, err
		}
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return nil, nil, err
	}
	return &WAL{f: f, path: path, offset: good, sync: syncOnAppend}, batches, nil
}
```

Returning the replayed batches from `Open` couples recovery to opening on purpose: there is no way to accidentally use a log you have not replayed.

The replay loop:

```go
// replay reads records until EOF or the first unreadable record. It returns
// the decoded batches and the offset just past the last valid record.
func replay(f *os.File) ([][]store.Mutation, int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}
	var (
		batches [][]store.Mutation
		off     int64
		header  [headerSize]byte
	)
	for {
		if _, err := io.ReadFull(f, header[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return batches, off, nil // clean end, or torn header
			}
			return nil, 0, err
		}
		length := binary.LittleEndian.Uint32(header[0:4])
		want := binary.LittleEndian.Uint32(header[4:8])
		if length == 0 || length > MaxRecordSize {
			return batches, off, nil // nonsense length: treat as torn
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return batches, off, nil // torn payload
			}
			return nil, 0, err
		}
		if crc32.Checksum(payload, castagnoli) != want {
			return batches, off, nil // bit rot or torn write
		}
		muts, err := decodeBatch(payload)
		if err != nil {
			return batches, off, nil
		}
		batches = append(batches, muts)
		off += headerSize + int64(length)
	}
}
```

**The central decision of this function** is that every kind of damage produces the same response: *stop, and report the offset of the last good record*. Short read, absurd length, bad checksum, undecodable payload — all treated as "the log ends here". That is safe only because of a specific property of an append-only log: damage can only be at the end. Nothing ever rewrites earlier bytes, so a bad record means the process died mid-append, and everything before it is intact.

There is a real error case that is *not* treated this way: an I/O error from the operating system (`EIO`, a disk that has gone away). That is returned, and `Open` fails. Silently truncating a database because the disk is having a bad day would be much worse than refusing to start.

Now append and reset:

```go
// Append writes one batch. On return (with sync enabled) the batch is durable.
func (w *WAL) Append(muts []store.Mutation) error {
	if len(muts) == 0 {
		return nil
	}
	payload := encodeBatch(muts)
	if len(payload) > MaxRecordSize {
		return fmt.Errorf("wal: batch too large (%d bytes)", len(payload))
	}
	buf := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.Checksum(payload, castagnoli))
	copy(buf[headerSize:], payload)

	n, err := w.f.Write(buf)
	w.offset += int64(n)
	if err != nil {
		return err
	}
	if w.sync {
		return w.f.Sync()
	}
	return nil
}

// Reset empties the log. Called after a commit has been made durable: those
// mutations are now recoverable from the object store instead.
func (w *WAL) Reset() error {
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w.offset = 0
	return w.f.Sync()
}

// Size is the current length of the log in bytes.
func (w *WAL) Size() int64 { return w.offset }

func (w *WAL) Sync() error { return w.f.Sync() }

func (w *WAL) Close() error { return w.f.Close() }
```

Header and payload go out in **one** `Write` call. Two calls would widen the window in which a crash leaves a header with no payload — the checksum would catch it, but a single write is both faster and less to reason about. Note also that `w.offset` is advanced by `n` *before* the error check: if a write is partial we want the offset to reflect what actually landed.

Finally the codec:

```go
func encodeBatch(muts []store.Mutation) []byte {
	buf := make([]byte, 0, 16*len(muts))
	var scratch [binary.MaxVarintLen64]byte
	putUvarint := func(v int) {
		n := binary.PutUvarint(scratch[:], uint64(v))
		buf = append(buf, scratch[:n]...)
	}
	putUvarint(len(muts))
	for _, m := range muts {
		buf = append(buf, byte(m.Op))
		putUvarint(len(m.Key))
		buf = append(buf, m.Key...)
		putUvarint(len(m.Value))
		buf = append(buf, m.Value...)
	}
	return buf
}

func decodeBatch(payload []byte) ([]store.Mutation, error) {
	readUvarint := func() (uint64, error) {
		v, n := binary.Uvarint(payload)
		if n <= 0 {
			return 0, ErrCorrupt
		}
		payload = payload[n:]
		return v, nil
	}
	readBytes := func() (string, error) {
		n, err := readUvarint()
		if err != nil {
			return "", err
		}
		if uint64(len(payload)) < n {
			return "", ErrCorrupt
		}
		s := string(payload[:n])
		payload = payload[n:]
		return s, nil
	}

	count, err := readUvarint()
	if err != nil {
		return nil, err
	}
	if count > uint64(len(payload))+1 {
		return nil, ErrCorrupt
	}
	muts := make([]store.Mutation, 0, count)
	for i := uint64(0); i < count; i++ {
		if len(payload) == 0 {
			return nil, ErrCorrupt
		}
		op := store.Op(payload[0])
		payload = payload[1:]
		if op != store.OpSet && op != store.OpDel {
			return nil, ErrCorrupt
		}
		key, err := readBytes()
		if err != nil {
			return nil, err
		}
		val, err := readBytes()
		if err != nil {
			return nil, err
		}
		muts = append(muts, store.Mutation{Op: op, Key: key, Value: val})
	}
	if len(payload) != 0 {
		return nil, ErrCorrupt
	}
	return muts, nil
}
```

`decodeBatch` is written defensively even though the checksum already passed, because a checksum only proves the bytes are the bytes we wrote — it does not prove we wrote them correctly. The `count > len(payload)+1` guard stops a `make` of a billion elements; the trailing `len(payload) != 0` check catches a payload that decoded successfully but had bytes left over, which would mean the encoder and decoder disagree.

### `fsync`: the thing that actually costs money

`Write` returning success means the kernel has your bytes in the page cache. It does **not** mean they are on stable storage. `f.Sync()` (`fsync(2)`) is what forces them out, and on real hardware it costs a fraction of a millisecond even on an SSD, because it has to wait for the device to acknowledge.

Measured on this project's own benchmarks:

| Mode | ns/op | writes/sec (single client) |
| --- | --- | --- |
| `Append` with `fsync` | ~139,000 | ~7,200 |
| `Append` without `fsync` | ~572 | ~1,700,000 |

Three hundred times slower. That is not a bug; it is the actual cost of durability, and it is why every database gives you this knob:

| Setting | Survives a process crash? | Survives power loss? | Our flag |
| --- | --- | --- | --- |
| fsync every write | yes | yes | `-sync=true` (default) |
| no fsync | yes | **no** — recent writes lost | `-sync=false` |
| fsync every N ms | yes | bounded loss | not implemented (Phase 14) |

The middle row is worth understanding precisely: if the *process* dies (panic, `kill -9`) the kernel still has the page cache and will write it out, so nothing is lost. Only a kernel panic or power loss loses data. Many systems find that trade acceptable; we default to safe and let you choose.

### `internal/engine/engine.go` (first version)

The engine is the component that owns write ordering. Right now it is thin; it grows in Phases 6, 7, 8 and 10.

```go
// Package engine wires the in-memory store and the write-ahead log together
// into one durable database. Later phases add versioning.
package engine

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/example/staash/internal/store"
	"github.com/example/staash/internal/wal"
)

// Options controls engine behaviour.
type Options struct {
	Dir     string
	SyncWAL bool
}

// Engine owns the store and the log. mu serialises writes so that a batch is
// appended to the log and applied to memory as one indivisible step.
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
```

**Two locks, on purpose.** `Engine.mu` serialises writers so that log-append and memory-apply are one step. `Store.mu` protects the map so that readers can run concurrently with each other. Reads take only `Store.mu` and never queue behind a writer's `fsync`.

**Why `Engine.mu` is a `Mutex` and not an `RWMutex`:** everything it guards is a write or a version-control operation. There is no read path through it.

**A wrinkle in `Del`.** The existence check happens outside the lock, so under concurrent deletion of the same key two clients can both be told the key existed. The alternative is to push the check inside `ApplyBatch` and return results from it. We accept the imprecision: the return value of `DEL` is advisory, and nobody should build logic on it in a concurrent client. That is a real limitation, and stating it is better than pretending it does not exist.

### Wiring it up

In `internal/server/session.go`, swap `*store.Store` for `*engine.Engine`:

```go
type session struct {
	eng *engine.Engine
}

func newSession(e *engine.Engine) *session { return &session{eng: e} }
```

and update the command bodies — the write commands now return errors:

```go
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
```

`EXISTS`, `KEYS` and `DBSIZE` change only in that `s.st` becomes `s.eng`. In `internal/server/server.go`, change the `Server` field `st *store.Store` to `eng *engine.Engine`, update `New`, and change `newSession(s.st)` to `newSession(s.eng)`.

Update `cmd/staash/main.go`:

```go
	var (
		addr    = flag.String("addr", "127.0.0.1:6380", "TCP address to listen on")
		dir     = flag.String("dir", "./data", "data directory")
		syncWAL = flag.Bool("sync", true, "fsync the write-ahead log on every write")
	)
	flag.Parse()

	eng, err := engine.Open(engine.Options{Dir: *dir, SyncWAL: *syncWAL})
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer eng.Close()

	srv := server.New(eng, server.Config{Addr: *addr})
```

### Crash scenarios

This is the heart of the phase. Work through each one.

**"What happens if the process dies halfway through writing a record?"**

The file ends with a partial record. On restart, `replay` hits one of: a short header (`io.ReadFull` → `ErrUnexpectedEOF`), a plausible header but a short payload, or a complete-looking record whose CRC does not match. All three return the offset of the last *good* record; `Open` truncates there. The half-written batch is gone — which is correct, because `Append` never returned, so the client never got an `+OK`.

**Crash after `Append` returns but before `ApplyBatch`.**

The record is on disk; memory never saw it. Recovery replays it. The client did get its `+OK`, and the write survived. This is precisely what write-ahead ordering buys.

**Crash after `ApplyBatch`, before the reply reaches the client.**

The write is durable but the client does not know. This is unavoidable in any networked system: the client must retry, and because mutations are absolute, retrying is safe. (Retrying `SET k v` twice is fine. This is another reason not to add `INCR` without thinking hard.)

**Bit rot in an old record, not the last one.**

Replay stops at the damaged record and **silently discards everything after it**, which in this case means discarding good data. Our justification is that in an append-only log this cannot normally happen — damage lives at the tail. A production system would distinguish "torn tail" from "corruption in the middle" and refuse to start on the latter. This is a known simplification; see Phase 14.

**Disk full during `Append`.**

`Write` returns `ENOSPC`, possibly after a partial write. `Apply` returns the error before touching memory, so memory and log stay consistent, and the client gets `-ERR`. On restart the partial record is truncated. The database keeps running and every subsequent write fails the same way — noisy, but correct.

### `internal/wal/wal_test.go`

```go
package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/example/staash/internal/store"
)

func openTemp(t *testing.T, dir string, sync bool) (*WAL, [][]store.Mutation) {
	t.Helper()
	w, batches, err := Open(filepath.Join(dir, "wal.log"), sync)
	if err != nil {
		t.Fatal(err)
	}
	return w, batches
}

func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	w, batches := openTemp(t, dir, true)
	if len(batches) != 0 {
		t.Fatalf("fresh log should be empty, got %d", len(batches))
	}
	if err := w.Append([]store.Mutation{{Op: store.OpSet, Key: "a", Value: "1"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]store.Mutation{
		{Op: store.OpSet, Key: "b", Value: "2"},
		{Op: store.OpDel, Key: "a"},
	}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2, batches := openTemp(t, dir, true)
	defer w2.Close()
	if len(batches) != 2 {
		t.Fatalf("got %d batches", len(batches))
	}
	if len(batches[1]) != 2 || batches[1][1].Op != store.OpDel || batches[1][1].Key != "a" {
		t.Fatalf("bad batch: %+v", batches[1])
	}
}

func TestResetEmptiesLog(t *testing.T) {
	dir := t.TempDir()
	w, _ := openTemp(t, dir, true)
	_ = w.Append([]store.Mutation{{Op: store.OpSet, Key: "a", Value: "1"}})
	if w.Size() == 0 {
		t.Fatal("expected non-zero size")
	}
	if err := w.Reset(); err != nil {
		t.Fatal(err)
	}
	w.Close()

	_, batches := openTemp(t, dir, true)
	if len(batches) != 0 {
		t.Fatalf("expected empty log, got %d batches", len(batches))
	}
}

// A crash in the middle of Append leaves a partial record. Recovery must drop
// it and keep everything before it.
func TestTornFinalRecordIsDropped(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, _, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Append([]store.Mutation{{Op: store.OpSet, Key: "good", Value: "1"}})
	_ = w.Append([]store.Mutation{{Op: store.OpSet, Key: "torn", Value: "2"}})
	good := w.Size()
	w.Close()

	// Chop the last record in half.
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, full[:len(full)-4], 0o644); err != nil {
		t.Fatal(err)
	}

	w2, batches, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if len(batches) != 1 || batches[0][0].Key != "good" {
		t.Fatalf("expected only the intact record, got %+v", batches)
	}
	if w2.Size() >= good {
		t.Fatalf("log was not truncated: size %d", w2.Size())
	}

	// The log must still be usable after recovery.
	if err := w2.Append([]store.Mutation{{Op: store.OpSet, Key: "after", Value: "3"}}); err != nil {
		t.Fatal(err)
	}
}

func TestChecksumCatchesBitFlip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, _, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Append([]store.Mutation{{Op: store.OpSet, Key: "key", Value: "value"}})
	w.Close()

	raw, _ := os.ReadFile(path)
	raw[len(raw)-1] ^= 0xFF // flip bits in the payload
	_ = os.WriteFile(path, raw, 0o644)

	_, batches, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 0 {
		t.Fatalf("corrupt record should have been rejected, got %+v", batches)
	}
}
```

Truncating and bit-flipping the file *is* the fault injection. There is no need for a mocked filesystem: the failure mode we care about is a specific byte pattern on disk, and we can just create it. Simulating faults by writing the damaged state directly is nearly always simpler and more convincing than intercepting syscalls.

### Checkpoint 4

```bash
go test ./... && go build ./...
rm -rf ./data
go run ./cmd/staash
```

```
$ nc 127.0.0.1 6380
SET persistent yes
+OK
```

Now kill the server with `Ctrl+C`, restart it, and reconnect:

```
$ nc 127.0.0.1 6380
GET persistent
$3
yes
```

Look at what is on disk — `ls -l data/` shows a `wal.log` of a few dozen bytes. `xxd data/wal.log` shows the header, the checksum, and your key and value in plain text.

```bash
git add . && git commit -m "feat: add write-ahead log and durable engine"
```

---

## Phase 5 — Content-addressed objects

We now start the Git half. The WAL made writes durable; objects will make *history* durable.

### Content addressing

Normally you choose a name and store data under it. Content addressing inverts that:

```
        id = SHA-256(content)
content ────────────────────────> id
```

The name is derived from the bytes. Four consequences fall out immediately, and all four are things we want:

1. **Deduplication is free.** Two identical values are one object. A branch that changes nothing shares every object with its parent.
2. **Integrity is free.** Re-hash on read; if it does not match the filename, the file is damaged. No separate checksum needed.
3. **Immutability is structural.** Changing the content changes the name, so you have not modified an object — you have created a different one. "Update in place" is not expressible.
4. **Equality is a 32-byte comparison.** Deciding whether two versions of a key are the same never reads the values. Phase 10's merge relies on this heavily.

Point 3 is the one that simplifies everything downstream. Consider what immutability removes:

- **No locking on reads.** An object cannot change while you read it.
- **No cache invalidation.** An object cached by ID is valid forever.
- **No partial-update crash window.** There is no update. A write either produced a complete new file or produced nothing.
- **No write ordering constraints between objects.** Because an object's ID depends only on its content, you can write children before parents and never have a dangling reference in the reverse direction.

That last one is the enabling property for Phase 7's commit protocol.

> **On SHA-256.** Git used SHA-1 and has spent years migrating away from it after practical collisions were demonstrated. We start with SHA-256 because it costs nothing to choose the right one at the beginning. Even so: a collision would break the deduplication invariant silently. At 2^128 work to find one, this is not a practical concern; it is a theoretical assumption worth naming.

### Encoding

An object on disk is:

```
"<kind> <payload length>\x00<payload>"
```

and its ID is the SHA-256 of *that whole byte string*, header included. Two reasons for the header:

- **Self-describing.** You can read an object off disk and know what it is without external metadata.
- **Type separation.** A blob and a tree whose payloads happen to be identical bytes get different IDs. Without the type in the hash, storing the value `"tree 0\x00"` as a blob would collide with the empty tree.

The length field is redundant with the file size, but it catches truncation, and it is what makes the format usable if objects are ever packed into a single file.

### `internal/fsutil/fsutil.go`

Before objects, the two filesystem primitives every durable component here needs.

```go
// Package fsutil contains small filesystem helpers used by every component
// that has to survive a crash: atomic file replacement and directory fsync.
package fsutil

import (
	"os"
	"path/filepath"
)

// SyncDir fsyncs a directory so that renames and creations inside it are
// durable. On most Unix filesystems a rename is only guaranteed to survive a
// power loss after the parent directory has been synced.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// WriteFileAtomic writes data to a temporary file in the same directory,
// fsyncs it, then renames it over path. A reader either sees the old file or
// the complete new file, never a half-written one.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		tmp.Close()
		os.Remove(tmpName) // no-op if the rename already succeeded
	}()

	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return SyncDir(dir)
}
```

**The atomic-rename pattern**, which appears three more times in this project:

```
1. create a temp file in the SAME directory as the target
2. write the full contents
3. fsync the file          <- the data is now on stable storage
4. rename temp -> target   <- atomic; readers see old or new, never partial
5. fsync the directory     <- the rename itself is now on stable storage
```

Every step matters:

- **Same directory**, because `rename(2)` is only atomic within a filesystem. A temp file in `/tmp` may be on a different one, and `os.Rename` would fall back to copy-and-delete, which is not atomic at all.
- **Step 3 before step 4.** Without it you can end up with the name pointing at a file whose contents were never flushed — the rename is durable and the data is not.
- **Step 5**, because a rename is a directory modification, and directory modifications are cached like any other. Skipping it is the single most common durability bug in real code.
- **The deferred `Remove`** cleans up if any step fails. After a successful rename the temp name no longer exists, so it is a harmless no-op.

`Chmod` before `Sync` sets the mode explicitly because `os.CreateTemp` creates files with `0600`, which is not what we want for shared data files.

### `internal/object/object.go`

```go
// Package object implements the content-addressed, immutable object store:
// blobs (values), trees (snapshots of the keyspace) and commits.
package object

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
)

// Kind is the type tag stored in every object header.
type Kind string

const (
	KindBlob   Kind = "blob"
	KindTree   Kind = "tree"
	KindCommit Kind = "commit"
)

// ID is the SHA-256 of an object's canonical encoding.
type ID [sha256.Size]byte

func (id ID) String() string { return hex.EncodeToString(id[:]) }

// Short returns the first 12 hex characters, for human-readable output.
func (id ID) Short() string { return id.String()[:12] }

func (id ID) IsZero() bool { return id == ID{} }

var ErrMalformed = errors.New("object: malformed encoding")

// ParseID accepts a full 64-character hex ID.
func ParseID(s string) (ID, error) {
	var id ID
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, fmt.Errorf("object: bad id %q: %w", s, err)
	}
	if len(b) != sha256.Size {
		return id, fmt.Errorf("object: bad id length %d", len(b))
	}
	copy(id[:], b)
	return id, nil
}

// Encode produces the canonical byte sequence that is hashed and stored.
func Encode(k Kind, payload []byte) []byte {
	header := string(k) + " " + strconv.Itoa(len(payload)) + "\x00"
	buf := make([]byte, 0, len(header)+len(payload))
	buf = append(buf, header...)
	buf = append(buf, payload...)
	return buf
}

// Hash returns the ID of an already-encoded object.
func Hash(encoded []byte) ID { return sha256.Sum256(encoded) }

// Decode splits a canonical encoding back into kind and payload.
func Decode(encoded []byte) (Kind, []byte, error) {
	nul := bytes.IndexByte(encoded, 0)
	if nul < 0 {
		return "", nil, ErrMalformed
	}
	head := string(encoded[:nul])
	sp := bytes.IndexByte([]byte(head), ' ')
	if sp < 0 {
		return "", nil, ErrMalformed
	}
	kind := Kind(head[:sp])
	n, err := strconv.Atoi(head[sp+1:])
	if err != nil {
		return "", nil, ErrMalformed
	}
	payload := encoded[nul+1:]
	if n != len(payload) {
		return "", nil, fmt.Errorf("%w: header says %d bytes, got %d", ErrMalformed, n, len(payload))
	}
	switch kind {
	case KindBlob, KindTree, KindCommit:
	default:
		return "", nil, fmt.Errorf("%w: unknown kind %q", ErrMalformed, kind)
	}
	return kind, payload, nil
}
```

`ID` is `[32]byte`, not `string` or `[]byte`. It is comparable with `==`, usable as a map key, and passed by value with no allocation. A `[]byte` would be none of those things.

### `internal/object/store.go`

```go
package object

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/example/staash/internal/fsutil"
)

// ErrNotFound is returned by Store.Get for an unknown ID.
var ErrNotFound = errors.New("object: not found")

// Store is a filesystem-backed content-addressed store.
//
// Layout:
//
//	<root>/ab/cdef...   the object whose ID starts with "ab"
//	<root>/tmp/         staging area for in-progress writes
//
// Invariant (relied on everywhere else): once a file exists under <root>/ab/,
// its contents are complete and never change. Writers stage into <root>/tmp
// and rename into place, and rename is atomic within a filesystem.
type Store struct {
	root string
}

func NewStore(root string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(root, "tmp"), 0o755); err != nil {
		return nil, err
	}
	return &Store{root: root}, nil
}

func (s *Store) path(id ID) string {
	h := id.String()
	return filepath.Join(s.root, h[:2], h[2:])
}

func (s *Store) Has(id ID) bool {
	_, err := os.Stat(s.path(id))
	return err == nil
}
```

**Why the two-character subdirectory?** Hashes are uniformly distributed, so with a flat layout every object lands in one directory. Many filesystems degrade badly past a few tens of thousands of entries in a single directory, and so do the tools you will use to debug (`ls` on a directory with a million entries is unpleasant). Splitting on the first byte gives 256 buckets — the same thing Git does, and for the same reason.

```go
// Put stores payload under kind and returns its ID. Writing an object that
// already exists is a no-op: identical content always has an identical ID,
// which gives us deduplication for free.
func (s *Store) Put(kind Kind, payload []byte) (ID, error) {
	encoded := Encode(kind, payload)
	id := Hash(encoded)
	if s.Has(id) {
		return id, nil
	}

	tmp, err := os.CreateTemp(filepath.Join(s.root, "tmp"), "obj-*")
	if err != nil {
		return id, err
	}
	tmpName := tmp.Name()
	defer func() {
		tmp.Close()
		os.Remove(tmpName)
	}()

	if _, err := tmp.Write(encoded); err != nil {
		return id, err
	}
	if err := tmp.Sync(); err != nil {
		return id, err
	}
	if err := tmp.Close(); err != nil {
		return id, err
	}

	dst := s.path(id)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return id, err
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return id, err
	}
	return id, fsutil.SyncDir(filepath.Dir(dst))
}
```

Same atomic-rename dance as `WriteFileAtomic`, but staging in `objects/tmp/` rather than the destination directory (both are under `objects/`, so still the same filesystem) and skipping the read-back of existing content.

The `Has` check is not just an optimisation. It is what makes `Put` **idempotent**: calling it twice with the same content is indistinguishable from calling it once, which means a retried or partially completed commit can simply be re-run.

```go
// Get reads an object and verifies that its content still hashes to its name.
// The check is cheap compared to the cost of silently serving corrupted data.
func (s *Store) Get(id ID) (Kind, []byte, error) {
	raw, err := os.ReadFile(s.path(id))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil, fmt.Errorf("%w: %s", ErrNotFound, id.Short())
		}
		return "", nil, err
	}
	if got := Hash(raw); got != id {
		return "", nil, fmt.Errorf("object: corrupted %s (content hashes to %s)", id.Short(), got.Short())
	}
	return Decode(raw)
}

// GetKind is Get plus an assertion on the object type.
func (s *Store) GetKind(id ID, want Kind) ([]byte, error) {
	k, payload, err := s.Get(id)
	if err != nil {
		return nil, err
	}
	if k != want {
		return nil, fmt.Errorf("object: %s is a %s, wanted %s", id.Short(), k, want)
	}
	return payload, nil
}

// CleanTmp removes staging files left behind by a crash. Safe to call at
// startup: a temp file is never referenced by anything.
func (s *Store) CleanTmp() error {
	entries, err := os.ReadDir(filepath.Join(s.root, "tmp"))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.Remove(filepath.Join(s.root, "tmp", e.Name())); err != nil {
			return err
		}
	}
	return nil
}
```

Verifying the hash on every read costs one SHA-256 pass over the object. For our sizes that is well under a microsecond, and it turns silent data corruption — the worst kind of bug a database can have — into a loud error. Phase 13 will confirm it is not a bottleneck; if it ever were, the right fix would be to verify only on a `FSCK` command, not to drop the check.

### Failure modes

| Failure | Behaviour |
| --- | --- |
| Crash mid-`Put`, before rename | a file in `objects/tmp/`, referenced by nothing; `CleanTmp` removes it at startup |
| Crash after rename, before `SyncDir` | on most filesystems the object is there; if not, it is simply absent and the commit that would have referenced it never happened |
| Two goroutines `Put` the same content | both write temp files, both rename to the same target; rename is atomic, one wins, the loser's identical bytes are discarded. Harmless — the contents are equal by construction |
| Object file truncated by external damage | `Get` hash mismatch → loud error |
| Object file deleted | `Get` → `ErrNotFound`, wrapped with the short ID |

### `internal/object/object_test.go`

```go
package object

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	payload := []byte("hello world")
	enc := Encode(KindBlob, payload)
	kind, got, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	if kind != KindBlob || !bytes.Equal(got, payload) {
		t.Fatalf("got %s %q", kind, got)
	}
}

func TestSameContentSameID(t *testing.T) {
	a := Hash(Encode(KindBlob, []byte("x")))
	b := Hash(Encode(KindBlob, []byte("x")))
	c := Hash(Encode(KindTree, []byte("x")))
	if a != b {
		t.Fatal("identical blobs must hash equally")
	}
	if a == c {
		t.Fatal("kind must participate in the hash")
	}
}

func TestStorePutGetDedup(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id1, err := s.Put(KindBlob, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	id2, err := s.Put(KindBlob, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatal("dedup failed")
	}
	payload, err := s.GetKind(id1, KindBlob)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "value" {
		t.Fatalf("got %q", payload)
	}
	if _, err := s.GetKind(id1, KindTree); err == nil {
		t.Fatal("expected a kind mismatch error")
	}
}

func TestStoreDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.Put(KindBlob, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	h := id.String()
	path := filepath.Join(dir, h[:2], h[2:])
	if err := os.WriteFile(path, []byte("blob 5\x00wrong"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Get(id); err == nil {
		t.Fatal("expected corruption to be detected")
	}
}
```

### Checkpoint 5

```bash
go test ./internal/object/ ./internal/fsutil/ && go build ./...
```

Nothing user-visible changed — the object store is not wired into the engine yet. That happens in the next two phases.

```bash
git add . && git commit -m "feat: add content-addressed immutable object store"
```

---

## Phase 6 — Snapshots as trees

We can store values. Now we need to store an entire *keyspace state* as one object graph with a single root ID.

### Four options

Say the database has *n* keys and a commit changes *k* of them.

| Representation | Commit cost | Sharing between commits | Complexity |
| --- | --- | --- | --- |
| **A. One giant blob** (serialise the whole map) | O(n) bytes hashed and written | none — one byte differs, the whole blob is new | trivial |
| **B. Flat tree** (one tree object listing every key → blob) | O(n) to build the tree, O(k) blobs | blobs shared, tree never | very low |
| **C. Sharded tree** (root → 256 shard trees → blobs) | O(n/256 + k) | 255/256 of the tree structure shared | low |
| **D. Persistent balanced tree** (HAMT, B-tree) | O(k log n) | near-perfect | high |

**A** is out: it defeats the purpose. At 1 million keys, every commit rewrites the whole database.

**B** is tempting because it is five lines shorter than C, but the tree object itself is O(n): with 1 million keys and 40-byte entries, every commit writes 40 MB. Blob sharing helps disk usage, not commit cost.

**D** is what a real system does. A hash-array-mapped trie gives you logarithmic commits and near-perfect structural sharing regardless of size. It is also several hundred lines with subtle invariants, and it would dominate this tutorial.

**We choose C.** Two levels: a root tree with at most 256 entries, and shard trees holding the actual keys. A commit rebuilds only the shards that contain changed keys, plus the root. At 1 million keys that is roughly 4,000 entries per shard — larger than ideal, but the root is tiny and constant, and the code is short enough to read in one sitting.

Concretely, the cost per commit is *(number of touched shards) × (shard size) + 256 root entries*, versus *n* for option B. At 10,000 keys changing one key, that is about 40 entries + 256 versus 10,000.

> **Note.** The natural upgrade path is to make the tree deeper — shard on the first byte, then the second, splitting a shard only when it exceeds some size. That is a trie, and it is option D arrived at incrementally. Phase 14 revisits it.

### Sharding by hash, not by prefix

```go
func shardOf(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:1])
}
```

Why hash the key rather than use its own first byte? Real keyspaces are not uniform. `user:1`, `user:2`, `session:abc` would put nearly everything into the `u` and `s` shards. Hashing spreads them evenly no matter what the naming convention is.

**The cost of that choice**, stated plainly: keys are no longer stored in lexicographic order, so an ordered range scan (`KEYS user:*` done efficiently, or a `SCAN` cursor) would have to read every shard. We do not have range scans, so we pay nothing today; if we added them, this is the decision we would revisit.

### Tree encoding — `internal/object/tree.go`

```go
package object

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// EntryKind distinguishes a subtree from a value.
type EntryKind byte

const (
	EntryBlob EntryKind = 'b'
	EntryTree EntryKind = 't'
)

// Entry is one row of a tree object.
type Entry struct {
	Name string
	Kind EntryKind
	ID   ID
}

// Tree is an immutable, sorted list of entries.
//
// Invariant: entries are sorted by Name and names are unique. Canonical
// ordering is what makes the encoding deterministic, and deterministic
// encoding is what makes content addressing useful: two identical keyspaces
// must produce the same tree ID.
type Tree struct {
	Entries []Entry
}

// NewTree copies and sorts entries.
func NewTree(entries []Entry) *Tree {
	cp := make([]Entry, len(entries))
	copy(cp, entries)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Name < cp[j].Name })
	return &Tree{Entries: cp}
}

// TreeFromMap builds a tree from a name -> entry map.
func TreeFromMap(m map[string]Entry) *Tree {
	entries := make([]Entry, 0, len(m))
	for _, e := range m {
		entries = append(entries, e)
	}
	return NewTree(entries)
}

// Map returns the entries keyed by name.
func (t *Tree) Map() map[string]Entry {
	m := make(map[string]Entry, len(t.Entries))
	for _, e := range t.Entries {
		m[e.Name] = e
	}
	return m
}

// Encode serialises the tree:
//
//	repeated: kind(1) namelen(uvarint) name(namelen) id(32)
func (t *Tree) Encode() []byte {
	buf := make([]byte, 0, len(t.Entries)*48)
	var scratch [binary.MaxVarintLen64]byte
	for _, e := range t.Entries {
		buf = append(buf, byte(e.Kind))
		n := binary.PutUvarint(scratch[:], uint64(len(e.Name)))
		buf = append(buf, scratch[:n]...)
		buf = append(buf, e.Name...)
		buf = append(buf, e.ID[:]...)
	}
	return buf
}

// DecodeTree is the inverse of Encode.
func DecodeTree(payload []byte) (*Tree, error) {
	var entries []Entry
	for len(payload) > 0 {
		kind := EntryKind(payload[0])
		if kind != EntryBlob && kind != EntryTree {
			return nil, fmt.Errorf("%w: bad entry kind %q", ErrMalformed, kind)
		}
		payload = payload[1:]
		nameLen, n := binary.Uvarint(payload)
		if n <= 0 {
			return nil, ErrMalformed
		}
		payload = payload[n:]
		if uint64(len(payload)) < nameLen+uint64(len(ID{})) {
			return nil, ErrMalformed
		}
		name := string(payload[:nameLen])
		payload = payload[nameLen:]
		var id ID
		copy(id[:], payload[:len(id)])
		payload = payload[len(id):]
		entries = append(entries, Entry{Name: name, Kind: kind, ID: id})
	}
	return &Tree{Entries: entries}, nil
}
```

> **Warning — the canonical-ordering invariant.** `TreeFromMap` sorts, because Go map iteration order is deliberately randomised. If we encoded entries in map order, the same keyspace would produce a different tree ID on every run, deduplication would collapse, and merges would report spurious differences. This is the single most important line in the file:
>
> ```go
> sort.Slice(cp, func(i, j int) bool { return cp[i].Name < cp[j].Name })
> ```

Length-prefixed names mean keys may contain any byte, including `/` and `\n`. The 32-byte ID is fixed-width, so no delimiter is needed after it.

Add the tree tests to `object_test.go`:

```go
func TestTreeRoundTripAndOrdering(t *testing.T) {
	var id1, id2 ID
	id1[0], id2[0] = 1, 2
	a := NewTree([]Entry{
		{Name: "zeta", Kind: EntryBlob, ID: id1},
		{Name: "alpha", Kind: EntryTree, ID: id2},
	})
	b := NewTree([]Entry{
		{Name: "alpha", Kind: EntryTree, ID: id2},
		{Name: "zeta", Kind: EntryBlob, ID: id1},
	})
	if !bytes.Equal(a.Encode(), b.Encode()) {
		t.Fatal("tree encoding must not depend on insertion order")
	}
	dec, err := DecodeTree(a.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if len(dec.Entries) != 2 || dec.Entries[0].Name != "alpha" || dec.Entries[1].ID != id1 {
		t.Fatalf("bad round trip: %+v", dec.Entries)
	}
}
```

### Tracking what changed

Incremental commits need to know which keys changed since the last commit. Add a field to `Engine`:

```go
type Engine struct {
	mu      sync.Mutex
	opts    Options
	store   *store.Store
	objects *object.Store
	refs    *refs.Store   // added in Phase 7
	log     *wal.WAL

	// dirty is the set of keys mutated since the last commit. It mirrors the
	// contents of the WAL and is what makes commits incremental.
	dirty map[string]struct{}
}
```

and maintain it in `Apply`:

```go
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
```

The dirty set and the WAL always hold the same key set, which is why recovery can rebuild it by replaying the log.

We also need a consistent copy of the keyspace to commit. Add to `internal/store/store.go`:

```go
// Snapshot returns a copy of the whole keyspace.
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// Replace swaps the entire keyspace. Used by CHECKOUT and by startup recovery.
func (s *Store) Replace(data map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = data
}
```

`Snapshot` is O(n) and allocates a full copy — the honest cost of taking a consistent view without a persistent data structure. Phase 14's MVCC section is where that cost goes away.

### Writing a tree — `internal/engine/engine.go`

Add these to the engine, along with `"crypto/sha256"` and `"encoding/hex"` imports:

```go
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
```

Read that function as an answer to one question: *what is the smallest amount of work that produces a correct new root?* We load the parent's root (256 entries at most), touch only the shards containing dirty keys, and hand the untouched shard IDs straight through to the new root. A key deleted from its last shard removes the shard entirely, so the empty-tree and full-tree cases need no special handling.

Note the order of writes: blobs, then shard trees, then the root. **Children before parents, always.** Combined with the crash behaviour of `Put`, this means the object store can never contain a tree pointing at an object that does not exist. It can contain objects nothing points at — that is just garbage, and Phase 14 discusses collecting it.

Now the readers:

```go
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
```

`treeKeys` returns blob *IDs*, not values. That is deliberate: Phase 10 compares two keyspaces for equality and only needs the IDs, so it never reads a single value off disk.

### Checkpoint 6

```bash
go build ./... && go test ./...
```

Still nothing new at the command line — trees exist but nothing creates a root yet. That is the next phase, and it is a short one, because all the hard parts are already done.

```bash
git add . && git commit -m "feat: represent keyspace snapshots as sharded trees"
```

---

## Phase 7 — Commits

A snapshot tells you *what* the data was. A commit tells you *when* it was that, *who* it followed, and *why*.

### The relationship

```
   Commit  ──parent──>  Commit  ──parent──>  Commit
      |                    |                    |
     tree                 tree                 tree
      |                    |                    |
    Tree                 Tree                 Tree     (root: shards)
   /    \               /    \               /    \
Tree    Tree         Tree    Tree         Tree    Tree (shards: keys)
  |       |            |       |            |       |
Blob    Blob         Blob    Blob         Blob    Blob (values)
```

Three layers, each addressed by content, each pointing only downward and backward. Commits form a **directed acyclic graph**:

- **Directed**, because a commit names its parents and never the reverse.
- **Acyclic**, and here is the nice part — *it cannot contain a cycle even if we wanted one*. A commit's ID is the hash of its contents, including its parent IDs. To make commit A point at B and B point at A, you would need A's hash before computing it. Content addressing makes cycles unrepresentable rather than merely forbidden.
- **A graph, not a list**, because a merge commit has two parents.

### `internal/object/commit.go`

```go
package object

import (
	"fmt"
	"strings"
	"time"
)

// Commit is an immutable snapshot pointer plus history.
//
// Invariant: a commit's parents already exist in the object store when the
// commit is written. That is what makes history a DAG we can always walk.
type Commit struct {
	Tree    ID
	Parents []ID
	Time    time.Time
	Message string
}

// Encode uses a line-oriented text format so that objects can be inspected
// with `cat` during debugging:
//
//	tree <hex>
//	parent <hex>        (zero or more)
//	time <RFC3339Nano>
//	<blank line>
//	<message bytes>
func (c *Commit) Encode() []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "tree %s\n", c.Tree)
	for _, p := range c.Parents {
		fmt.Fprintf(&b, "parent %s\n", p)
	}
	fmt.Fprintf(&b, "time %s\n", c.Time.UTC().Format(time.RFC3339Nano))
	b.WriteString("\n")
	b.WriteString(c.Message)
	return []byte(b.String())
}

// DecodeCommit parses the format written by Encode.
func DecodeCommit(payload []byte) (*Commit, error) {
	text := string(payload)
	head, msg, found := strings.Cut(text, "\n\n")
	if !found {
		return nil, fmt.Errorf("%w: commit has no message separator", ErrMalformed)
	}
	c := &Commit{Message: msg}
	for _, line := range strings.Split(head, "\n") {
		key, val, ok := strings.Cut(line, " ")
		if !ok {
			return nil, fmt.Errorf("%w: bad commit line %q", ErrMalformed, line)
		}
		switch key {
		case "tree":
			id, err := ParseID(val)
			if err != nil {
				return nil, err
			}
			c.Tree = id
		case "parent":
			id, err := ParseID(val)
			if err != nil {
				return nil, err
			}
			c.Parents = append(c.Parents, id)
		case "time":
			t, err := time.Parse(time.RFC3339Nano, val)
			if err != nil {
				return nil, fmt.Errorf("%w: bad commit time %q", ErrMalformed, val)
			}
			c.Time = t
		default:
			return nil, fmt.Errorf("%w: unknown commit field %q", ErrMalformed, key)
		}
	}
	if c.Tree.IsZero() {
		return nil, fmt.Errorf("%w: commit has no tree", ErrMalformed)
	}
	return c, nil
}
```

Commits are text, unlike trees, for one reason: you will read them by hand while debugging, and there are orders of magnitude fewer of them than there are blobs. `cat .../objects/ab/cdef...` on a commit is legible; on a tree it would be noise either way.

The message is everything after the first blank line, so multi-line messages need no escaping. The header cannot contain a blank line because every field is a single line. Times are stored UTC in RFC 3339 with nanoseconds — a fixed, sortable, unambiguous format. Storing a local time would make commit IDs depend on the machine's timezone.

Add the round-trip test:

```go
func TestCommitRoundTrip(t *testing.T) {
	var tree, parent ID
	tree[0], parent[0] = 7, 8
	now := time.Now().UTC().Truncate(time.Nanosecond)
	c := &Commit{Tree: tree, Parents: []ID{parent}, Time: now, Message: "hello\nmulti line"}
	got, err := DecodeCommit(c.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if got.Tree != tree || len(got.Parents) != 1 || got.Parents[0] != parent {
		t.Fatalf("bad ids: %+v", got)
	}
	if got.Message != c.Message {
		t.Fatalf("message = %q", got.Message)
	}
	if !got.Time.Equal(now) {
		t.Fatalf("time = %v want %v", got.Time, now)
	}
}
```

### `internal/refs/refs.go`

Commits are immutable, so something mutable has to point at the newest one. That is a *ref*: a file containing one commit ID.

```go
// Package refs stores the mutable pointers into immutable history: branches
// and HEAD.
package refs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/example/staash/internal/fsutil"
	"github.com/example/staash/internal/object"
)

var (
	ErrNoSuchBranch  = errors.New("refs: no such branch")
	ErrBadBranchName = errors.New("refs: invalid branch name")
)

// Store manages <dir>/HEAD and <dir>/refs/heads/<name>.
//
// Layout:
//
//	<dir>/HEAD              -> "ref: refs/heads/main\n"
//	<dir>/refs/heads/main   -> "<64 hex chars>\n"
type Store struct {
	dir string
}

func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(dir, "refs", "heads"), 0o755); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

func ValidBranchName(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	if strings.ContainsAny(name, "/\\ \t\n\r:*?\"<>|") {
		return false
	}
	if name == "." || name == ".." || strings.HasPrefix(name, ".") {
		return false
	}
	return true
}
```

> **Warning — path traversal.** A branch name goes straight into a file path. Without `ValidBranchName`, `BRANCH ../../../../etc/cron.d/evil` would let a client write a file anywhere the process can write. Any time user input becomes part of a path, validate it with an allowlist mindset — reject `/`, `\`, `..` and leading dots — rather than trying to sanitise. The length cap keeps us under filesystem limits.

```go
func (s *Store) branchPath(name string) string {
	return filepath.Join(s.dir, "refs", "heads", name)
}

func (s *Store) headPath() string { return filepath.Join(s.dir, "HEAD") }

// ReadBranch returns the commit a branch points at. ok is false if the branch
// file does not exist.
func (s *Store) ReadBranch(name string) (object.ID, bool, error) {
	if !ValidBranchName(name) {
		return object.ID{}, false, fmt.Errorf("%w: %q", ErrBadBranchName, name)
	}
	data, err := os.ReadFile(s.branchPath(name))
	if err != nil {
		if os.IsNotExist(err) {
			return object.ID{}, false, nil
		}
		return object.ID{}, false, err
	}
	id, err := object.ParseID(strings.TrimSpace(string(data)))
	if err != nil {
		return object.ID{}, false, err
	}
	return id, true, nil
}

// SetBranch points a branch at a commit, atomically.
func (s *Store) SetBranch(name string, id object.ID) error {
	if !ValidBranchName(name) {
		return fmt.Errorf("%w: %q", ErrBadBranchName, name)
	}
	return fsutil.WriteFileAtomic(s.branchPath(name), []byte(id.String()+"\n"), 0o644)
}

func (s *Store) DeleteBranch(name string) error {
	if !ValidBranchName(name) {
		return fmt.Errorf("%w: %q", ErrBadBranchName, name)
	}
	if err := os.Remove(s.branchPath(name)); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: %s", ErrNoSuchBranch, name)
		}
		return err
	}
	return fsutil.SyncDir(filepath.Join(s.dir, "refs", "heads"))
}

// ListBranches returns existing branch names in sorted order.
func (s *Store) ListBranches() ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, "refs", "heads"))
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

const headPrefix = "ref: refs/heads/"

// Head returns the branch name HEAD points at.
func (s *Store) Head() (string, error) {
	data, err := os.ReadFile(s.headPath())
	if err != nil {
		return "", err
	}
	line := strings.TrimSpace(string(data))
	if !strings.HasPrefix(line, headPrefix) {
		return "", fmt.Errorf("refs: unsupported HEAD contents %q", line)
	}
	name := strings.TrimPrefix(line, headPrefix)
	if !ValidBranchName(name) {
		return "", fmt.Errorf("%w: %q", ErrBadBranchName, name)
	}
	return name, nil
}

// SetHead points HEAD at a branch, atomically.
func (s *Store) SetHead(name string) error {
	if !ValidBranchName(name) {
		return fmt.Errorf("%w: %q", ErrBadBranchName, name)
	}
	return fsutil.WriteFileAtomic(s.headPath(), []byte(headPrefix+name+"\n"), 0o644)
}

// HeadExists reports whether a HEAD file is present.
func (s *Store) HeadExists() bool {
	_, err := os.Stat(s.headPath())
	return err == nil
}
```

The `ListBranches` filter skipping dot-files matters: `WriteFileAtomic` creates `.tmp-*` files in the target directory, and a listing taken at exactly the wrong moment would otherwise show one as a branch.

`HEAD` always contains a symbolic reference (`ref: refs/heads/<name>`), never a raw commit ID. Git supports the latter as "detached HEAD"; we do not, because every operation we have — commit, merge — needs a branch to move. Rejecting anything else at parse time keeps that assumption enforced rather than assumed.

### The engine, completed

Replace `internal/engine/engine.go`'s header, `Options`, `Engine` and `Open` with the final versions:

```go
// Package engine wires the in-memory store, the write-ahead log, the object
// store and the refs together into one database.
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

	"github.com/example/staash/internal/object"
	"github.com/example/staash/internal/refs"
	"github.com/example/staash/internal/store"
	"github.com/example/staash/internal/wal"
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
	// SyncWAL makes every write durable before the client is acknowledged.
	SyncWAL bool
	// Now is injectable for deterministic tests.
	Now func() time.Time
}

// Engine is the database.
//
// Locking model (deliberately coarse): mu serialises every *mutating* and
// every version-control operation. Point reads bypass mu and go straight to
// the store, which has its own RWMutex. This is simple and obviously correct;
// it is also a scalability ceiling, discussed in the final chapter.
type Engine struct {
	mu      sync.Mutex
	opts    Options
	store   *store.Store
	objects *object.Store
	refs    *refs.Store
	log     *wal.WAL

	dirty map[string]struct{}
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
```

That two-step `Open` is Phase 0's recovery equation, written out: *committed state, then uncommitted log on top*. The order is not optional — the WAL holds newer mutations than the commit, so it must be applied second.

Add the remaining readers and `materialize`:

```go
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
```

`materialize` is how *any* historical state is reconstructed — startup, checkout, merge, or inspecting an old commit. Note that it takes any commit ID, not just HEAD: reading the database as it was fifty commits ago is one call.

### `COMMIT`

```go
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
```

**Three orderings, all deliberate:**

1. `writeTree` before `finishCommit`: children before parents, so the commit object never references a tree that does not exist.
2. `SetBranch` before `log.Reset()`: the ref move is the commit point. Truncating the log first and then failing to move the ref would lose data permanently.
3. `e.dirty` is cleared only after the reset succeeds.

`finishCommit` is split out because Phase 10's merge needs steps 2–3 with a different tree and two parents.

The "nothing to commit" rule has an exception: when the branch has no commits at all, an empty first commit is allowed. Otherwise a brand-new database could never establish a root commit for branches to be created from.

### `LOG`, `SHOW`, `HEAD`

```go
// LogEntry pairs a commit with its ID.
type LogEntry struct {
	ID     object.ID
	Commit *object.Commit
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
```

The `seen` map is not an optimisation — without it, a merge commit's two parents would each be walked in full, and a history with *m* merges would be traversed 2^m times.

**A limitation to be honest about:** `Log` walks the *entire* reachable history before applying the limit, so `LOG 10` on a 100,000-commit database loads 100,000 commit objects. Git avoids this with a priority queue keyed on commit time, popping only as many as it needs. Sorting by wall-clock time is also a heuristic: clocks move backwards, and a commit can be older than its parent. The DAG edges are the real ordering; timestamps are for display. Fixing both is a good exercise.

### The commands

Add to `dispatch` in `internal/server/session.go` (and add `strconv`, `strings`, `time`, `fmt`, and the `object` and `engine` imports):

```go
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
			if err != nil || v <= 0 {
				return false, w.Error("LOG limit must be a positive integer")
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
```

Also improve the startup log in `cmd/staash/main.go` so recovery is visible:

```go
	branch, id, hasCommit, err := eng.HeadInfo()
	if err != nil {
		logger.Fatalf("read HEAD: %v", err)
	}
	if hasCommit {
		logger.Printf("recovered: branch=%s head=%s keys=%d uncommitted=%d",
			branch, id.Short(), eng.Len(), eng.DirtyCount())
	} else {
		logger.Printf("new database: branch=%s keys=%d uncommitted=%d",
			branch, eng.Len(), eng.DirtyCount())
	}
```

### Tests

```go
func TestCommitAndReopen(t *testing.T) {
	dir := t.TempDir()
	e := openTestEngine(t, dir)
	mustSet(t, e, "name", "alice")
	mustSet(t, e, "city", "berlin")
	id := mustCommit(t, e, "initial")
	if e.DirtyCount() != 0 {
		t.Fatal("commit should clear the dirty set")
	}
	e.Close()

	e2 := openTestEngine(t, dir)
	if got := mustGet(t, e2, "name"); got != "alice" {
		t.Fatalf("name = %q", got)
	}
	_, headID, ok, err := e2.HeadInfo()
	if err != nil || !ok || headID != id {
		t.Fatalf("HeadInfo = %v %v %v", headID, ok, err)
	}
}

func TestHistoryReconstruction(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	mustSet(t, e, "k", "v1")
	first := mustCommit(t, e, "v1")
	mustSet(t, e, "k", "v2")
	mustCommit(t, e, "v2")

	old, err := e.materialize(first)
	if err != nil {
		t.Fatal(err)
	}
	if old["k"] != "v1" {
		t.Fatalf("historical state = %v", old)
	}
	if got := mustGet(t, e, "k"); got != "v2" {
		t.Fatalf("current = %q", got)
	}

	entries, err := e.Log(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Commit.Message != "v2" {
		t.Fatalf("log = %+v", entries)
	}
}

// Unchanged shards must be reused, otherwise commits would cost O(database).
func TestTreeSharesUnchangedShards(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	for i := 0; i < 200; i++ {
		mustSet(t, e, fmt.Sprintf("key%03d", i), "v")
	}
	c1 := mustCommit(t, e, "bulk")
	mustSet(t, e, "key000", "changed")
	c2 := mustCommit(t, e, "one key")

	t1, _ := e.loadCommit(c1)
	t2, _ := e.loadCommit(c2)
	root1, _ := e.loadTree(t1.Tree)
	root2, _ := e.loadTree(t2.Tree)

	same := 0
	m2 := root2.Map()
	for name, entry := range root1.Map() {
		if other, ok := m2[name]; ok && other.ID == entry.ID {
			same++
		}
	}
	if same < len(root1.Entries)-1 {
		t.Fatalf("only %d/%d shards reused", same, len(root1.Entries))
	}
}
```

with these helpers at the top of `internal/engine/engine_test.go`:

```go
package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/example/staash/internal/object"
	"github.com/example/staash/internal/store"
)

func openTestEngine(t *testing.T, dir string) *Engine {
	t.Helper()
	e, err := Open(Options{Dir: dir, SyncWAL: false})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func mustSet(t *testing.T, e *Engine, k, v string) {
	t.Helper()
	if err := e.Set(k, v); err != nil {
		t.Fatal(err)
	}
}

func mustCommit(t *testing.T, e *Engine, msg string) object.ID {
	t.Helper()
	id, err := e.Commit(msg)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustGet(t *testing.T, e *Engine, k string) string {
	t.Helper()
	v, ok := e.Get(k)
	if !ok {
		t.Fatalf("key %q missing", k)
	}
	return v
}
```

`TestTreeSharesUnchangedShards` is worth pausing on: it asserts a *performance* property as a *correctness* test. Structural sharing is the entire justification for the design in Phase 6, and it is the kind of thing a careless refactor silently breaks. Phase 13 measures the consequence; this test protects it.

### Checkpoint 7

```bash
go test ./... && go build ./...
rm -rf ./data && go run ./cmd/staash
```

```
$ nc 127.0.0.1 6380
SET name alice
+OK
STATUS
$49
branch main, 1 uncommitted key(s), 1 key(s) total
COMMIT "first commit"
+fda6a9293d5015d0a7190fba24bdfc46797939e8d42b27dcc8f639b7902e11be
STATUS
$49
branch main, 0 uncommitted key(s), 1 key(s) total
SET name bob
+OK
COMMIT "rename"
+83ac73e40975ba166f350bdc7d815fa84e458ddfda19de14047229ed70dd2fd1
LOG
*2
$46
83ac73e40975 2026-08-26T07:37:05Z rename
$52
fda6a9293d50 2026-08-26T07:37:05Z first commit
```

Poke at the data directory:

```bash
$ cat data/HEAD
ref: refs/heads/main
$ cat data/refs/heads/main
83ac73e40975ba166f350bdc7d815fa84e458ddfda19de14047229ed70dd2fd1
$ cat data/objects/83/ac73e40975ba166f350bdc7d815fa84e458ddfda19de14047229ed70dd2fd1
commit 218
tree 9f4c...
parent fda6...
time 2026-08-26T07:37:05.123456789Z

rename
```

```bash
git add . && git commit -m "feat: add immutable commits, refs, LOG and SHOW"
```

---

## Phase 8 — Branches and checkout

Almost all the work is already done. A branch is a file with a commit ID in it, and because commits are immutable and objects are shared, creating one copies nothing.

### The idea

```
                        refs/heads/main ──┐
                                          v
     C1 <──── C2 <──── C3 <──── C4 <──── C5
                        ^
                        └── C3a <──── C3b
                                       ^
                        refs/heads/feature
```

Immutable commits, mutable refs. Creating `feature` writes 65 bytes. Both branches share C1–C3 and every object beneath them — the blobs, the shard trees, everything unchanged. This is why branching in Git is instant regardless of repository size, and it is why it is instant here.

Deleting a branch is also just deleting a file. The commits it uniquely reached become unreachable garbage; nothing breaks, disk is simply not reclaimed until someone implements GC (Phase 14).

### Implementation

Add to `internal/engine/engine.go`:

```go
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
```

**The dirty check is the invariant enforcer.** Recall Phase 0's rule: *in-memory state == committed state + WAL*. If checkout switched branches with a non-empty WAL, replaying that WAL after a crash would apply mutations meant for one branch onto another. Git handles this with a staging area and a working tree that can carry modifications across; we have neither, so we refuse. This is a real usability limitation with a clear justification, and it is stated in the error message the user sees.

**Ordering inside `Checkout`:** materialize (can fail: missing object, corruption), *then* `SetHead`, *then* replace memory. Materializing first means a failure leaves everything untouched. `SetHead` before `Replace` means a crash between them is recoverable — restart materializes the new HEAD and reaches the same place.

Note that `Checkout` does **not** write to the WAL, and does not need to: the WAL is empty (we just checked), and HEAD on disk fully determines the state after restart.

### Commands

```go
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
```

### Test

```go
func TestBranchCheckout(t *testing.T) {
	dir := t.TempDir()
	e := openTestEngine(t, dir)
	mustSet(t, e, "shared", "base")
	mustCommit(t, e, "base")

	if err := e.Branch("feature"); err != nil {
		t.Fatal(err)
	}
	if err := e.Checkout("feature"); err != nil {
		t.Fatal(err)
	}
	mustSet(t, e, "feature-only", "1")
	mustCommit(t, e, "on feature")

	if err := e.Checkout("main"); err != nil {
		t.Fatal(err)
	}
	if e.Exists("feature-only") {
		t.Fatal("feature key leaked into main")
	}
	if err := e.Checkout("feature"); err != nil {
		t.Fatal(err)
	}
	if !e.Exists("feature-only") {
		t.Fatal("feature key lost")
	}

	// Checkout with uncommitted work must be refused.
	mustSet(t, e, "scratch", "x")
	if err := e.Checkout("main"); err == nil {
		t.Fatal("expected checkout to be refused while dirty")
	}
}
```

### Checkpoint 8

```
$ nc 127.0.0.1 6380
SET name alice
+OK
COMMIT "base"
+7f1a...
BRANCH feature
+OK
BRANCHES
*2
$6
* main
$9
  feature
CHECKOUT feature
+OK
SET name bob
+OK
COMMIT "rename on feature"
+2b90...
CHECKOUT main
+OK
GET name
$5
alice
```

Two divergent states of the same database, switchable instantly.

```bash
git add . && git commit -m "feat: add branches, checkout and HEAD tracking"
```

---

## Phase 9 — Transactions

### The naming collision

`COMMIT` is already taken by version control. Rather than overload it, transactions use Redis's vocabulary:

| Command | Meaning |
| --- | --- |
| `BEGIN` | start buffering writes |
| `EXEC` | apply all buffered writes atomically; returns the count |
| `ROLLBACK` (alias `DISCARD`) | throw the buffer away |

Overloading `COMMIT` would have meant guessing from context whether the user wanted a snapshot in history or an atomic write batch, and getting it wrong in either direction is bad. Distinct verbs, no ambiguity.

### Semantics — precisely

State exactly what is guaranteed, because "transaction" is a word people load with assumptions.

**What we guarantee:**

- **Atomicity.** All buffered mutations apply, or none do. They go into one WAL record and one `ApplyBatch` under one lock. No client and no crash can observe half a transaction.
- **Durability.** `EXEC` returns after the WAL record is written (and fsynced, if `-sync=true`).
- **Read-your-own-writes.** Inside a transaction, `GET` sees the buffer.
- **Isolation of *uncommitted* writes.** No other client can see buffered writes before `EXEC`.

**What we do not guarantee:**

- **Not serializable, not repeatable read.** Reads inside a transaction that miss the buffer go straight to live shared state. Another client's committed write is visible immediately. Read the same key twice and you may get two answers.
- **No conflict detection.** Two transactions writing the same key both succeed; the later `EXEC` wins. There is no `WATCH`, no version check, no abort.
- **No read locks.** A transaction that reads K and then writes K based on that read can be interleaved with another doing the same. This is the lost-update anomaly, and our design permits it.
- **No rollback of applied writes.** `ROLLBACK` discards the *buffer*. After `EXEC` there is nothing to roll back.

In ANSI terms this is roughly **read committed with atomic batched writes**. Redis's `MULTI`/`EXEC` offers the same shape, and calling it "read committed" is already generous, since our reads take no snapshot at all.

**Say it plainly:** this is a single process using one coarse-grained mutex. `EXEC` takes `Engine.mu`, appends one WAL record, and applies one batch under `Store.mu`. That is the whole mechanism. It is not MVCC, it is not two-phase locking, and it does not scale to fine-grained concurrency. Phase 14 sketches what would.

### Why buffer instead of lock?

The alternative — take the write lock at `BEGIN` and hold it until `EXEC` — would give real serializability. It would also let any client freeze the entire database by typing `BEGIN` and walking away. For a network service where sessions are driven by remote humans and applications, buffering is the only defensible choice. Databases that do hold locks across a transaction also have deadlock detection and lock timeouts; those are the missing pieces, not the locking itself.

### `internal/server/session.go`

```go
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
```

The `*string` overlay distinguishes three states that a plain `map[string]string` cannot: *not touched* (absent from the map), *set to V* (non-nil pointer), and *deleted* (nil pointer). Without the third state, `DEL k` inside a transaction followed by `GET k` would fall through to the engine and return the old value.

Writing the same key twice in one transaction updates `overlay` and does not re-append to `order`, so the emitted batch has one mutation per key with last-write-wins. Since mutations are absolute, that is exactly equivalent to applying them in sequence.

Now the commands:

```go
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
```

Note that `s.tx = nil` happens *before* `Apply`. If the append fails, the transaction is over and its writes did not happen — leaving it open would invite a retry of `EXEC` that re-applies a buffer the client thinks was already consumed.

Modify `SET`, `DEL`, `GET`, `EXISTS` and `KEYS` to respect the buffer:

```go
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
```

`DEL` inside a transaction returns `+QUEUED` rather than `:0`/`:1`. It has to: whether the key exists at `EXEC` time is not known at `DEL` time. Returning a guess would be worse than returning nothing.

The `KEYS` overlay merge:

```go
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
```

Finally, forbid version-control commands inside a transaction. Put this at the top of `dispatch`, before the main switch:

```go
	// Version-control commands change the whole keyspace, so they are refused
	// while a transaction is buffered rather than given ill-defined semantics.
	switch cmd.Name {
	case "COMMIT", "CHECKOUT", "MERGE", "BRANCH":
		if s.tx != nil {
			return false, w.Error(cmd.Name + " is not allowed inside a transaction")
		}
	}
```

What would `CHECKOUT` inside a transaction even mean — does the buffer apply to the old branch or the new one? There is no good answer, so we refuse rather than invent one. Refusing an ambiguous operation is a legitimate design choice and a better one than picking a behaviour users cannot predict.

### Failure modes

| Situation | Behaviour |
| --- | --- |
| Client disconnects with a transaction open | the session struct is garbage collected; buffered writes never applied. Correct — no `EXEC`, no effect |
| `BEGIN` twice | `-ERR transaction already open`; the existing buffer is untouched |
| Huge transaction | the buffer is in memory, and the WAL record must fit `MaxRecordSize` (64 MiB). Beyond that, `EXEC` fails and the writes are lost |
| Two clients `EXEC` overlapping key sets | both succeed; whoever takes `Engine.mu` second wins. Lost update — by design, documented above |
| Crash between `Append` and `ApplyBatch` in `EXEC` | recovery replays the whole record; the transaction is atomic across the crash |

### Test

```go
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
```

The two-connection structure is what makes this a real isolation test. A single-connection test would pass even if the buffer wrote through to shared state immediately. The test harness (`startServer`, `dial`, `do`) is in Phase 12.

### Checkpoint 9

```
$ nc 127.0.0.1 6380
BEGIN
+OK
SET a 1
+QUEUED
SET b 2
+QUEUED
GET a
$1
1
EXEC
:2
KEYS
*2
$1
a
$1
b
```

Open a second `nc` before `EXEC` and confirm `GET a` returns `$-1` there.

```bash
git add . && git commit -m "feat: add buffered transactions with atomic EXEC"
```

---

## Phase 10 — Merging branches

### Three-way merge

Comparing two branches directly cannot tell you what changed. If `main` has `name=bob` and `feature` has `name=charlie`, which is the edit and which is the original? You need the **merge base**: the most recent commit both branches descend from.

```
              base: name=alice
                    /      \
                   /        \
      ours: name=bob      theirs: name=charlie
```

Now each side is a diff against a shared origin, and the merge rules follow mechanically:

| base | ours | theirs | result | reasoning |
| --- | --- | --- | --- | --- |
| x | x | x | x | nobody changed it |
| x | y | x | y | only we changed it |
| x | x | y | y | only they changed it |
| x | y | y | y | both made the same change |
| x | y | z | **CONFLICT** | both changed it differently |

"Changed" includes creation and deletion — a key missing from a side is just another state, so deleting on one side while the other leaves it alone merges cleanly as a deletion, and delete-vs-edit is a conflict.

**Values are compared by blob ID.** They are pure content hashes, so equality is exact and costs a 32-byte comparison instead of reading two values off disk. That is a direct dividend of content addressing.

**What we are not doing:** Git merges *file contents* line by line, producing merged text with conflict markers. We merge at key granularity, and a conflict is reported, not written. Merging two values of a key would require knowing their structure, which a key-value store does not.

### Merge base

```go
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
```

Two passes: collect every ancestor of `a`, then walk back from `b` and return the first hit. Both include the starting commit itself, which is how the fast-forward cases are detected — if `mergeBase(ours, theirs) == theirs`, then `theirs` is already an ancestor of `ours`.

**Where this is wrong**, stated honestly: with a criss-cross history (two branches that merged each other and diverged again) there can be several minimal common ancestors, and BFS order picks one arbitrarily. Git resolves this by recursively merging the candidate bases. Our BFS is also not strictly nearest-first, since it explores by graph distance rather than commit order. For the histories this database produces in practice these differ rarely, and when they do the result is a spurious conflict rather than silent data loss — the safe direction to be wrong in.

### `internal/engine/merge.go`

```go
package engine

import (
	"errors"
	"fmt"
	"sort"

	"github.com/example/staash/internal/object"
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
```

A struct error rather than a sentinel, because the caller needs the conflicting key list to show the user. `errors.As` recovers it through any amount of wrapping.

```go
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
```

A fast-forward creates no commit at all. Our branch has nothing the other lacks, so pointing the ref at their commit is the whole operation — no new objects, no new history node, no possibility of conflict.

```go
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
```

That loop is the table above, transcribed. `commitKeys` returns `map[string]object.ID`, so the whole comparison happens on hashes.

**On conflict we abort completely.** No partial merge, no conflict markers, no half-updated keyspace — we return before writing a single object. Recovery for the user is "resolve it manually": check out one branch, set the disputed keys, commit, and merge again.

```go
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
```

The merge commit reuses `writeTree` with our tree as the parent and only the keys they changed marked dirty — the same incremental machinery as a normal commit. And it reuses `finishCommit`, so the object → ref → WAL-reset ordering is identical; the only difference is two parents instead of one.

`e.store.Replace(merged)` comes last, after the commit is durable. If any step failed, memory still holds the pre-merge state, matching the ref that was never moved.

### Command

```go
	case "MERGE":
		if n != 1 {
			return false, argErr()
		}
		res, err := s.eng.Merge(cmd.Args[0])
		if err != nil {
			var conflict *engine.ErrMergeConflict
			if errors.As(err, &conflict) {
				return false, w.Error("CONFLICT " + strings.Join(conflict.Keys, " "))
			}
			return false, w.Error(err.Error())
		}
		return false, w.Simple(res.Kind + " " + res.Commit.Short())
```

### Tests

```go
func TestThreeWayMergeNoConflict(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	mustSet(t, e, "name", "alice")
	mustCommit(t, e, "base")
	_ = e.Branch("feature")

	mustSet(t, e, "city", "berlin") // on main
	mustCommit(t, e, "main change")

	_ = e.Checkout("feature")
	mustSet(t, e, "email", "a@example.com")
	mustCommit(t, e, "feature change")

	_ = e.Checkout("main")
	res, err := e.Merge("feature")
	if err != nil {
		t.Fatal(err)
	}
	if res.Kind != "merge" {
		t.Fatalf("kind = %s", res.Kind)
	}
	for k, want := range map[string]string{"name": "alice", "city": "berlin", "email": "a@example.com"} {
		if got := mustGet(t, e, k); got != want {
			t.Fatalf("%s = %q want %q", k, got, want)
		}
	}
	c, err := e.Show(res.Commit)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Parents) != 2 {
		t.Fatalf("merge commit has %d parents", len(c.Parents))
	}
}

func TestMergeConflict(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	mustSet(t, e, "name", "alice")
	mustCommit(t, e, "base")
	_ = e.Branch("feature")

	mustSet(t, e, "name", "bob")
	mustCommit(t, e, "main renames")

	_ = e.Checkout("feature")
	mustSet(t, e, "name", "charlie")
	mustCommit(t, e, "feature renames")

	_ = e.Checkout("main")
	_, err := e.Merge("feature")
	var conflict *ErrMergeConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("err = %v, want conflict", err)
	}
	if len(conflict.Keys) != 1 || conflict.Keys[0] != "name" {
		t.Fatalf("conflict keys = %v", conflict.Keys)
	}
	// The failed merge must leave the database untouched.
	if got := mustGet(t, e, "name"); got != "bob" {
		t.Fatalf("name = %q after aborted merge", got)
	}
}

func TestMergeIdenticalChangeIsNotAConflict(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	mustSet(t, e, "k", "base")
	mustCommit(t, e, "base")
	_ = e.Branch("feature")
	mustSet(t, e, "k", "same")
	mustCommit(t, e, "main")
	_ = e.Checkout("feature")
	mustSet(t, e, "k", "same")
	mustCommit(t, e, "feature")
	_ = e.Checkout("main")
	if _, err := e.Merge("feature"); err != nil {
		t.Fatalf("identical changes should merge cleanly: %v", err)
	}
}

func TestMergeDeletion(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	mustSet(t, e, "gone", "1")
	mustSet(t, e, "kept", "1")
	mustCommit(t, e, "base")
	_ = e.Branch("feature")
	_ = e.Checkout("feature")
	if _, err := e.Del("gone"); err != nil {
		t.Fatal(err)
	}
	mustCommit(t, e, "delete on feature")
	_ = e.Checkout("main")
	mustSet(t, e, "other", "1")
	mustCommit(t, e, "main change")
	if _, err := e.Merge("feature"); err != nil {
		t.Fatal(err)
	}
	if e.Exists("gone") {
		t.Fatal("deletion was not merged")
	}
	if !e.Exists("kept") || !e.Exists("other") {
		t.Fatal("merge dropped unrelated keys")
	}
}

func TestFastForwardMerge(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	mustSet(t, e, "a", "1")
	mustCommit(t, e, "base")
	_ = e.Branch("feature")
	_ = e.Checkout("feature")
	mustSet(t, e, "b", "2")
	mustCommit(t, e, "feature work")
	_ = e.Checkout("main")

	res, err := e.Merge("feature")
	if err != nil {
		t.Fatal(err)
	}
	if res.Kind != "fast-forward" {
		t.Fatalf("kind = %s", res.Kind)
	}
	if got := mustGet(t, e, "b"); got != "2" {
		t.Fatalf("b = %q", got)
	}
}
```

`TestMergeIdenticalChangeIsNotAConflict` is the one people forget to write. Two branches independently setting the same key to the same value produce the same blob ID, so the "both changed identically" rule fires and the merge is clean. Get the comparison wrong — compare commits instead of blobs, say — and this test fails while all the others pass.

### Checkpoint 10

```
$ nc 127.0.0.1 6380
SET name alice
+OK
COMMIT "base"
+e7c2...
BRANCH feature
+OK
CHECKOUT feature
+OK
SET name charlie
+OK
COMMIT "feature rename"
+91ab...
CHECKOUT main
+OK
SET name bob
+OK
COMMIT "main rename"
+55de...
MERGE feature
-ERR CONFLICT name
GET name
$3
bob
```

The conflict is reported, and the database is exactly as it was.

```bash
git add . && git commit -m "feat: add three-way merge with conflict detection"
```

---

## Phase 11 — Crash recovery, audited

Every phase so far has made local durability claims. This phase collects them, checks them against each other, and tests the ones that were only asserted.

### The two primitives

Everything rests on two operations.

**`fsync`** forces a file's data out of the page cache to stable storage. Without it, "the write succeeded" means "the kernel has a copy in RAM". A power cut loses it.

**`rename`** is atomic within a filesystem: after it, the target name refers either to the old inode or the new one, never to a partially written file. This is what turns "write a file" into "replace a file safely".

Combined, they give the pattern used in `fsutil.WriteFileAtomic` and `object.Store.Put`:

```
write temp  ->  fsync temp  ->  rename  ->  fsync directory
             (data durable)  (visible)  (name durable)
```

Skip the first fsync and you may end up with a durable name pointing at nothing. Skip the last and the rename may not survive a power cut, leaving the old name intact — which for us is recoverable, but only by luck.

### Write ordering across the system

```
   SET / EXEC                       COMMIT
   ----------                       ------
   1. WAL append  (+fsync)          1. blobs      (fsync each)
   2. apply to memory               2. shard trees (fsync each)
                                    3. root tree   (fsync)
                                    4. commit obj  (fsync)
                                    5. branch ref  (atomic rename + dir fsync)  <-- COMMIT POINT
                                    6. WAL truncate (+fsync)
                                    7. clear dirty set (in memory)
```

Steps 1–4 of a commit write only objects nothing references yet. They are pure garbage if we stop there. Step 5 is the instant at which history changes, and it is a single atomic rename. Steps 6–7 are cleanup.

### Every crash point

| # | Crash point | On-disk state | Recovery | Data loss? |
| --- | --- | --- | --- | --- |
| 1 | mid-WAL-append | partial record at tail | truncated by `replay` | no — never acknowledged |
| 2 | after WAL append, before memory apply | complete record | replayed | no |
| 3 | mid-object-write | file in `objects/tmp/` | `CleanTmp` deletes it | no — object unreferenced |
| 4 | after object rename, before commit object | orphan blobs/trees | ignored; they are garbage | no |
| 5 | after commit object, before ref update | orphan commit | ignored; branch still points at the old commit; the WAL still holds the mutations, so they are replayed and the commit can be retried | no |
| 6 | mid-ref-rename | atomic — old or new, never partial | whichever landed | no |
| 7 | after ref update, before WAL truncate | new commit *and* a stale WAL | WAL replayed **on top of** the new commit | no — records are absolute, replay is a no-op |
| 8 | after WAL truncate, before dirty clear | consistent | dirty set is in-memory only, rebuilt from the (now empty) WAL | no |
| 9 | mid-`SetHead` during checkout | atomic | old or new branch; memory rebuilt from whichever | no |
| 10 | disk full during WAL append | partial record | truncated; the write returned an error to the client | no — client was told |
| 11 | disk full during object write | temp file, no rename | `CleanTmp`; commit returned an error | no |

**Row 7 is the one worth dwelling on**, because it is where the design pays off. After the ref moves, the WAL still contains mutations that are now also inside the commit. Recovery materializes the new commit and *then* replays those records. Because a record says "K is now V" rather than "change K", reapplying it produces the same state. The dirty set is rebuilt from those keys, so the next `COMMIT` re-commits them — producing a tree identical to the one that already exists, deduplicated to the same object ID, and a commit that is a no-op. Ugly, harmless, correct.

Contrast with what would happen if mutations were relative. A record saying "append X to K" replayed after the commit would double the append. That is the entire reason for Invariant 4.

### What is *not* protected

State the gaps rather than implying completeness.

- **Corruption in the middle of the WAL** truncates everything after it. Justified because append-only logs are damaged only at the tail, but a malicious or bizarrely-failing disk breaks the assumption.
- **A corrupted object is fatal for anything referencing it.** `Get` detects it and errors, but there is no repair, no replica, no `FSCK` to find them proactively.
- **A corrupted ref file** makes `ParseID` fail and the database refuse to open. Recoverable by hand (the commit objects are still there, and you can find the newest and write its ID into the file), but there is no tooling.
- **`fsync` may lie.** Consumer drives with volatile write caches have been known to acknowledge before the data is durable. Nothing at this layer can help.
- **No two-phase commit across files.** Steps 5 and 6 are separate operations. We arranged for the intermediate state to be harmless rather than making it impossible.

### Startup, in order

`engine.Open` runs recovery in a specific sequence:

```go
1. MkdirAll(dir)
2. object.NewStore(dir/objects)   // creates objects/ and objects/tmp/
3. objects.CleanTmp()             // discard staged writes from a crash
4. refs.NewStore(dir)             // creates refs/heads/
5. if no HEAD -> SetHead("main")  // first run
6. materialize(HEAD commit)       // committed state
7. wal.Open() -> replay + truncate trailing garbage
8. ApplyBatch each replayed batch; rebuild the dirty set
```

Step 3 before step 6 is the only ordering that matters within the first half — a stale temp file could otherwise fill the disk over many crashes. Steps 6 then 7 is the recovery equation and is not negotiable.

### Fault-injection tests

Add to `internal/engine/engine_test.go`:

```go
// Uncommitted writes must survive a restart via the WAL.
func TestUncommittedWritesRecovered(t *testing.T) {
	dir := t.TempDir()
	e := openTestEngine(t, dir)
	mustSet(t, e, "committed", "yes")
	mustCommit(t, e, "c1")
	mustSet(t, e, "pending", "yes")
	if _, err := e.Del("committed"); err != nil {
		t.Fatal(err)
	}
	e.Close() // simulate a crash: no commit, WAL holds two mutations

	e2 := openTestEngine(t, dir)
	if v, ok := e2.Get("pending"); !ok || v != "yes" {
		t.Fatalf("pending lost: %q %v", v, ok)
	}
	if e2.Exists("committed") {
		t.Fatal("delete was not replayed")
	}
	if e2.DirtyCount() != 2 {
		t.Fatalf("dirty = %d, want 2", e2.DirtyCount())
	}
}

// A crash between "branch ref updated" and "WAL truncated" must be harmless.
func TestCrashBetweenCommitAndWALReset(t *testing.T) {
	dir := t.TempDir()
	e := openTestEngine(t, dir)
	mustSet(t, e, "k", "v")
	mustCommit(t, e, "c1")
	e.Close()

	// Re-create the situation by hand: append a record that the (already
	// committed) state already contains.
	e2 := openTestEngine(t, dir)
	mustSet(t, e2, "k", "v")
	e2.Close()

	e3 := openTestEngine(t, dir)
	if got := mustGet(t, e3, "k"); got != "v" {
		t.Fatalf("k = %q", got)
	}
}

func TestStrayTempObjectsAreCleaned(t *testing.T) {
	dir := t.TempDir()
	e := openTestEngine(t, dir)
	mustSet(t, e, "a", "1")
	mustCommit(t, e, "c")
	e.Close()

	stray := filepath.Join(dir, "objects", "tmp", "obj-crash")
	if err := os.WriteFile(stray, []byte("half written"), 0o644); err != nil {
		t.Fatal(err)
	}
	e2 := openTestEngine(t, dir)
	if _, err := os.Stat(stray); !os.IsNotExist(err) {
		t.Fatal("stray temp file survived recovery")
	}
	if got := mustGet(t, e2, "a"); got != "1" {
		t.Fatalf("a = %q", got)
	}
}
```

Notice the technique in all three: **produce the post-crash disk state directly, then open the database and assert.** `Close()` without a commit *is* a crash from the recovery code's point of view — the WAL is exactly what it would have been. A stray temp file is a `WriteFile` away. A torn WAL record is a truncation away (Phase 4). None of this needs a mock filesystem or a syscall interceptor, and the tests are readable years later.

A useful manual test that is hard to automate portably:

```bash
go run ./cmd/staash -dir ./data &
printf 'SET a 1\r\nSET b 2\r\n' | nc 127.0.0.1 6380
kill -9 %1        # SIGKILL: no cleanup, no flush, no deferred Close
go run ./cmd/staash -dir ./data
# the log line should report keys=2 uncommitted=2
```

`kill -9` cannot be caught, so nothing in our shutdown path runs. That is a genuine process crash, and everything must come back from the WAL alone.

### Checkpoint 11

```bash
go test ./... && go test -race ./...
```

```bash
git add . && git commit -m "test: add crash-recovery and fault-injection tests"
```

---

## Phase 12 — Testing strategy

The tests already exist, scattered through the phases. This phase organises them and explains the tooling.

### The pyramid, as built

| Layer | Where | What it proves |
| --- | --- | --- |
| Unit | `store`, `object`, `protocol`, `wal` | encoding round-trips, locking, parsing |
| Component | `engine` | commits, trees, branches, merges, recovery |
| Integration | `server` | the real protocol over a real socket |
| Fuzz | `protocol` | the parser cannot be crashed by arbitrary input |
| Concurrency | `store`, `server` (with `-race`) | no data races under real parallelism |
| Benchmark | `store`, `wal`, `engine`, `server` | performance regressions |

### Table-driven tests

Go's idiom for anything with many similar cases (`TestParse` in Phase 3 is the example). Two rules that make them useful rather than annoying:

- Use `t.Errorf`, not `t.Fatalf`, inside the loop, so one bad case does not hide the other nine.
- Include the input in every failure message. `Parse("SET a \"b") = ...` tells you what broke; `mismatch at index 5` does not.

### `-race`

The race detector instruments memory accesses and reports when two goroutines touch the same address without synchronisation and at least one writes. It finds bugs that have never manifested — the ones that will appear in production at 3 a.m. under load you never tested.

```bash
go test -race ./...
```

It slows execution roughly 10x and increases memory use, so it is a CI and pre-commit tool, not a default. It only detects races on code paths that actually execute, which is why `TestConcurrentAccess` and `TestConcurrentClients` exist: they exist to *give the detector something to detect*.

### `-fuzz`

Fuzzing generates inputs, keeps the ones that reach new code paths, and mutates them. It is the right tool wherever untrusted bytes meet a parser — which for us means the protocol parser, and it earned its place immediately:

```go
// FuzzParse asserts only that the parser never panics and never invents a
// command out of nothing.
func FuzzParse(f *testing.F) {
	seeds := []string{"", "SET a 1", `SET a "b c"`, `GET "`, "\x00\x01", strings.Repeat("a ", 100)}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, line string) {
		cmd, err := Parse(line)
		if err != nil {
			return
		}
		if cmd.Name == "" {
			t.Fatalf("parsed %q into an empty command name", line)
		}
	})
}
```

```bash
go test -run xxx -fuzz FuzzParse -fuzztime 30s ./internal/protocol/
```

Within seconds this reports:

```
--- FAIL: FuzzParse (0.02s)
    protocol_test.go:74: parsed "\"\"" into an empty command name
    Failing input written to testdata/fuzz/FuzzParse/2a2fdac69e4311b8
```

That is the bug fixed in Phase 3 with the `tokens[0] == ""` guard. **The failing input is written to `testdata/` and becomes a permanent seed** — `go test` (without `-fuzz`) replays every saved corpus entry from then on, so a fixed bug cannot silently return. Commit that directory.

Good fuzz properties are invariants, not expected outputs: "never panics", "round-trips", "output satisfies X". `DecodeTree` and `DecodeCommit` are excellent additional targets:

```go
// Exercise: add this to internal/object.
func FuzzDecodeTree(f *testing.F) {
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, payload []byte) {
		tree, err := DecodeTree(payload)
		if err != nil {
			return
		}
		// Anything that decodes must re-encode to the same bytes,
		// modulo the sorting invariant.
		if !bytes.Equal(NewTree(tree.Entries).Encode(), tree.Encode()) {
			t.Fatal("decode/encode is not stable")
		}
	})
}
```

> **Exercise.** That property is *not currently true*, and finding out why is the point. `DecodeTree` accepts entries in any order, but `NewTree` sorts them. Decide which behaviour you want — reject unsorted trees at decode time (strict canonical form, better integrity) or sort on decode (lenient) — implement it, and make the fuzz test pass. Strictness is the better answer here: an unsorted tree can only come from a buggy or malicious writer, and accepting it would let two byte-different objects represent the same keyspace.

### The integration test harness

Integration tests need a client. Twenty lines gets one:

```go
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
```

Three things this gets right:

- **`Addr: "127.0.0.1:0"`.** The OS picks a free port and `srv.Addr()` reports it. Hard-coding a port makes tests fail when run in parallel or when something else is listening.
- **`t.TempDir()`** gives each test a fresh directory that the framework deletes afterwards. Tests never share state.
- **`t.Cleanup`** instead of `defer`, so helper functions can register their own teardown.

`readReply` being recursive for arrays is what makes it a genuine protocol test — it will notice if we ever emit an array header whose count does not match the elements that follow.

### The remaining integration tests

```go
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
	if got := c.do("SET k %s", strings.Repeat("v", 1024)); !strings.HasPrefix(got, "-ERR request too long") {
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
```

> **Warning.** Note `t.Error` rather than `t.Fatal` inside the goroutines of `TestConcurrentClients`. `t.Fatal` calls `runtime.Goexit`, which terminates the *calling* goroutine — that goroutine never reaches `wg.Done()`, and the test hangs forever instead of failing. This is a common and genuinely confusing mistake.

`TestGracefulShutdownUnblocksClients` deserves its own mention: it does not assert on data at all, it asserts that `Close()` *returns*. Without the connection-tracking set from Phase 2 it deadlocks in `wg.Wait()`, and the timeout turns a hang into a readable failure.

### Coverage

```bash
go test -cover ./...
go test -coverprofile=cover.out ./... && go tool cover -html=cover.out
```

Coverage tells you what has *never run*, which is useful. It does not tell you what is *correct*: a test that calls every function and asserts nothing scores 100%. Use it to find untested error paths, not as a target.

### The commands worth knowing

```bash
go test ./...                                # everything
go test -race ./...                          # with the race detector
go test -run TestMerge ./internal/engine/    # one test by regex
go test -v ./internal/server/                # verbose
go test -count=1 ./...                       # bypass the result cache
go test -count=20 -run TestConcurrent ./...  # flush out flakiness
go test -run xxx -fuzz FuzzParse ./internal/protocol/
go test -timeout 30s ./...                   # fail hangs instead of waiting
```

`-count=1` is the one people search for: Go caches successful test results, and a test that depends on something outside its source (an environment variable, a file) can appear to pass without running.

### Checkpoint 12

```bash
go test ./... && go test -race ./... && go vet ./...
```

```bash
git add . && git commit -m "test: add integration harness, fuzzing and concurrency tests"
```

---

## Phase 13 — Benchmarks and profiling

### Writing a benchmark

```go
func BenchmarkEngineSetNoSync(b *testing.B) {
	e := benchEngine(b, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := e.Set("key", "value"); err != nil {
			b.Fatal(err)
		}
	}
}
```

The framework calls the function repeatedly with increasing `b.N` until timings stabilise. Three habits:

- **`b.ResetTimer()`** after setup, or you are benchmarking `t.TempDir()`.
- **`b.ReportAllocs()`** — allocations per operation are often the more actionable number, because they predict GC pressure.
- **Use the result.** The compiler can delete a call whose result is discarded. Assigning to a package-level `var sink` prevents it. Ours all return errors that we check, which is enough.

The remaining benchmarks are short. In `internal/store/store_test.go`:

```go
func BenchmarkSet(b *testing.B) {
	s := New()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Set("key", "value")
	}
}

func BenchmarkGet(b *testing.B) {
	s := New()
	s.Set("key", "value")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Get("key")
	}
}

// RunParallel spreads iterations across GOMAXPROCS goroutines, which is the
// only way to see whether RWMutex read parallelism is actually helping.
func BenchmarkGetParallel(b *testing.B) {
	s := New()
	s.Set("key", "value")
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get("key")
		}
	})
}
```

In `internal/wal/wal_test.go`, the pair that produces the most important number in this phase:

```go
func BenchmarkAppendSync(b *testing.B) {
	w, _, err := Open(filepath.Join(b.TempDir(), "wal.log"), true)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	muts := []store.Mutation{{Op: store.OpSet, Key: "key", Value: "value"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(muts); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendNoSync(b *testing.B) {
	w, _, err := Open(filepath.Join(b.TempDir(), "wal.log"), false)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	muts := []store.Mutation{{Op: store.OpSet, Key: "key", Value: "value"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(muts); err != nil {
			b.Fatal(err)
		}
	}
}
```

Now `internal/engine/bench_test.go` — the setup helper and the two most interesting cases:

```go
func benchEngine(b *testing.B, sync bool) *Engine {
	b.Helper()
	e, err := Open(Options{Dir: b.TempDir(), SyncWAL: sync})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { e.Close() })
	return e
}

// BenchmarkCommit measures the cost of a commit that touches one key in a
// database of `size` keys. If tree sharing works, the numbers should be
// roughly flat as size grows.
func BenchmarkCommit(b *testing.B) {
	for _, size := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("db=%d", size), func(b *testing.B) {
			e := benchEngine(b, false)
			muts := make([]store.Mutation, size)
			for i := range muts {
				muts[i] = store.Mutation{Op: store.OpSet, Key: fmt.Sprintf("key%06d", i), Value: "value"}
			}
			if err := e.Apply(muts); err != nil {
				b.Fatal(err)
			}
			if _, err := e.Commit("seed"); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := e.Set("key000000", fmt.Sprintf("v%d", i)); err != nil {
					b.Fatal(err)
				}
				if _, err := e.Commit("bench"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
```

The rest of `bench_test.go` follows the same shape:

```go
func BenchmarkEngineSetSync(b *testing.B) {
	e := benchEngine(b, true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := e.Set("key", "value"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEngineGet(b *testing.B) {
	e := benchEngine(b, false)
	_ = e.Set("key", "value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Get("key")
	}
}

func BenchmarkEngineDel(b *testing.B) {
	e := benchEngine(b, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.Del("key"); err != nil {
			b.Fatal(err)
		}
	}
}

// One WAL record and one lock acquisition for 100 keys: this is the
// measurement that justifies transactions as a performance feature.
func BenchmarkEngineBatch100(b *testing.B) {
	e := benchEngine(b, false)
	muts := make([]store.Mutation, 100)
	for i := range muts {
		muts[i] = store.Mutation{Op: store.OpSet, Key: fmt.Sprintf("k%d", i), Value: "v"}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := e.Apply(muts); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMaterialize(b *testing.B) {
	e := benchEngine(b, false)
	muts := make([]store.Mutation, 5000)
	for i := range muts {
		muts[i] = store.Mutation{Op: store.OpSet, Key: fmt.Sprintf("key%06d", i), Value: "value"}
	}
	if err := e.Apply(muts); err != nil {
		b.Fatal(err)
	}
	id, err := e.Commit("seed")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.materialize(id); err != nil {
			b.Fatal(err)
		}
	}
}
```

And in `internal/server/bench_test.go`, the whole client path:

```go
func benchServer(b *testing.B) string {
	b.Helper()
	eng, err := engine.Open(engine.Options{Dir: b.TempDir(), SyncWAL: false})
	if err != nil {
		b.Fatal(err)
	}
	srv := New(eng, Config{Addr: "127.0.0.1:0", IdleTimeout: time.Minute})
	if err := srv.Listen(); err != nil {
		b.Fatal(err)
	}
	go srv.Serve()
	b.Cleanup(func() {
		srv.Close()
		eng.Close()
	})
	return srv.Addr()
}

func BenchmarkRoundTrip(b *testing.B) {
	addr := benchServer(b)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	r := bufio.NewReader(conn)
	req := []byte("SET key value\r\n")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(req); err != nil {
			b.Fatal(err)
		}
		if _, err := r.ReadString('\n'); err != nil {
			b.Fatal(err)
		}
	}
}
```

### Running them

```bash
go test -run '^$' -bench . ./internal/...            # all benchmarks, no tests
go test -run '^$' -bench BenchmarkCommit -benchmem ./internal/engine/
go test -run '^$' -bench . -benchtime 5s ./internal/store/
```

`-run '^$'` matches no test, so only benchmarks run.

### Actual numbers

Measured on the machine this tutorial was written on (Linux, container filesystem, Go 1.22). Absolute numbers will differ on your hardware; the **ratios** are the point.

| Benchmark | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| `store.Get` | 21 | 0 | 0 |
| `engine.Get` | 21 | 0 | 0 |
| `engine.Set` (no fsync) | 682 | 42 | 2 |
| `engine.Set` (fsync) | 142,945 | 42 | 2 |
| `engine.Del` (no fsync) | 691 | 32 | 2 |
| `wal.Append` (no fsync) | 572 | — | — |
| `wal.Append` (fsync) | 138,603 | — | — |
| `engine.Apply` batch of 100 | 7,912 | 2,576 | 2 |
| TCP round-trip `SET` (no fsync) | 15,604 | — | — |
| `Commit`, 1 key in a 100-key db | 3,108,933 | 72,459 | 334 |
| `Commit`, 1 key in a 1,000-key db | 3,524,589 | 237,735 | 508 |
| `Commit`, 1 key in a 10,000-key db | 4,812,102 | 781,326 | 559 |

Five things these numbers say:

1. **Reads are essentially free.** 21 ns, zero allocations. A read is a map lookup under an `RLock`; there is nothing left to optimise.
2. **fsync dominates everything.** 143 µs versus 0.68 µs — a factor of 210. On a durable configuration, the database is an fsync benchmark with a key-value store attached. No amount of Go optimisation moves that number; only batching (group commit) or turning it off does.
3. **Batching is the biggest available win.** 100 keys in one `Apply` costs 7.9 µs total (79 ns/key) versus 68 µs as individual calls, and with fsync on it is one sync instead of 100. This is the argument for transactions as a *performance* feature, not just a correctness one.
4. **The network layer costs ~15 µs**, twenty times the engine's non-durable write. Parsing and dispatch are noise; this is syscalls and scheduling. Pipelining would amortise it.
5. **Tree sharing works.** A 100x larger database makes a commit 1.5x slower, not 100x. Without structural sharing that last row would be two orders of magnitude worse. The residual growth is real and explainable: as the database grows the root tree fills toward its 256 entries and each shard holds more keys, so the two trees we rewrite get bigger. That is exactly the ceiling that motivates a deeper trie in Phase 14.

Also note the absolute commit cost: ~3 ms even for a tiny database, dominated by four or five `fsync` calls in `object.Put` (one per object, plus directory syncs). Commits are rare, so we accept it; the fix, if we needed one, would be to write all objects and fsync once at the end.

### Profiling

Benchmarks tell you *that* something is slow. Profiles tell you *where*.

```bash
go test -run '^$' -bench BenchmarkCommit -cpuprofile cpu.out -memprofile mem.out ./internal/engine/
go tool pprof cpu.out
```

Inside `pprof`:

```
(pprof) top20          # functions by self time
(pprof) top -cum       # by cumulative time (includes callees)
(pprof) list writeTree # annotated source, line by line
(pprof) web            # SVG call graph (needs graphviz)
(pprof) peek fsync     # callers and callees of anything matching
```

For memory, `top` shows allocation sites; `-sample_index=alloc_objects` counts allocations rather than bytes, which is usually what you want when chasing GC pressure.

For a running server, add:

```go
import _ "net/http/pprof"

go func() {
	log.Println(http.ListenAndServe("127.0.0.1:6060", nil))
}()
```

Then:

```bash
go tool pprof http://127.0.0.1:6060/debug/pprof/profile?seconds=30   # CPU
go tool pprof http://127.0.0.1:6060/debug/pprof/heap                 # heap
go tool pprof http://127.0.0.1:6060/debug/pprof/block                # blocking
curl http://127.0.0.1:6060/debug/pprof/goroutine?debug=1             # goroutine dump
```

> **Warning.** `net/http/pprof` registers handlers on the default mux and exposes internals and a CPU-burning endpoint. Bind it to localhost, never to a public interface.

The goroutine dump is worth knowing for its own sake: if the server ever stops accepting connections, that endpoint shows you every goroutine and where it is blocked, which usually identifies a leak or a deadlock in seconds.

### How to actually optimise

The discipline, in order:

1. **Measure first.** Write the benchmark before the optimisation. Without a number, "faster" is an opinion.
2. **Profile, do not guess.** Everyone's intuition about hot spots is bad. The fsync result above is a good example: a reasonable person might have spent a day optimising the tree encoder, which does not appear in the profile at all.
3. **Fix the top item, then re-measure.** Profiles shift after every change.
4. **Use `benchstat` for comparisons.** Single runs are noisy:
   ```bash
   go test -run '^$' -bench . -count=10 ./internal/engine/ > old.txt
   # make the change
   go test -run '^$' -bench . -count=10 ./internal/engine/ > new.txt
   go install golang.org/x/perf/cmd/benchstat@latest
   benchstat old.txt new.txt
   ```
   It reports the delta and whether it is statistically significant.
5. **Stop when it is fast enough.** Every optimisation costs readability. The 21 ns read path needs nothing.

Applied to what we measured, the ranked list of real opportunities is: **group commit** (batch fsyncs across concurrent clients — the 210x factor), **pipelining** (amortise the 15 µs round trip), **sharded locks** (only once the first two are done and the mutex actually shows up in a block profile). Nothing in the tree code is worth touching.

### Checkpoint 13

```bash
go test -run '^$' -bench . ./internal/...
```

```bash
git add . && git commit -m "perf: add benchmarks for store, wal, engine and server"
```

---

## Phase 14 — Where this could go

Separated into two piles, because they are very different kinds of work.

### Reasonable next steps

Each of these is a weekend or less, fits the existing architecture, and does not require rethinking anything.

**Group commit.** The single biggest performance win available. Instead of every writer calling `fsync` itself, writers append to a shared buffer, one designated writer syncs, and everyone waiting is released. Ten concurrent writers cost one fsync instead of ten. Shape: a `sync.Cond` or a channel of pending batches plus a flusher goroutine.

**Periodic fsync.** A third durability mode between "every write" and "never": sync at most every N milliseconds, bounding worst-case loss to N ms of writes. One `time.Ticker` and a dirty flag in the WAL.

**Pipelining.** Currently a client waits for each reply. Reading all buffered commands, executing them, and flushing once would multiply throughput for bulk loads. The server already reads through `bufio`; the change is to not flush until `reader.Buffered() == 0`.

**WAL segments and size-triggered commits.** Our WAL is bounded by "work since the last commit", which is fine until a client never commits. Roll the log at a size threshold, or auto-commit when it exceeds a limit.

**Garbage collection.** Deleted branches and aborted commits leave unreachable objects forever. A mark-and-sweep is straightforward: walk from every ref, mark reachable object IDs, delete the rest. The hard part is concurrency — an object being written by an in-flight commit is not yet reachable from any ref, so you need either a stop-the-world pause or a grace period based on file mtime.

**Compression.** Objects are stored raw. `compress/flate` on blobs above a size threshold would cut disk use substantially for text values, at some CPU cost on read. Git does exactly this. Keep the hash over the *uncompressed* canonical encoding so IDs do not depend on the compression level.

**TTL and expiration.** `SET k v EX 60`. An expiry map plus lazy deletion on read and a background sweeper. The interesting design question is version control: is an expiry part of the committed state, or a property of the live keyspace only? (The second is much simpler and probably right.)

**More commands.** `MGET`, `MSET`, `INCR`, `GETSET`, `SCAN` with a cursor, `TYPE`. Note that `INCR` is not a pure win: it is a *relative* operation, and Invariant 4 says WAL records are absolute. Implement it by reading the current value under the lock and writing an absolute `SET` record — do not add a relative record type.

**`WATCH` for optimistic transactions.** Record the version of each watched key at `WATCH`, re-check at `EXEC`, abort if any changed. This turns our lost-update-permitting transactions into something usable for read-modify-write, and it needs a per-key version counter — a small, contained change.

**`DIFF <commit> <commit>`.** All the machinery exists: `commitKeys` on both sides and compare blob IDs. Twenty lines, and genuinely useful for debugging.

**`RESET --hard`.** Discard uncommitted changes: reset the WAL, clear the dirty set, re-materialize HEAD. It makes the `CHECKOUT` dirty-check tolerable to live with.

**Metrics.** Counters for commands, latency histograms, WAL size, object count, dirty keys. Expose on a `/metrics` HTTP endpoint in Prometheus format; the client library is small, or hand-format the text.

**An HTTP admin API.** Read-only endpoints for `LOG`, `SHOW`, `BRANCHES` and `STATUS` make debugging pleasant and are trivial next to the TCP server.

**Authentication.** An `AUTH <password>` command gating everything else, compared with `crypto/subtle.ConstantTimeCompare`. Necessary before this listens on anything but localhost — and TLS via `crypto/tls` on the listener is another few lines.

### Major architectural changes

These require rethinking the design, not extending it.

**MVCC.** Today a reader sees the live map, and a snapshot copies it. Under multi-version concurrency control, each write creates a new version and readers hold a consistent snapshot without copying or blocking. This is what makes real repeatable-read and serializable transactions possible. It requires replacing `map[string]string` with a persistent (immutable, structurally shared) data structure — which, satisfyingly, is the *same* structure that would improve the tree layer. Doing both at once, so the in-memory state and the committed tree are the same shape, is the deepest available redesign and would make `Commit` nearly free.

**Fine-grained locking.** Shard the store into N independent maps with N locks. Straightforward on its own, but it breaks multi-key atomicity: `ApplyBatch` would have to take several locks in a defined order, and transactions get much harder. Do MVCC instead, or accept the coarse lock. Do not sharded-lock a system that needs atomic batches without a plan for both.

**A deeper tree.** Two levels cap out: a million keys means 4,000-entry shards, and every commit rewrites one. A HAMT or B-tree gives O(log n) commits and near-perfect sharing at any size. This is the principled fix for the growth visible in the `BenchmarkCommit` numbers.

**Data larger than memory.** Everything here assumes the keyspace fits in RAM. Removing that assumption means an on-disk index (a B-tree or an LSM tree) and a buffer pool — that is a different project, and a much bigger one.

**Replication.** The WAL is already a replication log; that is not a coincidence, it is why real systems ship WAL records. Add a follower mode that streams records from a leader, and read-only replicas fall out. Then: what happens when the leader dies?

**Raft.** The answer to that question. Consensus over the WAL gives automatic failover and a linearizable multi-node database. `hashicorp/raft` is the practical route; implementing Raft yourself is an excellent project in its own right and roughly the size of this entire tutorial.

**Distributed sharding.** Split the keyspace across nodes by hash. Now branches and commits span machines, and a global snapshot needs distributed coordination — versioning and sharding interact badly, and resolving that is genuinely open-ended.

**Merge policies.** Our merge aborts on conflict. Real systems offer strategies: last-writer-wins, prefer-ours, prefer-theirs, custom resolvers per key prefix, or CRDT values that merge by construction. The last one is the most interesting: if values were counters or sets with defined merge semantics, conflicts would largely disappear.

---

## What you built

A versioned key-value database, in about 2,300 lines of Go with no dependencies outside the standard library.

```
   TCP clients (many, concurrent)
        |
   goroutine per connection
        |  bounded line reads, deadlines, graceful shutdown
        v
   protocol: inline text requests, length-prefixed RESP-style replies
        |
   session: command dispatch, per-connection transaction buffer
        |
   engine: one mutex for writes and version control
        |
        +--> store: map[string]string under an RWMutex        (reads: 21 ns)
        |
        +--> wal: CRC'd append-only log of absolute mutations  (durability)
        |
        +--> object store: SHA-256 content-addressed, immutable
        |         blobs -> shard trees -> root tree -> commits (the DAG)
        |
        +--> refs: HEAD and refs/heads/*, updated by atomic rename
```

The whole thing rests on the recovery equation from Phase 0 —

```
state = materialize(HEAD commit) + replay(WAL)
```

— and every design decision downstream follows from keeping it true.

### What you learned

**Go.** Package boundaries that follow dependency direction rather than taxonomy. Errors as values, with sentinels for identity, wrapping for context, and struct errors when the caller needs data. `defer` for cleanup that survives refactoring. Table-driven tests, `t.Helper`, `t.Cleanup`, `t.TempDir`. Benchmarks, fuzzing, and the race detector as everyday tools rather than exotica.

**Concurrency.** Why a Go map needs synchronisation and what the runtime does if you skip it. `RWMutex` versus `Mutex` and when the read-side parallelism is worth it. Goroutine-per-connection. Graceful shutdown via connection tracking and a `WaitGroup`. Why locks are never held across an unbounded wait — the argument that made transactions buffer rather than lock. Why `t.Fatal` in a goroutine hangs your test suite.

**Networking.** TCP has no message boundaries; framing is your job. The three framing strategies and why request and reply can reasonably choose differently. Deadlines as the defence against slow and absent clients. Bounded reads as the defence against hostile ones. `bufio` on both directions, and flushing exactly once per reply.

**Storage.** Write-ahead logging and why order is the whole mechanism. Record framing with length prefixes and checksums. Why every length read from disk needs a bound. `fsync` as the only thing that means durable, and its real cost — 210x, measured. `rename` as the only atomic file operation you get, and the write-temp/fsync/rename/fsync-dir pattern built from it. Torn writes, and why an append-only log can safely truncate at the first damaged record.

**Databases.** Recovery as an equation to preserve rather than a procedure to follow. Idempotence as the property that makes recovery simple, and absolute-not-relative records as the way to get it. Atomicity through batching under a single lock. Being precise about isolation, and refusing to claim more than you implement.

**Version control.** Content addressing, and how deduplication, integrity, immutability and cheap equality all fall out of one idea. Immutable objects plus mutable refs. Why commit DAGs cannot contain cycles by construction. Structural sharing, measured: 100x more data, 1.5x slower commits. Three-way merges, merge bases, and why comparing hashes is enough.

**Reliability.** Enumerating crash points instead of hoping. Choosing orderings so intermediate states are harmless. Fault injection by writing the damaged state directly. Naming what is *not* protected — mid-log corruption, lying disks, corrupted refs — because an unstated gap is worse than a known one.

### Final project tree

```
staash/
├── go.mod
├── .gitignore
├── cmd/
│   └── staash/
│       └── main.go                  flags, signals, wiring
└── internal/
    ├── fsutil/
    │   └── fsutil.go                WriteFileAtomic, SyncDir
    ├── store/
    │   ├── store.go                 in-memory map, Mutation, ApplyBatch
    │   └── store_test.go            unit + concurrency + benchmarks
    ├── wal/
    │   ├── wal.go                   framing, append, replay, truncate
    │   └── wal_test.go              torn records, bit flips, benchmarks
    ├── object/
    │   ├── object.go                ID, Kind, Encode/Decode/Hash
    │   ├── store.go                 on-disk CAS with atomic writes
    │   ├── tree.go                  sorted entry lists
    │   ├── commit.go                commit encoding
    │   └── object_test.go           round-trips, dedup, corruption
    ├── refs/
    │   └── refs.go                  HEAD and refs/heads/*
    ├── protocol/
    │   ├── protocol.go              request tokenizer
    │   ├── reply.go                 RESP-style writer
    │   └── protocol_test.go         table tests + FuzzParse
    ├── engine/
    │   ├── engine.go                recovery, apply, trees, commits, branches
    │   ├── merge.go                 merge base + three-way merge
    │   ├── engine_test.go           commits, branches, merges, crash recovery
    │   └── bench_test.go            set/get/del/commit/materialize
    └── server/
        ├── server.go                listener, connections, shutdown
        ├── session.go               dispatch, transactions
        ├── server_test.go           integration harness + tests
        └── bench_test.go            TCP round-trip
```

Data directory:

```
data/
├── HEAD                      "ref: refs/heads/main\n"
├── wal.log                   uncommitted mutations only
├── refs/
│   └── heads/
│       ├── main              64 hex chars + newline
│       └── feature
└── objects/
    ├── tmp/                  staging; emptied at startup
    ├── 02/1c3e...            blob, tree or commit
    ├── 3e/9f4c...
    └── ...
```

### Running it

```bash
go build -o staash ./cmd/staash

./staash                                   # 127.0.0.1:6380, ./data, fsync on
./staash -addr 127.0.0.1:7000 -dir /var/lib/staash
./staash -sync=false                       # fast, loses recent writes on power cut
./staash -idle-timeout 1m

go test ./...
go test -race ./...
go test -run '^$' -bench . ./internal/...
go test -run xxx -fuzz FuzzParse -fuzztime 30s ./internal/protocol/
```

### Command reference

| Command | Reply | Notes |
| --- | --- | --- |
| `PING [msg]` | `+PONG` / bulk | |
| `SET key value` | `+OK` / `+QUEUED` | quote values containing spaces |
| `GET key` | bulk / nil | |
| `DEL key` | `:0`/`:1` / `+QUEUED` | |
| `EXISTS key` | `:0`/`:1` | |
| `KEYS` | array | sorted; includes transaction overlay |
| `DBSIZE` | integer | |
| `BEGIN` / `EXEC` / `ROLLBACK` | `+OK` / `:n` / `+OK` | `DISCARD` aliases `ROLLBACK` |
| `COMMIT "message"` | `+<commit id>` | not allowed in a transaction |
| `LOG [n]` | array | default 20, newest first |
| `SHOW [commit]` | bulk | defaults to HEAD |
| `HEAD` | bulk | branch and commit |
| `STATUS` | bulk | branch, uncommitted keys, total keys |
| `BRANCH name` | `+OK` | at the current commit |
| `BRANCHES` | array | `*` marks the current branch |
| `CHECKOUT name` | `+OK` | refused if there are uncommitted changes |
| `MERGE name` | `+<kind> <id>` | `-ERR CONFLICT k1 k2 ...` on conflict |
| `QUIT` | `+OK` | closes the connection |

### Example session

Verbatim from a running server, with replies decoded for readability.

```
> PING
+PONG

> SET name alice
+OK
> SET city "berlin, de"
+OK
> GET city
berlin, de
> KEYS
[city, name]
> STATUS
branch main, 2 uncommitted key(s), 2 key(s) total

> COMMIT "initial import"
+fda6a9293d5015d0a7190fba24bdfc46797939e8d42b27dcc8f639b7902e11be
> LOG
fda6a9293d50 2026-08-26T07:37:05Z initial import

> BRANCH feature
+OK
> CHECKOUT feature
+OK
> SET name bob
+OK
> SET role admin
+OK
> COMMIT "feature work"
+83ac73e40975ba166f350bdc7d815fa84e458ddfda19de14047229ed70dd2fd1

> CHECKOUT main
+OK
> GET name
alice                          <- main is untouched
> SET city hamburg
+OK
> COMMIT "main work"
+99cdabb599cb0f6ea5a43127db3809981343f673c4a1810e66ddcf1333efbdee

> MERGE feature
+merge d1b170a7926d           <- both sides changed different keys
> GET name
bob
> GET role
admin
> LOG
d1b170a7926d 2026-08-26T07:37:05Z merge branch "feature" into "main"
99cdabb599cb 2026-08-26T07:37:05Z main work
83ac73e40975 2026-08-26T07:37:05Z feature work
fda6a9293d50 2026-08-26T07:37:05Z initial import

> BEGIN
+OK
> SET a 1
+QUEUED
> GET a
1                              <- read-your-own-writes
> EXEC
:1
> KEYS
[a, city, name, role]
> HEAD
main d1b170a7926db771ada167bbc2dc42a050f5b8db49b9efb6ae627588cdd646f8
```

And the conflict case:

```
> CHECKOUT main
+OK
> SET name bob
+OK
> COMMIT "main renames"
+55de...
> CHECKOUT other
+OK
> SET name charlie
+OK
> COMMIT "other renames"
+91ab...
> CHECKOUT main
+OK
> MERGE other
-ERR CONFLICT name
> GET name
bob                            <- aborted merge changed nothing
```

### Where to go from here

Five directions, all reachable from this codebase:

- **A replicated database.** Stream WAL records to followers. The log is already the right shape; the work is the network protocol and follower catch-up.
- **A Raft implementation.** Put consensus under the WAL and get automatic failover. Comparable in size to this whole tutorial, and the best possible follow-on project.
- **A real storage engine.** Replace the in-memory map with an LSM tree or a B-tree and remove the fits-in-RAM assumption. This is where you learn about buffer pools, compaction and page layout.
- **A benchmark harness.** A load generator with configurable key distributions, read/write ratios and concurrency, reporting p50/p99/p999. You will learn more about your own system from p999 than from any profile.
- **A cloud-native service.** Containerise it, add health checks and Prometheus metrics, run it under Kubernetes with a persistent volume, and discover which of the assumptions in Phase 0 survive contact with an orchestrator that can kill your process at any moment.

Whichever you pick, the useful habit from this project carries over: **state the invariant, choose the ordering that keeps it true when things fail, and then write the test that proves it.**
