# Staash

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/staash-logo-dark.png">
    <img src="assets/staash-logo.png" alt="Staash logo" width="700">
  </picture>
</p>

<!-- <p align="center">
  <strong>Stash your data. Version everything.</strong>
</p> -->

<p align="center">
  A lightweight, versioned key-value database written in Go.
</p>

---

## What is Staash?

Staash is a lightweight, versioned key-value database written in Go.

It combines the simplicity of a Redis-like key-value store with the immutable history and branching model of Git.

The goal is to build a small, understandable storage engine that is useful on its own while serving as a deep exploration of databases, concurrency, persistence, networking, and version control.

Traditional key-value stores give you a current state:

```text
user:1 → alice
user:2 → bob
```

Staash lets you treat that state as something that can be **saved, versioned, branched, inspected, and restored**.

For example:

```text
$ staash set user:1 alice
OK

$ staash commit -m "initial users"
a81f2c

$ staash set user:1 bob
OK

$ staash commit -m "rename alice"
c42e91

$ staash log

c42e91  rename alice
a81f2c  initial users
```

You can then create an independent line of development:

```text
$ staash branch experiment
$ staash checkout experiment

$ staash set user:3 charlie
$ staash commit -m "add experimental user"
```

The underlying data is stored as immutable, content-addressed objects, allowing different versions and branches to share unchanged data.

---

## Why Staash?

Staash sits somewhere between a **database** and a **version-control system**.

It explores a simple question:

> What if the state of a database could be treated like a Git repository?

The project is designed to be:

* **Simple** — easy to understand and run
* **Persistent** — data survives process restarts
* **Concurrent** — multiple clients can interact with it
* **Versioned** — every committed state has a history
* **Branchable** — different states can evolve independently
* **Content-addressed** — immutable objects are identified by their hashes
* **Recoverable** — crashes should not casually destroy the database
* **Hackable** — the internals should remain understandable

Staash is **not intended to replace Redis, Git, or production databases**. It is a compact systems project designed to explore how these kinds of tools work internally.

---

## Core concepts

### Key-value store

At its simplest:

```text
SET user:1 alice
GET user:1
DEL user:1
```

The current state is represented as key-value pairs.

### Commits

A commit records a specific database state:

```text
Commit
 ├── parent
 ├── timestamp
 ├── message
 └── root snapshot
```

Commits are immutable.

### Branches

Branches are references to commits:

```text
             A
             │
             B
            / \
           C   D
           │
           E
```

Different branches can share most of their underlying data.

### Content-addressed storage

Objects are identified by their content:

```text
object = hash(content)
```

The same content therefore produces the same object ID.

This provides natural deduplication and makes immutable objects easy to reference.

### Persistence

The database does not rely solely on memory.

Staash progressively introduces:

* write-ahead logging
* durable objects
* snapshots
* recovery
* atomic filesystem operations

---

## Example

A typical session might look like:

```text
$ staash set user:1 alice
OK

$ staash set user:2 bob
OK

$ staash get user:1
alice

$ staash commit -m "initial users"
a81f2c

$ staash branch experiment
Created branch experiment

$ staash checkout experiment
Switched to experiment

$ staash set user:3 eve
OK

$ staash commit -m "add eve"
c71a92

$ staash log
c71a92  add eve
a81f2c  initial users
```

Switching back restores the previous database state:

```text
$ staash checkout main

$ staash get user:3
(nil)
```

---

## Architecture

The system is built from several relatively independent layers:

```text
                   Client
                     │
                     ▼
              ┌──────────────┐
              │ TCP Server   │
              └──────┬───────┘
                     │
                     ▼
              ┌──────────────┐
              │   Protocol   │
              └──────┬───────┘
                     │
                     ▼
              ┌──────────────┐
              │ Command Layer│
              └──────┬───────┘
                     │
              ┌──────▼───────┐
              │   KV Store   │
              └──────┬───────┘
                     │
          ┌──────────┼──────────┐
          ▼          ▼          ▼
        WAL       Snapshot   Transactions
                     │
                     ▼
              Object Database
                     │
                     ▼
             Immutable Objects
```

The versioning layer sits on top of the storage layer:

```text
              Branch
                │
                ▼
              Commit
             /      \
        parent      root
          │           │
       Commit        Tree
                      │
                 ┌────┴────┐
                 ▼         ▼
               Blob       Blob
```

---

## Features

### Current / planned

* [ ] In-memory key-value store
* [ ] `GET`
* [ ] `SET`
* [ ] `DEL`
* [ ] `EXISTS`
* [ ] `KEYS`
* [ ] Concurrent TCP clients
* [ ] Command protocol
* [ ] Persistence
* [ ] Write-ahead log
* [ ] Crash recovery
* [ ] Content-addressed object storage
* [ ] Immutable snapshots
* [ ] Commits
* [ ] Commit history
* [ ] Branches
* [ ] Checkout
* [ ] Transactions
* [ ] Three-way merge
* [ ] Conflict detection
* [ ] Concurrency tests
* [ ] Fuzz tests
* [ ] Benchmarks
* [ ] CPU and memory profiling

### Possible future features

* [ ] TTL / expiration
* [ ] WAL compaction
* [ ] Object garbage collection
* [ ] Compression
* [ ] MVCC
* [ ] Replication
* [ ] Raft
* [ ] Metrics
* [ ] HTTP administration API

---

## Project structure

The exact structure may evolve as the project grows, but the intended architecture is roughly:

```text
staash/
├── cmd/
│   └── staash/
│       └── main.go
│
├── internal/
│   ├── server/
│   ├── protocol/
│   ├── store/
│   ├── object/
│   ├── commit/
│   ├── branch/
│   ├── transaction/
│   └── persistence/
│
├── tests/
│
├── assets/
│   └── staash-logo.png
│
├── go.mod
├── .gitignore
├── LICENSE
└── README.md
```

The project intentionally avoids introducing abstractions before they are needed.

---

## Design philosophy

### Immutable data where possible

Once an object has been committed, it should not change.

Instead of modifying history, new objects are created.

This makes history easier to reason about and makes branching naturally cheap.

### Simple before clever

The first implementation should favor correctness and clarity over maximum performance.

Optimizations should be introduced after benchmarks identify actual bottlenecks.

### Explicit failure handling

Storage systems fail in interesting ways.

Staash therefore treats situations such as these as first-class engineering problems:

* interrupted writes
* truncated logs
* corrupted objects
* client disconnects
* concurrent access
* process crashes
* disk failures

### Understandable internals

The project is intentionally small enough that one developer can understand the entire architecture.

---

## Learning goals

Building Staash provides practical experience with:

### Go

* goroutines
* channels
* mutexes
* interfaces
* error handling
* package design
* `net`
* `os`
* `encoding`
* testing
* fuzzing
* benchmarking
* profiling

### Networking

* TCP
* connection lifecycle
* request framing
* protocols
* concurrent clients
* timeouts
* graceful shutdown

### Databases

* key-value storage
* snapshots
* write-ahead logs
* persistence
* transactions
* crash recovery
* indexing
* storage layouts

### Version control

* immutable objects
* content addressing
* commits
* DAGs
* branches
* references
* three-way merges
* conflict detection

### Systems engineering

* concurrency
* durability
* atomic filesystem operations
* failure modes
* performance measurement
* memory usage
* recovery

---

## Development

Clone the repository:

```bash
git clone https://github.com/CM-exe/staash.git
cd staash
```

Initialize the Go module if starting from scratch:

```bash
go mod init github.com/CM-exe/staash
```

Run the tests:

```bash
go test ./...
```

Run with the race detector:

```bash
go test -race ./...
```

Build:

```bash
go build ./cmd/staash
```

Run:

```bash
./staash
```

---

## Status

Staash is an educational systems project under active development.

The architecture is expected to evolve as new requirements expose weaknesses in earlier implementations.

The important part is not simply reaching a final feature list. The project is about understanding **why storage systems are designed the way they are**.

---

## License

Staash is released under the MIT License.

[MIT License](https://github.com/CM-exe/staash/?tab=MIT-1-ov-file&utm_source=chatgpt.com)

---

## The idea

> **Stash your data. Version everything.**

Store it.

Change it.

Commit it.

Branch it.

Merge it.

And understand what happens underneath.
