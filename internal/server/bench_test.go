package server

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/CM-exe/staash/internal/engine"
)

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

	for b.Loop() {
		if _, err := conn.Write(req); err != nil {
			b.Fatal(err)
		}
		if _, err := r.ReadString('\n'); err != nil {
			b.Fatal(err)
		}
	}
}
