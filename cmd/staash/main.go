package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/CM-exe/staash/internal/engine"
	"github.com/CM-exe/staash/internal/server"
)

func main() {
	var (
		addr    = flag.String("addr", "127.0.0.1:6380", "TCP address to listen on")
		dir     = flag.String("dir", "./data", "directory to store data")
		syncWAL = flag.Bool("sync", true, "fsync the write-ahead log on every write (slower but safer)")
	)
	flag.Parse()

	eng, err := engine.Open(engine.Options{Dir: *dir, SyncWAL: *syncWAL})
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer eng.Close()
	srv := server.New(eng, server.Config{Addr: *addr})
	if err := srv.Listen(); err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on %s", srv.Addr())

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil {
			log.Print(err)
		}
	case sig := <-sigCh:
		log.Printf("received signal %s, shutting down", sig)
		srv.Close()
	}
}
