package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/CM-exe/staash/internal/store"
)

const headerSize = 8

// MaxrecordSize bound how much memory a single corrupt length field can make
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

// Open opens (creating if needed) the log at path, replays it, truncates any
// trailing garbage, and positions the file at the end.
//
// syncOnAppened controls durability: true means fsync after every batch (slow, safe
// against power loss), false means rely on the OS page cache (fast, safe
// only against process crashes).
func Open(path string, syncOnAppened bool) (*WAL, [][]store.Mutation, error) {
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
		// Trailing bytes are an incomplete or corrupt record: a write that
		// was interrupted by a crash. Dropping them is correct because the client
		// never recevied an acknowledgement for that batch.
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
	return &WAL{f: f, path: path, offset: good, sync: syncOnAppened}, batches, nil
}

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
			return batches, off, nil // nonsense length; treat as torn
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

// Append writes one batch. On return (with sync enabled) the batch is durable.
func (w *WAL) Append(muts []store.Mutation) error {
	if len(muts) == 0 {
		return nil
	}
	payload := encodeBatch(muts)
	if len(payload) > MaxRecordSize {
		return fmt.Errorf("wal: batch too large: %d bytes", len(payload))
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
