package refs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/CM-exe/staash/internal/fsutil"
	"github.com/CM-exe/staash/internal/object"
)

var (
	ErrNoSuchBranch  = errors.New("refs: no such branch")
	ErrBadBranchName = errors.New("refs: invalid branch name")
)

// Store manages <dir>/HEAD and <dir>/refs/heads/<name>.
//
// Layout:
//
// <dir>/HEAD              -> "ref: refs/heads/main\n"
// <dir>/refs/heads/main   -> "<64 hex chars>\n"
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
