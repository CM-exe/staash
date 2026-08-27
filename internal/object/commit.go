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
// tree <hex>
// parent <hex>        (zero or more)
// time <RFC3339Nano>
// <blank line>
// <message bytes>
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
