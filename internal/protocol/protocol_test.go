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
		{in: "SET a     1   ", want: Command{Name: "SET", Args: []string{"a", "1"}}},
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
				t.Errorf("Parse(%q) = %+v; want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%q) = %v", tc.in, err)
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
		t.Errorf("got %q\nwant %q", sb.String(), want)
	}
}

// FuzzParse asserts only that the parser never panics and never invents a
// command out of nothing.
func FuzzParse(f *testing.F) {
	seeds := []string{"", "SET a 1", `SET a "b c"`, `GET "`, "\x00\x01", strings.Repeat("a ", 1000)}
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
