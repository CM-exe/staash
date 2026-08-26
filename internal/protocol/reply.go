package protocol

import (
	"bufio"
	"strconv"
	"strings"
)

// Writer encodes replies. Length-prefixed bulk strings are what make the
// protocol binary safe: the reader never has to scan for a delimiter that might appear in the data.
type Writer struct {
	w *bufio.Writer
}

func NewWriter(w *bufio.Writer) *Writer { return &Writer{w: w} }

func (w *Writer) Simple(s string) error {
	_, err := w.w.WriteString("+" + s + "\r\n")
	return err
}

func (w *Writer) OK() error { return w.Simple("OK") }

func (w *Writer) BYE() error { return w.Simple("BYE") }

func (w *Writer) Banner(b string) error {
	_, err := w.w.WriteString(b + "\r\n")
	return err
}

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
