package protocol

import (
	"errors"
	"strings"
	"unicode"
)

// Command is a parsed request line.
type Command struct {
	Name string   // upper-cased verb, e.g. "GET"
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
		// '""' tokenizes to a single empty token; an empty verb is not a
		// command.
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
	case 'r':
		return '\r'
	case 't':
		return '\t'
	default:
		return c
	}
}
