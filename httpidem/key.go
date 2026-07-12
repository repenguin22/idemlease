package httpidem

import (
	"fmt"
	"net/http"
	"strings"
)

// Header names used by the middleware.
const (
	// HeaderIdempotencyKey is the request header carrying the key.
	HeaderIdempotencyKey = "Idempotency-Key"
	// HeaderIdempotencyReplayed marks replayed responses ("true").
	HeaderIdempotencyReplayed = "Idempotency-Replayed"
)

// ParseKey parses an Idempotency-Key header value (REQUIREMENTS §4.1).
//
// The canonical form is the raw, unquoted string: after trimming
// leading and trailing OWS, any byte sequence of 1-255 bytes that
// contains no control characters (0x00-0x1F, 0x7F) is the key as-is,
// including internal whitespace and non-ASCII bytes. Values starting
// with a double quote are parsed as an RFC 8941 String for
// compatibility with draft-07 clients; the decoded inner string is the
// key, so `"abc"` and `abc` name the same key. Known trade-off: a raw
// key that itself starts with `"` must be sent in RFC 8941 form.
//
// Errors wrap ErrKeyInvalid.
func ParseKey(value string) (string, error) {
	v := strings.Trim(value, " \t")
	if v == "" {
		return "", fmt.Errorf("%w: empty value", ErrKeyInvalid)
	}
	var key string
	if v[0] == '"' {
		decoded, ok := parseSFString(v)
		if !ok {
			return "", fmt.Errorf("%w: malformed RFC 8941 string", ErrKeyInvalid)
		}
		key = decoded
	} else {
		for i := 0; i < len(v); i++ {
			if c := v[i]; c <= 0x1F || c == 0x7F {
				return "", fmt.Errorf("%w: control character 0x%02X", ErrKeyInvalid, c)
			}
		}
		key = v
	}
	if len(key) == 0 || len(key) > 255 {
		return "", fmt.Errorf("%w: key is %d bytes, want 1-255", ErrKeyInvalid, len(key))
	}
	return key, nil
}

// parseSFString parses an RFC 8941 §3.3.3 String: DQUOTE-delimited
// printable ASCII where only `\"` and `\\` escapes are allowed. The
// closing quote must end the input.
func parseSFString(s string) (string, bool) {
	if len(s) < 2 || s[0] != '"' {
		return "", false
	}
	var b strings.Builder
	i := 1
	for i < len(s) {
		switch c := s[i]; {
		case c == '\\':
			if i+1 >= len(s) {
				return "", false
			}
			next := s[i+1]
			if next != '"' && next != '\\' {
				return "", false
			}
			b.WriteByte(next)
			i += 2
		case c == '"':
			if i != len(s)-1 {
				return "", false
			}
			return b.String(), true
		case c < 0x20 || c > 0x7E:
			return "", false
		default:
			b.WriteByte(c)
			i++
		}
	}
	return "", false // unclosed quote
}

// KeyFromHeader extracts and parses the Idempotency-Key header.
// A missing header yields ErrKeyMissing; multiple headers or a grammar
// violation yield an error wrapping ErrKeyInvalid.
func KeyFromHeader(h http.Header) (string, error) {
	values := h.Values(HeaderIdempotencyKey)
	switch len(values) {
	case 0:
		return "", ErrKeyMissing
	case 1:
		return ParseKey(values[0])
	default:
		return "", fmt.Errorf("%w: %d Idempotency-Key headers, want exactly 1", ErrKeyInvalid, len(values))
	}
}
