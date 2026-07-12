package httpidem_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/repenguin22/idemlease/httpidem"
)

// TestParseKey is the §4.1 acceptance table (§12-2).
func TestParseKey(t *testing.T) {
	uuid := "123e4567-e89b-12d3-a456-426614174000"
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{"raw uuid", uuid, uuid, false},
		{"quoted uuid", `"` + uuid + `"`, uuid, false},
		{"quoted abc decodes to abc", `"abc"`, "abc", false},
		{"internal whitespace allowed", "order 42 v2", "order 42 v2", false},
		{"utf-8 key allowed", "注文-123", "注文-123", false},
		{"leading and trailing OWS trimmed", " \tabc\t ", "abc", false},
		{"escaped quote", `"a\"b"`, `a"b`, false},
		{"escaped backslash", `"a\\b"`, `a\b`, false},
		{"quote in the middle of a raw key", `a"b`, `a"b`, false},
		{"1 byte", "a", "a", false},
		{"255 bytes", strings.Repeat("a", 255), strings.Repeat("a", 255), false},

		{"empty", "", "", true},
		{"only OWS", " \t ", "", true},
		{"256 bytes", strings.Repeat("a", 256), "", true},
		{"control 0x1F rejected", "ab\x1fc", "", true},
		{"control DEL 0x7F rejected", "ab\x7fc", "", true},
		{"control NUL rejected", "a\x00b", "", true},
		{"unclosed quote", `"abc`, "", true},
		{"invalid escape", `"a\nb"`, "", true},
		{"escape at end of input", `"abc\`, "", true},
		{"trailing bytes after closing quote", `"abc"x`, "", true},
		{"non-ascii inside quotes", "\"café\"", "", true},
		{"quoted empty string", `""`, "", true},
		{"quoted 256 bytes", `"` + strings.Repeat("a", 256) + `"`, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := httpidem.ParseKey(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseKey(%q) = %q, want error", tt.in, got)
				}
				if !errors.Is(err, httpidem.ErrKeyInvalid) {
					t.Fatalf("err = %v, want ErrKeyInvalid", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseKey(%q): %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("ParseKey(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestQuotedAndRawKeysAreTheSameKey pins the compatibility rule: "abc"
// and abc must address the same record.
func TestQuotedAndRawKeysAreTheSameKey(t *testing.T) {
	raw, err := httpidem.ParseKey("abc")
	if err != nil {
		t.Fatal(err)
	}
	quoted, err := httpidem.ParseKey(`"abc"`)
	if err != nil {
		t.Fatal(err)
	}
	if raw != quoted {
		t.Fatalf("raw = %q, quoted = %q; want identical keys", raw, quoted)
	}
}

func TestKeyFromHeader(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		h := http.Header{}
		_, err := httpidem.KeyFromHeader(h)
		if !errors.Is(err, httpidem.ErrKeyMissing) {
			t.Fatalf("err = %v, want ErrKeyMissing", err)
		}
	})
	t.Run("single", func(t *testing.T) {
		h := http.Header{}
		h.Set(httpidem.HeaderIdempotencyKey, "k1")
		key, err := httpidem.KeyFromHeader(h)
		if err != nil || key != "k1" {
			t.Fatalf("KeyFromHeader = (%q, %v), want (\"k1\", nil)", key, err)
		}
	})
	t.Run("multiple headers rejected", func(t *testing.T) {
		h := http.Header{}
		h.Add(httpidem.HeaderIdempotencyKey, "k1")
		h.Add(httpidem.HeaderIdempotencyKey, "k2")
		_, err := httpidem.KeyFromHeader(h)
		if !errors.Is(err, httpidem.ErrKeyInvalid) {
			t.Fatalf("err = %v, want ErrKeyInvalid", err)
		}
	})
}
