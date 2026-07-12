package httpidem

import (
	"crypto/sha256"
	"io"
	"net/url"
	"strings"
)

// Fingerprint computes the request fingerprint (REQUIREMENTS §4.2):
//
//	SHA-256( UPPER(method) + "\n" + URL.EscapedPath() + "?" + URL.RawQuery + "\n" + body )
//
// The query is always included and the "?" separator is present even
// when RawQuery is empty. Host, headers, and Content-Type are not part
// of the fingerprint. body must be the complete request body (an empty
// body hashes the prefix only).
//
// The function is exported for framework adapters (§2.3), which must
// use it to stay fingerprint-compatible with the middleware.
func Fingerprint(method string, u *url.URL, body []byte) []byte {
	h := sha256.New()
	io.WriteString(h, strings.ToUpper(method))
	io.WriteString(h, "\n")
	io.WriteString(h, u.EscapedPath())
	io.WriteString(h, "?")
	io.WriteString(h, u.RawQuery)
	io.WriteString(h, "\n")
	h.Write(body)
	return h.Sum(nil)
}
