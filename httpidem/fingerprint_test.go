package httpidem_test

import (
	"bytes"
	"net/url"
	"testing"

	"github.com/repenguin22/idemlease/httpidem"
)

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// TestFingerprint covers §4.2 acceptance (a) and (b): identical
// requests agree, and any change in method, query, path, or body
// disagrees.
func TestFingerprint(t *testing.T) {
	base := httpidem.Fingerprint("POST", mustURL(t, "/orders?a=1"), []byte(`{"amount":42}`))

	same := httpidem.Fingerprint("POST", mustURL(t, "/orders?a=1"), []byte(`{"amount":42}`))
	if !bytes.Equal(base, same) {
		t.Error("identical requests must produce identical fingerprints")
	}
	if got := httpidem.Fingerprint("post", mustURL(t, "/orders?a=1"), []byte(`{"amount":42}`)); !bytes.Equal(base, got) {
		t.Error("method comparison must be case-insensitive (ToUpper)")
	}

	diffs := map[string][]byte{
		"different method": httpidem.Fingerprint("PATCH", mustURL(t, "/orders?a=1"), []byte(`{"amount":42}`)),
		"different query":  httpidem.Fingerprint("POST", mustURL(t, "/orders?a=2"), []byte(`{"amount":42}`)),
		"missing query":    httpidem.Fingerprint("POST", mustURL(t, "/orders"), []byte(`{"amount":42}`)),
		"different path":   httpidem.Fingerprint("POST", mustURL(t, "/payments?a=1"), []byte(`{"amount":42}`)),
		"different body":   httpidem.Fingerprint("POST", mustURL(t, "/orders?a=1"), []byte(`{"amount":43}`)),
		"empty body":       httpidem.Fingerprint("POST", mustURL(t, "/orders?a=1"), nil),
	}
	for name, fp := range diffs {
		if bytes.Equal(base, fp) {
			t.Errorf("%s must change the fingerprint", name)
		}
	}

	if got := len(base); got != 32 {
		t.Errorf("fingerprint length = %d, want 32 (SHA-256)", got)
	}
}
