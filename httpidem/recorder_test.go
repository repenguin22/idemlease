package httpidem_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/repenguin22/idemlease/httpidem"
)

func TestRecorderImplicit200(t *testing.T) {
	rec := httpidem.NewRecorder(100, nil)
	if got := rec.Status(); got != 200 {
		t.Fatalf("Status = %d, want implicit 200", got)
	}
	sr, ok := rec.StoredResponse()
	if !ok || sr.StatusCode != 200 || len(sr.Body) != 0 {
		t.Fatalf("StoredResponse = (%+v, %v), want empty 200", sr, ok)
	}
}

func TestRecorderHeaderFiltering(t *testing.T) {
	// Set-Cookie is requested in the allowlist but must still be
	// excluded (§4.4: always-excluded headers cannot be re-enabled).
	rec := httpidem.NewRecorder(100, []string{"content-type", "Location", "Set-Cookie", "Transfer-Encoding"})
	h := http.Header{
		"Content-Type":      {"application/json"},
		"Location":          {"/orders/42"},
		"Set-Cookie":        {"sid=secret"},
		"Transfer-Encoding": {"chunked"},
		"X-Other":           {"dropped: not allowlisted"},
	}
	rec.RecordStatus(201, h)
	rec.RecordBody([]byte("body"))

	sr, ok := rec.StoredResponse()
	if !ok {
		t.Fatal("StoredResponse not ok")
	}
	want := http.Header{
		"Content-Type": {"application/json"},
		"Location":     {"/orders/42"},
	}
	if diff := cmp.Diff(want, sr.Header); diff != "" {
		t.Fatalf("captured headers mismatch (-want +got):\n%s", diff)
	}
}

func TestRecorderStatusSemantics(t *testing.T) {
	rec := httpidem.NewRecorder(100, nil)
	rec.RecordStatus(100, http.Header{}) // informational: ignored
	rec.RecordStatus(201, http.Header{})
	rec.RecordStatus(500, http.Header{}) // second real status: ignored
	if got := rec.Status(); got != 201 {
		t.Fatalf("Status = %d, want 201 (first non-1xx wins)", got)
	}
}

func TestRecorderBodyOverflow(t *testing.T) {
	rec := httpidem.NewRecorder(10, nil)
	rec.RecordBody([]byte(strings.Repeat("a", 6)))
	rec.RecordBody([]byte(strings.Repeat("b", 6)))
	if !rec.Overflowed() {
		t.Fatal("Overflowed = false, want true")
	}
	if _, ok := rec.StoredResponse(); ok {
		t.Fatal("StoredResponse ok = true, want false after overflow")
	}
}

func TestRecorderAbandon(t *testing.T) {
	rec := httpidem.NewRecorder(100, nil)
	rec.RecordStatus(200, http.Header{})
	rec.RecordBody([]byte("streamed"))
	rec.Abandon()
	if !rec.Abandoned() {
		t.Fatal("Abandoned = false, want true")
	}
	if _, ok := rec.StoredResponse(); ok {
		t.Fatal("StoredResponse ok = true, want false after Abandon")
	}
}
