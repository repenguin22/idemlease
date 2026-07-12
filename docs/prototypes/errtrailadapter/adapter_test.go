package errtrailadapter_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/repenguin22/errtrail"
	"github.com/repenguin22/idemlease"
	errtrailadapter "github.com/repenguin22/idemlease/docs/prototypes/errtrailadapter"
	"github.com/repenguin22/idemlease/httpidem"
	"github.com/repenguin22/idemlease/memstore"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func do(h http.Handler, key, body string) *httptest.ResponseRecorder {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest("POST", "/x", rd)
	if key != "" {
		req.Header.Set(httpidem.HeaderIdempotencyKey, key)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// TestErrorWriterMapsSentinels drives the middleware rejections through
// the adapter's ErrorWriter and checks the problem responses.
func TestErrorWriterMapsSentinels(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})

	t.Run("missing key → 400 with public message", func(t *testing.T) {
		h := httpidem.New(memstore.New(),
			httpidem.Require(true), httpidem.Errors(errtrailadapter.Errors()), httpidem.Logger(quietLogger()),
		)(okHandler)
		rr := do(h, "", "b")
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rr.Code)
		}
		if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
			t.Fatalf("Content-Type = %q, want problem+json", ct)
		}
		if !strings.Contains(rr.Body.String(), "Idempotency-Key header is required") {
			t.Fatalf("body = %s, want the registered public message", rr.Body.String())
		}
	})
	t.Run("in-flight → 409", func(t *testing.T) {
		store := memstore.New()
		if _, err := store.Reserve(context.Background(), idemlease.Record{
			Key: "k", State: idemlease.StateReserved, Token: "t",
			LeaseExpiresAt: time.Now().Add(time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
		h := httpidem.New(store, httpidem.Errors(errtrailadapter.Errors()), httpidem.Logger(quietLogger()))(okHandler)
		rr := do(h, "k", "b")
		if rr.Code != http.StatusConflict {
			t.Fatalf("status = %d, want 409", rr.Code)
		}
		if rr.Header().Get("Retry-After") == "" {
			t.Fatal("the middleware's Retry-After must survive a custom ErrorWriter")
		}
		if !strings.Contains(rr.Body.String(), "being processed") {
			t.Fatalf("body = %s, want the in-flight public message", rr.Body.String())
		}
	})
	t.Run("payload mismatch → 422", func(t *testing.T) {
		h := httpidem.New(memstore.New(), httpidem.Errors(errtrailadapter.Errors()), httpidem.Logger(quietLogger()))(okHandler)
		do(h, "k", "body-1")
		rr := do(h, "k", "body-2")
		if rr.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want 422", rr.Code)
		}
		if !strings.Contains(rr.Body.String(), "different request") {
			t.Fatalf("body = %s, want the mismatch public message", rr.Body.String())
		}
	})
	t.Run("body too large → 413", func(t *testing.T) {
		h := httpidem.New(memstore.New(),
			httpidem.MaxRequestBody(4), httpidem.Errors(errtrailadapter.Errors()), httpidem.Logger(quietLogger()),
		)(okHandler)
		if rr := do(h, "k", "0123456789"); rr.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413", rr.Code)
		}
	})
}

// TestPolicy verifies the Code-driven replay decisions and the
// status-driven fallback for non-errtrail errors.
func TestPolicy(t *testing.T) {
	run := func(t *testing.T, handlerErr error) int32 {
		t.Helper()
		var count atomic.Int32
		h := httpidem.New(memstore.New(),
			httpidem.Policy(errtrailadapter.Policy()), httpidem.Logger(quietLogger()),
		)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			if handlerErr != nil {
				httpidem.SetError(r.Context(), handlerErr)
			}
			w.WriteHeader(http.StatusOK)
		}))
		do(h, "k", "b")
		do(h, "k", "b")
		return count.Load()
	}

	t.Run("retryable code → Discard (re-executes)", func(t *testing.T) {
		if got := run(t, errtrail.New(errtrail.Unavailable, "dependency down")); got != 2 {
			t.Fatalf("handler ran %d times, want 2", got)
		}
	})
	t.Run("non-retryable code → Persist (replays)", func(t *testing.T) {
		if got := run(t, errtrail.New(errtrail.InvalidArgument, "bad input")); got != 1 {
			t.Fatalf("handler ran %d times, want 1", got)
		}
	})
	t.Run("non-errtrail error → status-driven fallback (200 persists)", func(t *testing.T) {
		if got := run(t, errors.New("plain error")); got != 1 {
			t.Fatalf("handler ran %d times, want 1", got)
		}
	})
	t.Run("no SetError → status-driven default", func(t *testing.T) {
		if got := run(t, nil); got != 1 {
			t.Fatalf("handler ran %d times, want 1", got)
		}
	})
}
