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
	"github.com/repenguin22/idemlease/errtrailadapter"
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

func okHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusCreated) }
}

// TestErrorWriterMapsSentinels drives the middleware rejections through
// the adapter's ErrorWriter and checks the problem responses. Options()
// is used here to also exercise the bundle helper.
func TestErrorWriterMapsSentinels(t *testing.T) {
	t.Run("missing key -> 400 with public message", func(t *testing.T) {
		opts := append(errtrailadapter.Options(), httpidem.Require(true), httpidem.Logger(quietLogger()))
		h := httpidem.New(memstore.New(), opts...)(okHandler())
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
	t.Run("in-flight -> 409 with Retry-After", func(t *testing.T) {
		store := memstore.New()
		if _, err := store.Reserve(context.Background(), idemlease.Record{
			Key: "k", State: idemlease.StateReserved, Token: "t",
			LeaseExpiresAt: time.Now().Add(time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
		h := httpidem.New(store, httpidem.Errors(errtrailadapter.Errors()), httpidem.Logger(quietLogger()))(okHandler())
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
	t.Run("payload mismatch -> 422", func(t *testing.T) {
		h := httpidem.New(memstore.New(), httpidem.Errors(errtrailadapter.Errors()), httpidem.Logger(quietLogger()))(okHandler())
		do(h, "k", "body-1")
		rr := do(h, "k", "body-2")
		if rr.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want 422", rr.Code)
		}
		if !strings.Contains(rr.Body.String(), "different request") {
			t.Fatalf("body = %s, want the mismatch public message", rr.Body.String())
		}
	})
	t.Run("body too large -> 413", func(t *testing.T) {
		h := httpidem.New(memstore.New(),
			httpidem.MaxRequestBody(4), httpidem.Errors(errtrailadapter.Errors()), httpidem.Logger(quietLogger()),
		)(okHandler())
		if rr := do(h, "k", "0123456789"); rr.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413", rr.Code)
		}
	})
	t.Run("store unavailable -> 503 without leaking detail", func(t *testing.T) {
		h := httpidem.New(unavailableStore{},
			httpidem.Errors(errtrailadapter.Errors()), httpidem.Logger(quietLogger()),
		)(okHandler())
		rr := do(h, "k", "b")
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rr.Code)
		}
		if strings.Contains(rr.Body.String(), "secret-dsn") {
			t.Fatalf("internal detail leaked to the client: %s", rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "temporarily unavailable") {
			t.Fatalf("body = %s, want the store-unavailable public message", rr.Body.String())
		}
	})
}

// TestErrorWriterStatusesMatchCodes guards the invariant that every
// mapped code's registered HTTP status equals the status the middleware
// intended (problem.Write derives status from the code, ignoring the
// passed-in status).
func TestErrorWriterStatusesMatchCodes(t *testing.T) {
	cases := []struct {
		code errtrail.Code
		want int
	}{
		{errtrailadapter.CodeKeyMissing, http.StatusBadRequest},
		{errtrailadapter.CodeKeyInvalid, http.StatusBadRequest},
		{errtrailadapter.CodeInFlight, http.StatusConflict},
		{errtrailadapter.CodePayloadMismatch, http.StatusUnprocessableEntity},
		{errtrailadapter.CodeBodyTooLarge, http.StatusRequestEntityTooLarge},
	}
	for _, c := range cases {
		if got := c.code.HTTPStatus(); got != c.want {
			t.Errorf("code %v HTTPStatus = %d, want %d", c.code, got, c.want)
		}
	}
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

	t.Run("retryable code -> Discard (re-executes)", func(t *testing.T) {
		if got := run(t, errtrail.New(errtrail.Unavailable, "dependency down")); got != 2 {
			t.Fatalf("handler ran %d times, want 2", got)
		}
	})
	t.Run("non-retryable code -> Persist (replays)", func(t *testing.T) {
		if got := run(t, errtrail.New(errtrail.InvalidArgument, "bad input")); got != 1 {
			t.Fatalf("handler ran %d times, want 1", got)
		}
	})
	t.Run("adapter in-flight code is retryable -> Discard", func(t *testing.T) {
		if got := run(t, errtrail.New(errtrailadapter.CodeInFlight, "dup")); got != 2 {
			t.Fatalf("handler ran %d times, want 2 (CodeInFlight is registered Retryable)", got)
		}
	})
	t.Run("non-errtrail error -> status-driven fallback (200 persists)", func(t *testing.T) {
		if got := run(t, errors.New("plain error")); got != 1 {
			t.Fatalf("handler ran %d times, want 1", got)
		}
	})
	t.Run("no SetError -> status-driven default", func(t *testing.T) {
		if got := run(t, nil); got != 1 {
			t.Fatalf("handler ran %d times, want 1", got)
		}
	})
}

// unavailableStore always fails Reserve, exercising the 503 path.
type unavailableStore struct{}

var _ idemlease.Store = unavailableStore{}

func (unavailableStore) Reserve(context.Context, idemlease.Record) (*idemlease.Record, error) {
	return nil, errors.New("dial tcp secret-dsn:5432: connection refused")
}
func (unavailableStore) Complete(context.Context, string, string, []byte, time.Duration) error {
	return errors.New("unavailable")
}
func (unavailableStore) Release(context.Context, string, string) error {
	return errors.New("unavailable")
}
func (unavailableStore) Get(context.Context, string) (*idemlease.Record, error) {
	return nil, errors.New("unavailable")
}
