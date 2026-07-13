package httpidem_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/repenguin22/idemlease"
	"github.com/repenguin22/idemlease/httpidem"
	"github.com/repenguin22/idemlease/memstore"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// failingStore wraps a memstore with injectable failures and call
// counters, for pinning the §4.6 client observations.
type failingStore struct {
	inner idemlease.Store

	mu          sync.Mutex
	reserveErr  error
	completeErr error
	releaseErr  error

	reserveCalls  atomic.Int32
	completeCalls atomic.Int32
	releaseCalls  atomic.Int32
}

var _ idemlease.Store = (*failingStore)(nil)

func newFailingStore() *failingStore { return &failingStore{inner: memstore.New()} }

func (s *failingStore) setErrs(reserve, complete, release error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reserveErr, s.completeErr, s.releaseErr = reserve, complete, release
}

func (s *failingStore) errs() (reserve, complete, release error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reserveErr, s.completeErr, s.releaseErr
}

func (s *failingStore) Reserve(ctx context.Context, rec idemlease.Record) (*idemlease.Record, error) {
	s.reserveCalls.Add(1)
	if e, _, _ := s.errs(); e != nil {
		return nil, e
	}
	return s.inner.Reserve(ctx, rec)
}

func (s *failingStore) Complete(ctx context.Context, key, token string, payload []byte, recordTTL time.Duration) error {
	s.completeCalls.Add(1)
	if _, e, _ := s.errs(); e != nil {
		return e
	}
	return s.inner.Complete(ctx, key, token, payload, recordTTL)
}

func (s *failingStore) Release(ctx context.Context, key, token string) error {
	s.releaseCalls.Add(1)
	if _, _, e := s.errs(); e != nil {
		return e
	}
	return s.inner.Release(ctx, key, token)
}

func (s *failingStore) Get(ctx context.Context, key string) (*idemlease.Record, error) {
	return s.inner.Get(ctx, key)
}

func do(h http.Handler, method, target, key, body string, mutate ...func(*http.Request)) *httptest.ResponseRecorder {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rd)
	if key != "" {
		req.Header.Set(httpidem.HeaderIdempotencyKey, key)
	}
	for _, m := range mutate {
		m(req)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// TestMiddlewareLifecycle drives the happy path of the §4.3 behavior
// table: first execution, replay with header filtering, quoted-key
// equivalence, and the completed-fingerprint-mismatch 422 (§12-10).
func TestMiddlewareLifecycle(t *testing.T) {
	var count atomic.Int32
	var seenBody []byte
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		seenBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Location", "/orders/42")
		w.Header().Set("X-Request-Trace", "trace-1")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":42}`))
	})
	h := httpidem.New(memstore.New(), httpidem.Logger(quietLogger()))(handler)
	body := `{"amount":42}`

	first := do(h, "POST", "/orders?a=1", "k1", body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first request: status = %d, want 201", first.Code)
	}
	if string(seenBody) != body {
		t.Fatalf("handler saw body %q, want the full body %q (§4.2)", seenBody, body)
	}
	if first.Header().Get("X-Request-Trace") != "trace-1" {
		t.Error("the first response must pass through unmodified")
	}
	if first.Header().Get(httpidem.HeaderIdempotencyReplayed) != "" {
		t.Error("the first response must not be marked replayed")
	}

	replay := do(h, "POST", "/orders?a=1", "k1", body)
	if replay.Code != http.StatusCreated || replay.Body.String() != `{"id":42}` {
		t.Fatalf("replay = (%d, %q), want the stored 201 response", replay.Code, replay.Body.String())
	}
	if replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
		t.Error("replay must carry Idempotency-Replayed: true")
	}
	if replay.Header().Get("Content-Type") != "application/json" || replay.Header().Get("Location") != "/orders/42" {
		t.Error("allowlisted headers must be replayed")
	}
	if replay.Header().Get("X-Request-Trace") != "" {
		t.Error("non-allowlisted headers must not be replayed")
	}
	if count.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", count.Load())
	}

	quoted := do(h, "POST", "/orders?a=1", `"k1"`, body)
	if quoted.Code != http.StatusCreated || quoted.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
		t.Errorf(`quoted "k1" must replay the record stored under raw k1 (got %d)`, quoted.Code)
	}

	mismatch := do(h, "POST", "/orders?a=1", "k1", `{"amount":43}`)
	if mismatch.Code != http.StatusUnprocessableEntity {
		t.Fatalf("different payload on a completed key: status = %d, want 422", mismatch.Code)
	}
	if ct := mismatch.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("rejection Content-Type = %q, want application/problem+json", ct)
	}
	if count.Load() != 1 {
		t.Fatalf("handler ran %d times, want still 1", count.Load())
	}
}

// TestGatekeeping covers pass-through and 400 rejections ahead of Begin.
func TestGatekeeping(t *testing.T) {
	newH := func(store idemlease.Store, count *atomic.Int32, opts ...httpidem.Option) http.Handler {
		opts = append(opts, httpidem.Logger(quietLogger()))
		return httpidem.New(store, opts...)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
	}

	t.Run("non-target method passes through", func(t *testing.T) {
		fs := newFailingStore()
		var count atomic.Int32
		rr := do(newH(fs, &count), "GET", "/x", "k", "")
		if rr.Code != 200 || count.Load() != 1 || fs.reserveCalls.Load() != 0 {
			t.Fatalf("GET must bypass idempotency entirely (code=%d handler=%d reserve=%d)",
				rr.Code, count.Load(), fs.reserveCalls.Load())
		}
	})
	t.Run("missing key passes through by default", func(t *testing.T) {
		fs := newFailingStore()
		var count atomic.Int32
		rr := do(newH(fs, &count), "POST", "/x", "", "b")
		if rr.Code != 200 || count.Load() != 1 || fs.reserveCalls.Load() != 0 {
			t.Fatalf("keyless POST must pass through with Require(false) (code=%d handler=%d reserve=%d)",
				rr.Code, count.Load(), fs.reserveCalls.Load())
		}
	})
	t.Run("missing key with Require is 400", func(t *testing.T) {
		var count atomic.Int32
		rr := do(newH(newFailingStore(), &count, httpidem.Require(true)), "POST", "/x", "", "b")
		if rr.Code != 400 || count.Load() != 0 {
			t.Fatalf("code = %d handler = %d, want 400 and no execution", rr.Code, count.Load())
		}
	})
	t.Run("invalid key is 400", func(t *testing.T) {
		var count atomic.Int32
		rr := do(newH(newFailingStore(), &count), "POST", "/x", "bad\x1fkey", "b")
		if rr.Code != 400 || count.Load() != 0 {
			t.Fatalf("code = %d handler = %d, want 400 and no execution", rr.Code, count.Load())
		}
	})
	t.Run("multiple key headers are 400", func(t *testing.T) {
		var count atomic.Int32
		rr := do(newH(newFailingStore(), &count), "POST", "/x", "k1", "b", func(r *http.Request) {
			r.Header.Add(httpidem.HeaderIdempotencyKey, "k2")
		})
		if rr.Code != 400 || count.Load() != 0 {
			t.Fatalf("code = %d handler = %d, want 400 and no execution", rr.Code, count.Load())
		}
	})
	t.Run("KeyValidator rejection is 400", func(t *testing.T) {
		var count atomic.Int32
		isUUID := func(k string) bool { return len(k) == 36 }
		rr := do(newH(newFailingStore(), &count, httpidem.KeyValidator(isUUID)), "POST", "/x", "short", "b")
		if rr.Code != 400 || count.Load() != 0 {
			t.Fatalf("code = %d handler = %d, want 400 and no execution", rr.Code, count.Load())
		}
	})
}

// TestInFlight409 pins the reserved-state behavior: concurrent same-key
// requests get 409 with Retry-After, regardless of fingerprint (§12-10).
func TestInFlight409(t *testing.T) {
	var count atomic.Int32
	var once sync.Once
	entered := make(chan struct{})
	release := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		once.Do(func() { close(entered) })
		<-release
		w.WriteHeader(http.StatusCreated)
	})
	h := httpidem.New(memstore.New(), httpidem.Logger(quietLogger()))(handler)

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- do(h, "POST", "/x", "k", "body") }()
	<-entered

	same := do(h, "POST", "/x", "k", "body")
	if same.Code != http.StatusConflict {
		t.Fatalf("in-flight same fingerprint: status = %d, want 409", same.Code)
	}
	if ra, err := strconv.Atoi(same.Header().Get("Retry-After")); err != nil || ra < 1 {
		t.Fatalf("Retry-After = %q, want an integer >= 1", same.Header().Get("Retry-After"))
	}
	diff := do(h, "POST", "/x", "k", "OTHER-BODY")
	if diff.Code != http.StatusConflict {
		t.Fatalf("in-flight different fingerprint: status = %d, want 409 (decision 7)", diff.Code)
	}

	close(release)
	if first := <-done; first.Code != http.StatusCreated {
		t.Fatalf("first request: status = %d, want 201", first.Code)
	}
	replay := do(h, "POST", "/x", "k", "body")
	if replay.Code != http.StatusCreated || replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
		t.Fatalf("after completion the key must replay (got %d)", replay.Code)
	}
	if count.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", count.Load())
	}
}

// TestRetryAfterCeiling checks §4.4: remaining lease is rounded up to
// whole seconds with a minimum of 1.
func TestRetryAfterCeiling(t *testing.T) {
	tests := []struct {
		name      string
		remaining time.Duration
		want      string
	}{
		{"1.5s rounds up to 2", 1500 * time.Millisecond, "2"},
		{"sub-second clamps to 1", 300 * time.Millisecond, "1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := memstore.New()
			_, err := store.Reserve(context.Background(), idemlease.Record{
				Key: "k", State: idemlease.StateReserved, Token: "t",
				LeaseExpiresAt: time.Now().Add(tt.remaining),
			})
			if err != nil {
				t.Fatal(err)
			}
			h := httpidem.New(store, httpidem.Logger(quietLogger()))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
			rr := do(h, "POST", "/x", "k", "")
			if rr.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409", rr.Code)
			}
			if got := rr.Header().Get("Retry-After"); got != tt.want {
				t.Fatalf("Retry-After = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestRequestBodyLimits covers §4.2 body handling: 413 by default and
// full pass-through (no fingerprint, no Begin) with BypassLargeBody.
func TestRequestBodyLimits(t *testing.T) {
	bigBody := strings.Repeat("x", 16)

	t.Run("413 by default", func(t *testing.T) {
		fs := newFailingStore()
		var count atomic.Int32
		h := httpidem.New(fs, httpidem.MaxRequestBody(8), httpidem.Logger(quietLogger()))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { count.Add(1) }))
		rr := do(h, "POST", "/x", "k", bigBody)
		if rr.Code != http.StatusRequestEntityTooLarge || count.Load() != 0 || fs.reserveCalls.Load() != 0 {
			t.Fatalf("code=%d handler=%d reserve=%d, want 413 and nothing else", rr.Code, count.Load(), fs.reserveCalls.Load())
		}
	})
	t.Run("bypass passes the full body through", func(t *testing.T) {
		fs := newFailingStore()
		var count atomic.Int32
		var seenBody []byte
		h := httpidem.New(fs, httpidem.MaxRequestBody(8), httpidem.BypassLargeBody(true), httpidem.Logger(quietLogger()))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				seenBody, _ = io.ReadAll(r.Body)
				w.WriteHeader(http.StatusCreated)
			}))
		rr := do(h, "POST", "/x", "k", bigBody)
		if rr.Code != http.StatusCreated || string(seenBody) != bigBody {
			t.Fatalf("bypassed handler must see the full body (code=%d body=%q)", rr.Code, seenBody)
		}
		if fs.reserveCalls.Load() != 0 {
			t.Fatal("bypass must not touch the store")
		}
		do(h, "POST", "/x", "k", bigBody)
		if count.Load() != 2 {
			t.Fatalf("bypassed requests must not be replayed (handler ran %d times)", count.Load())
		}
	})
}

// TestKeyScope pins §4.5 / §12-11: equal keys in different scopes are
// independent; the same scope replays normally.
func TestKeyScope(t *testing.T) {
	var count atomic.Int32
	h := httpidem.New(memstore.New(),
		httpidem.KeyScope(func(r *http.Request) string { return r.Header.Get("X-Tenant") }),
		httpidem.Logger(quietLogger()),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	tenant := func(id string) func(*http.Request) {
		return func(r *http.Request) { r.Header.Set("X-Tenant", id) }
	}

	if rr := do(h, "POST", "/x", "k", "same-body", tenant("A")); rr.Code != 201 {
		t.Fatalf("tenant A first request: %d", rr.Code)
	}
	if rr := do(h, "POST", "/x", "k", "same-body", tenant("B")); rr.Code != 201 || rr.Header().Get(httpidem.HeaderIdempotencyReplayed) != "" {
		t.Fatalf("tenant B with the same key must execute independently (code=%d)", rr.Code)
	}
	if count.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2 (one per scope)", count.Load())
	}
	if rr := do(h, "POST", "/x", "k", "same-body", tenant("A")); rr.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
		t.Fatal("tenant A retry must replay within its own scope")
	}
	if count.Load() != 2 {
		t.Fatalf("handler ran %d times, want still 2", count.Load())
	}
}

// TestStoreFailureOnBegin pins §4.6 row 1: fail-closed 503 by default,
// FailOpen passes through without idempotency.
func TestStoreFailureOnBegin(t *testing.T) {
	t.Run("fail-closed 503", func(t *testing.T) {
		fs := newFailingStore()
		fs.setErrs(errors.New("redis down: 10.0.0.5:6379"), nil, nil)
		var count atomic.Int32
		h := httpidem.New(fs, httpidem.Logger(quietLogger()))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { count.Add(1) }))
		rr := do(h, "POST", "/x", "k", "b")
		if rr.Code != http.StatusServiceUnavailable || count.Load() != 0 {
			t.Fatalf("code=%d handler=%d, want 503 and no execution", rr.Code, count.Load())
		}
		if strings.Contains(rr.Body.String(), "redis down") {
			t.Error("5xx problem detail must not leak infrastructure errors to clients")
		}
	})
	t.Run("fail-open passes through", func(t *testing.T) {
		fs := newFailingStore()
		fs.setErrs(errors.New("redis down"), nil, nil)
		var count atomic.Int32
		h := httpidem.New(fs, httpidem.FailOpen(true), httpidem.Logger(quietLogger()))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				w.WriteHeader(http.StatusCreated)
			}))
		if rr := do(h, "POST", "/x", "k", "b"); rr.Code != http.StatusCreated || count.Load() != 1 {
			t.Fatalf("fail-open must run the handler (code=%d handler=%d)", rr.Code, count.Load())
		}
		fs.setErrs(nil, nil, nil)
		do(h, "POST", "/x", "k", "b")
		if count.Load() != 2 {
			t.Fatalf("fail-open responses must not be stored (handler ran %d times)", count.Load())
		}
	})
}

// TestStoreFailureOnFinish pins §4.6 rows 2-3: the captured response is
// returned, Complete failure triggers a best-effort Release, and
// Release failure leaves the key reserved until lease expiry.
func TestStoreFailureOnFinish(t *testing.T) {
	newH := func(fs *failingStore, count *atomic.Int32, status int) http.Handler {
		return httpidem.New(fs, httpidem.Logger(quietLogger()))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				w.WriteHeader(status)
				_, _ = w.Write([]byte("work-done"))
			}))
	}

	t.Run("Complete fails, best-effort Release succeeds", func(t *testing.T) {
		fs := newFailingStore()
		fs.setErrs(nil, errors.New("boom"), nil)
		var count atomic.Int32
		h := newH(fs, &count, http.StatusCreated)

		rr := do(h, "POST", "/x", "k", "b")
		if rr.Code != http.StatusCreated || rr.Body.String() != "work-done" {
			t.Fatalf("the captured response must still reach the client (code=%d body=%q)", rr.Code, rr.Body.String())
		}
		if fs.releaseCalls.Load() < 1 {
			t.Fatal("a failed Complete must trigger a best-effort Release")
		}
		fs.setErrs(nil, nil, nil)
		do(h, "POST", "/x", "k", "b")
		if count.Load() != 2 {
			t.Fatalf("after a successful Release the key must be re-executable (handler ran %d times)", count.Load())
		}
	})
	t.Run("Complete and Release both fail: reserved until lease expiry", func(t *testing.T) {
		fs := newFailingStore()
		fs.setErrs(nil, errors.New("boom"), errors.New("boom2"))
		var count atomic.Int32
		h := newH(fs, &count, http.StatusCreated)

		if rr := do(h, "POST", "/x", "k", "b"); rr.Code != http.StatusCreated {
			t.Fatalf("code = %d, want 201", rr.Code)
		}
		if rr := do(h, "POST", "/x", "k", "b"); rr.Code != http.StatusConflict {
			t.Fatalf("retry while still reserved: code = %d, want 409", rr.Code)
		}
	})
	t.Run("Release fails after Discard: reserved until lease expiry", func(t *testing.T) {
		fs := newFailingStore()
		fs.setErrs(nil, nil, errors.New("boom"))
		var count atomic.Int32
		h := newH(fs, &count, http.StatusInternalServerError) // 500 → Discard

		if rr := do(h, "POST", "/x", "k", "b"); rr.Code != http.StatusInternalServerError {
			t.Fatalf("code = %d, want the handler's 500", rr.Code)
		}
		if rr := do(h, "POST", "/x", "k", "b"); rr.Code != http.StatusConflict {
			t.Fatalf("retry while still reserved: code = %d, want 409", rr.Code)
		}
	})
}

// TestLeaseLost pins §3.3 at the HTTP level: the response is returned,
// a lease_lost warning is logged, and nothing is stored for replay.
func TestLeaseLost(t *testing.T) {
	run := func(t *testing.T, key string, opts ...httpidem.Option) *bytes.Buffer {
		t.Helper()
		fs := newFailingStore()
		fs.setErrs(nil, idemlease.ErrTokenMismatch, nil) // Finish normalizes to leaseLost
		var buf bytes.Buffer
		opts = append(opts, httpidem.Logger(slog.New(slog.NewTextHandler(&buf, nil))))
		h := httpidem.New(fs, opts...)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte("work-done"))
		}))

		rr := do(h, "POST", "/x", key, "b")
		if rr.Code != http.StatusCreated || rr.Body.String() != "work-done" {
			t.Fatalf("the finished work must be returned to the client (code=%d)", rr.Code)
		}
		if fs.releaseCalls.Load() != 0 {
			t.Error("lease loss is not a store failure; no best-effort Release expected")
		}
		if rr := do(h, "POST", "/x", key, "b"); rr.Header().Get(httpidem.HeaderIdempotencyReplayed) == "true" {
			t.Error("a lease-lost response must not be replayed")
		}
		return &buf
	}

	t.Run("logs lease_lost", func(t *testing.T) {
		buf := run(t, "k")
		if !strings.Contains(buf.String(), "lease_lost") || !strings.Contains(buf.String(), "idempotency_key=k") {
			t.Fatalf("log must carry event=lease_lost with the key, got:\n%s", buf.String())
		}
	})
	t.Run("HashKeysInLogs hides the raw key", func(t *testing.T) {
		buf := run(t, "secret-key-value", httpidem.HashKeysInLogs(true))
		if strings.Contains(buf.String(), "secret-key-value") {
			t.Fatalf("raw key leaked into logs:\n%s", buf.String())
		}
		if !strings.Contains(buf.String(), "sha256:") {
			t.Fatalf("hashed key missing from logs:\n%s", buf.String())
		}
	})
}

// TestReplayPolicy pins §5.1 through the middleware: 4xx persists,
// 429/5xx discard, and SetError feeds custom policies.
func TestReplayPolicy(t *testing.T) {
	newCounting := func(status int, opts ...httpidem.Option) (http.Handler, *atomic.Int32) {
		var count atomic.Int32
		opts = append(opts, httpidem.Logger(quietLogger()))
		h := httpidem.New(memstore.New(), opts...)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			w.WriteHeader(status)
		}))
		return h, &count
	}

	t.Run("404 is persisted and replayed", func(t *testing.T) {
		h, count := newCounting(http.StatusNotFound)
		do(h, "POST", "/x", "k", "b")
		rr := do(h, "POST", "/x", "k", "b")
		if rr.Code != 404 || rr.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" || count.Load() != 1 {
			t.Fatalf("code=%d handler=%d, want a replayed 404 after 1 execution", rr.Code, count.Load())
		}
	})
	t.Run("429 is discarded", func(t *testing.T) {
		h, count := newCounting(http.StatusTooManyRequests)
		do(h, "POST", "/x", "k", "b")
		do(h, "POST", "/x", "k", "b")
		if count.Load() != 2 {
			t.Fatalf("handler ran %d times, want 2 (429 must not be replayed)", count.Load())
		}
	})
	t.Run("500 is discarded", func(t *testing.T) {
		h, count := newCounting(http.StatusInternalServerError)
		do(h, "POST", "/x", "k", "b")
		do(h, "POST", "/x", "k", "b")
		if count.Load() != 2 {
			t.Fatalf("handler ran %d times, want 2 (500 must not be replayed)", count.Load())
		}
	})
	t.Run("SetError reaches a custom policy", func(t *testing.T) {
		var count atomic.Int32
		policy := httpidem.ReplayPolicyFunc(func(status int, err error) idemlease.Decision {
			if err != nil {
				return idemlease.Discard
			}
			return httpidem.DefaultPolicy.Decide(status, nil)
		})
		h := httpidem.New(memstore.New(), httpidem.Policy(policy), httpidem.Logger(quietLogger()))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				httpidem.SetError(r.Context(), errors.New("transient business failure"))
				w.WriteHeader(http.StatusOK)
			}))
		do(h, "POST", "/x", "k", "b")
		do(h, "POST", "/x", "k", "b")
		if count.Load() != 2 {
			t.Fatalf("handler ran %d times, want 2 (SetError should force Discard)", count.Load())
		}
	})
}

// TestPanicDiscardsAndRepanics pins §5.1: the reservation is released
// and the panic propagates for outer middleware to handle.
func TestPanicDiscardsAndRepanics(t *testing.T) {
	var count atomic.Int32
	h := httpidem.New(memstore.New(), httpidem.Logger(quietLogger()))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if count.Add(1) == 1 {
				panic("boom")
			}
			w.WriteHeader(http.StatusCreated)
		}))

	func() {
		defer func() {
			if p := recover(); p != "boom" {
				t.Fatalf("recovered %v, want the original panic value", p)
			}
		}()
		do(h, "POST", "/x", "k", "b")
		t.Fatal("panic must propagate out of the middleware")
	}()

	if rr := do(h, "POST", "/x", "k", "b"); rr.Code != http.StatusCreated {
		t.Fatalf("after a panic the key must be re-executable, got %d", rr.Code)
	}
	if count.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2", count.Load())
	}
}

// TestStreamingAndOversizeDiscard pins §6.1: flushed responses and
// responses over MaxResponseBody are delivered but never stored.
func TestStreamingAndOversizeDiscard(t *testing.T) {
	t.Run("Flush abandons capture", func(t *testing.T) {
		var count atomic.Int32
		h := httpidem.New(memstore.New(), httpidem.Logger(quietLogger()))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				_, _ = w.Write([]byte("chunk-1"))
				w.(http.Flusher).Flush()
				_, _ = w.Write([]byte("chunk-2"))
			}))
		rr := do(h, "POST", "/x", "k", "b")
		if rr.Body.String() != "chunk-1chunk-2" || !rr.Flushed {
			t.Fatalf("streamed response must reach the client (body=%q flushed=%v)", rr.Body.String(), rr.Flushed)
		}
		do(h, "POST", "/x", "k", "b")
		if count.Load() != 2 {
			t.Fatalf("handler ran %d times, want 2 (streamed responses are not stored)", count.Load())
		}
	})
	t.Run("response over MaxResponseBody is not stored", func(t *testing.T) {
		var count atomic.Int32
		big := strings.Repeat("y", 20)
		h := httpidem.New(memstore.New(), httpidem.MaxResponseBody(8), httpidem.Logger(quietLogger()))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				_, _ = w.Write([]byte(big))
			}))
		rr := do(h, "POST", "/x", "k", "b")
		if rr.Body.String() != big {
			t.Fatalf("oversized response must still reach the client in full (body=%q)", rr.Body.String())
		}
		do(h, "POST", "/x", "k", "b")
		if count.Load() != 2 {
			t.Fatalf("handler ran %d times, want 2 (oversized responses are not stored)", count.Load())
		}
	})
}

// TestImplicitOKPersisted pins §5.1: a handler that writes nothing is
// treated as 200 and its (header-only) response is replayable.
func TestImplicitOKPersisted(t *testing.T) {
	var count atomic.Int32
	h := httpidem.New(memstore.New(), httpidem.Logger(quietLogger()))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			w.Header().Set("Location", "/things/9")
		}))

	if rr := do(h, "POST", "/x", "k", "b"); rr.Code != 200 {
		t.Fatalf("first: code = %d, want implicit 200", rr.Code)
	}
	rr := do(h, "POST", "/x", "k", "b")
	if rr.Code != 200 || rr.Header().Get("Location") != "/things/9" || rr.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
		t.Fatalf("replay of implicit 200 must carry the captured headers (code=%d Location=%q)", rr.Code, rr.Header().Get("Location"))
	}
	if count.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", count.Load())
	}
}

// TestCompressedResponseReplaysIntact pins the fix for the v1.0.0
// review finding H1: a handler-written compressed body must replay with
// its Content-Encoding, and the replayed bytes must still decompress.
func TestCompressedResponseReplaysIntact(t *testing.T) {
	const plain = `{"id":42}`
	var gzBody bytes.Buffer
	zw := gzip.NewWriter(&gzBody)
	_, _ = zw.Write([]byte(plain))
	_ = zw.Close()

	h := httpidem.New(memstore.New(), httpidem.Logger(quietLogger()))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Encoding", "gzip")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write(gzBody.Bytes())
		}))

	do(h, "POST", "/x", "k", "b")
	replay := do(h, "POST", "/x", "k", "b")
	if replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
		t.Fatal("second request must be a replay")
	}
	if got := replay.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("replayed Content-Encoding = %q, want gzip (default allowlist)", got)
	}
	if !bytes.Equal(replay.Body.Bytes(), gzBody.Bytes()) {
		t.Fatal("replayed body must be the stored compressed bytes")
	}
	zr, err := gzip.NewReader(bytes.NewReader(replay.Body.Bytes()))
	if err != nil {
		t.Fatalf("replayed body is not valid gzip: %v", err)
	}
	decoded, err := io.ReadAll(zr)
	if err != nil || string(decoded) != plain {
		t.Fatalf("decompressed replay = (%q, %v), want %q", decoded, err, plain)
	}
}

// TestLowercaseMethodIsProtected pins the fix for review finding N1:
// method matching is case-insensitive, like the Methods option.
func TestLowercaseMethodIsProtected(t *testing.T) {
	fs := newFailingStore()
	var count atomic.Int32
	h := httpidem.New(fs, httpidem.Logger(quietLogger()))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			w.WriteHeader(http.StatusCreated)
		}))
	if rr := do(h, "post", "/x", "k", "b"); rr.Code != http.StatusCreated {
		t.Fatalf("code = %d, want 201", rr.Code)
	}
	if fs.reserveCalls.Load() != 1 {
		t.Fatalf("Reserve called %d times, want 1 (lowercase method must be processed)", fs.reserveCalls.Load())
	}
	rr := do(h, "post", "/x", "k", "b")
	if rr.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" || count.Load() != 1 {
		t.Fatalf("retry = (replayed=%q, handler=%d), want a replay after 1 execution",
			rr.Header().Get(httpidem.HeaderIdempotencyReplayed), count.Load())
	}
}

// TestReplayRefusesOversizedPayload pins the fix for review finding M3:
// a stored payload beyond the configured limits (foreign data) must be
// refused instead of allocated and served.
func TestReplayRefusesOversizedPayload(t *testing.T) {
	store := memstore.New()
	ctx := context.Background()
	body := "b"
	fp := httpidem.Fingerprint("POST", mustURL(t, "/x"), []byte(body))
	if _, err := store.Reserve(ctx, idemlease.Record{
		Key: "k", State: idemlease.StateReserved, Token: "t",
		Fingerprint: fp, LeaseExpiresAt: time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, "k", "t", make([]byte, 2<<20), time.Hour); err != nil {
		t.Fatal(err)
	}

	h := httpidem.New(store, httpidem.Logger(quietLogger()))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("handler must not run on the replay path")
		}))
	rr := do(h, "POST", "/x", "k", body)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("code = %d, want 500 for an oversized stored payload", rr.Code)
	}
}

// TestHandlerFinishedItself pins the transactional-join handshake
// (§9.3, design matrix T1 at the middleware level): a handler that
// completes the record itself and calls MarkFinished must not trigger
// the middleware's own Finish, and the handler-stored payload is what
// replays.
func TestHandlerFinishedItself(t *testing.T) {
	store := memstore.New()
	var count atomic.Int32
	var logBuf bytes.Buffer
	sr := httpidem.StoredResponse{
		StatusCode: http.StatusCreated,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       []byte(`{"joined":true}`),
	}
	h := httpidem.New(store, httpidem.Logger(slog.New(slog.NewTextHandler(&logBuf, nil))))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			rsv, ok := httpidem.ReservationFromContext(r.Context())
			if !ok {
				t.Error("ReservationFromContext must succeed under the middleware")
				return
			}
			payload, err := sr.MarshalBinary()
			if err != nil {
				t.Error(err)
				return
			}
			// Stand-in for pgstore.CompleteTx + tx.Commit: complete the
			// record directly, then hand off.
			if err := store.Complete(r.Context(), rsv.Key, rsv.Token, payload, rsv.Options.RecordTTL); err != nil {
				t.Errorf("Complete: %v", err)
				return
			}
			httpidem.MarkFinished(r.Context())
			_ = httpidem.WriteStored(w, sr)
		}))

	first := do(h, "POST", "/x", "k", "b")
	if first.Code != http.StatusCreated || first.Body.String() != `{"joined":true}` {
		t.Fatalf("first = (%d, %q), want the handler's stored response", first.Code, first.Body.String())
	}
	if strings.Contains(logBuf.String(), "lease_lost") {
		t.Fatalf("MarkFinished must suppress the middleware Finish; log:\n%s", logBuf.String())
	}

	replay := do(h, "POST", "/x", "k", "b")
	if replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" || replay.Body.String() != `{"joined":true}` {
		t.Fatalf("replay = (%q, replayed=%q), want the handler-persisted response",
			replay.Body.String(), replay.Header().Get(httpidem.HeaderIdempotencyReplayed))
	}
	if replay.Header().Get("Content-Type") != "application/json" || count.Load() != 1 {
		t.Fatalf("replay Content-Type=%q handler=%d, want stored header and a single execution",
			replay.Header().Get("Content-Type"), count.Load())
	}
}

// TestMarkFinishedForgotten pins design matrix T7 at the middleware
// level: a handler that completed the record but forgot MarkFinished
// causes exactly one spurious lease_lost warning, and the record stays
// intact and replayable.
func TestMarkFinishedForgotten(t *testing.T) {
	store := memstore.New()
	var count atomic.Int32
	var logBuf bytes.Buffer
	sr := httpidem.StoredResponse{StatusCode: 201, Header: http.Header{}, Body: []byte("joined")}
	h := httpidem.New(store, httpidem.Logger(slog.New(slog.NewTextHandler(&logBuf, nil))))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			rsv, _ := httpidem.ReservationFromContext(r.Context())
			payload, _ := sr.MarshalBinary()
			_ = store.Complete(r.Context(), rsv.Key, rsv.Token, payload, rsv.Options.RecordTTL)
			// MarkFinished forgotten (handler bug).
			_ = httpidem.WriteStored(w, sr)
		}))

	do(h, "POST", "/x", "k", "b")
	if !strings.Contains(logBuf.String(), "lease_lost") {
		t.Fatalf("want the spurious lease_lost warning (T7), log:\n%s", logBuf.String())
	}
	replay := do(h, "POST", "/x", "k", "b")
	if replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" || replay.Body.String() != "joined" || count.Load() != 1 {
		t.Fatalf("the record must survive the forgotten MarkFinished (replayed=%q body=%q handler=%d)",
			replay.Header().Get(httpidem.HeaderIdempotencyReplayed), replay.Body.String(), count.Load())
	}
}

// TestReservationOutsideMiddleware pins the ok=false path.
func TestReservationOutsideMiddleware(t *testing.T) {
	if _, ok := httpidem.ReservationFromContext(context.Background()); ok {
		t.Fatal("ReservationFromContext must report ok=false outside the middleware")
	}
	httpidem.MarkFinished(context.Background()) // must be a safe no-op
}

// TestSetCookieNeverReplayed is the §4.4 / §12-5 acceptance test: even
// when explicitly allowlisted, Set-Cookie never appears in a replay.
func TestSetCookieNeverReplayed(t *testing.T) {
	h := httpidem.New(memstore.New(),
		httpidem.StoreHeaders("Set-Cookie", "Content-Type"),
		httpidem.Logger(quietLogger()),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Set-Cookie", "sid=secret")
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok"))
	}))

	first := do(h, "POST", "/x", "k", "b")
	if first.Header().Get("Set-Cookie") != "sid=secret" {
		t.Fatal("the live response must keep its Set-Cookie")
	}
	replay := do(h, "POST", "/x", "k", "b")
	if replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
		t.Fatal("second request must be a replay")
	}
	if replay.Header().Get("Set-Cookie") != "" {
		t.Fatal("Set-Cookie must never be replayed, even when allowlisted (§4.4)")
	}
	if replay.Header().Get("Content-Type") != "text/plain" {
		t.Fatal("allowlisted Content-Type must be replayed")
	}
}
