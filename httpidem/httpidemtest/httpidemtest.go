// Package httpidemtest provides an HTTP-level conformance suite for
// idemlease.Store implementations driven through the httpidem
// middleware. It pins the client-observable scenarios of the contract:
// replay, in-flight 409 (same and different fingerprint),
// completed-fingerprint 422, KeyScope isolation, the three store
// failure modes (§4.6), lease expiry racing a surviving execution
// (§3.3), and single execution under concurrency (§10).
//
// The suite builds its own middleware around stores produced by
// newStore. Expiry scenarios wait real time (well under a second), so
// the suite works against out-of-process stores such as Redis. Like
// idemleasetest, the package depends on the standard library only.
package httpidemtest

import (
	"bytes"
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
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
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

// flakyStore decorates a conforming store with injectable failures so
// the §4.6 client observations can be pinned against any store.
type flakyStore struct {
	inner idemlease.Store

	mu          sync.Mutex
	reserveErr  error
	completeErr error
	releaseErr  error
}

var _ idemlease.Store = (*flakyStore)(nil)

func (s *flakyStore) setErrs(reserve, complete, release error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reserveErr, s.completeErr, s.releaseErr = reserve, complete, release
}

func (s *flakyStore) errs() (reserve, complete, release error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reserveErr, s.completeErr, s.releaseErr
}

func (s *flakyStore) Reserve(ctx context.Context, rec idemlease.Record) (*idemlease.Record, error) {
	if e, _, _ := s.errs(); e != nil {
		return nil, e
	}
	return s.inner.Reserve(ctx, rec)
}

func (s *flakyStore) Complete(ctx context.Context, key, token string, payload []byte, recordTTL time.Duration) error {
	if _, e, _ := s.errs(); e != nil {
		return e
	}
	return s.inner.Complete(ctx, key, token, payload, recordTTL)
}

func (s *flakyStore) Release(ctx context.Context, key, token string) error {
	if _, _, e := s.errs(); e != nil {
		return e
	}
	return s.inner.Release(ctx, key, token)
}

func (s *flakyStore) Get(ctx context.Context, key string) (*idemlease.Record, error) {
	return s.inner.Get(ctx, key)
}

// RunHTTPTests runs the HTTP scenario conformance suite against
// middleware built over stores produced by newStore. newStore is called
// once per subtest and must return an empty store ready for use.
func RunHTTPTests(t *testing.T, newStore func(t *testing.T) idemlease.Store) {
	t.Run("ReplayScenario", func(t *testing.T) {
		var count atomic.Int32
		h := httpidem.New(newStore(t), httpidem.Logger(quietLogger()))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"id":1}`))
			}))

		first := do(h, "POST", "/orders", "k", `{"n":1}`)
		if first.Code != http.StatusCreated || first.Header().Get(httpidem.HeaderIdempotencyReplayed) != "" {
			t.Fatalf("first request = %d (replayed=%q), want a fresh 201", first.Code, first.Header().Get(httpidem.HeaderIdempotencyReplayed))
		}
		replay := do(h, "POST", "/orders", "k", `{"n":1}`)
		if replay.Code != http.StatusCreated || replay.Body.String() != `{"id":1}` {
			t.Fatalf("replay = (%d, %q), want the stored 201 body", replay.Code, replay.Body.String())
		}
		if replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
			t.Fatal("replay must carry Idempotency-Replayed: true")
		}
		if replay.Header().Get("Content-Type") != "application/json" {
			t.Fatal("allowlisted headers must be replayed")
		}
		if count.Load() != 1 {
			t.Fatalf("handler ran %d times, want 1", count.Load())
		}
	})

	t.Run("CompletedFingerprintMismatch422", func(t *testing.T) {
		var count atomic.Int32
		h := httpidem.New(newStore(t), httpidem.Logger(quietLogger()))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				w.WriteHeader(http.StatusCreated)
			}))

		if rr := do(h, "POST", "/orders", "k", `{"n":1}`); rr.Code != http.StatusCreated {
			t.Fatalf("first request = %d, want 201", rr.Code)
		}
		if rr := do(h, "POST", "/orders", "k", `{"n":2}`); rr.Code != http.StatusUnprocessableEntity {
			t.Fatalf("same key with a different payload = %d, want 422 (§12-10)", rr.Code)
		}
		if count.Load() != 1 {
			t.Fatalf("handler ran %d times, want 1", count.Load())
		}
	})

	t.Run("InFlight409", func(t *testing.T) {
		var count atomic.Int32
		var once sync.Once
		entered := make(chan struct{})
		release := make(chan struct{})
		h := httpidem.New(newStore(t), httpidem.Logger(quietLogger()))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				once.Do(func() { close(entered) })
				<-release
				w.WriteHeader(http.StatusCreated)
			}))

		done := make(chan *httptest.ResponseRecorder, 1)
		go func() { done <- do(h, "POST", "/x", "k", "body") }()
		<-entered

		same := do(h, "POST", "/x", "k", "body")
		if same.Code != http.StatusConflict {
			t.Fatalf("in-flight same fingerprint = %d, want 409", same.Code)
		}
		if ra, err := strconv.Atoi(same.Header().Get("Retry-After")); err != nil || ra < 1 {
			t.Fatalf("Retry-After = %q, want an integer >= 1", same.Header().Get("Retry-After"))
		}
		if diff := do(h, "POST", "/x", "k", "OTHER-BODY"); diff.Code != http.StatusConflict {
			t.Fatalf("in-flight different fingerprint = %d, want 409 (§12-10, decision 7)", diff.Code)
		}

		close(release)
		if first := <-done; first.Code != http.StatusCreated {
			t.Fatalf("first request = %d, want 201", first.Code)
		}
		if replay := do(h, "POST", "/x", "k", "body"); replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
			t.Fatal("after completion the key must replay")
		}
		if count.Load() != 1 {
			t.Fatalf("handler ran %d times, want 1", count.Load())
		}
	})

	t.Run("KeyScopeIsolation", func(t *testing.T) {
		var count atomic.Int32
		h := httpidem.New(newStore(t),
			httpidem.KeyScope(func(r *http.Request) string { return r.Header.Get("X-Tenant") }),
			httpidem.Logger(quietLogger()),
		)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			w.WriteHeader(http.StatusCreated)
		}))
		tenant := func(id string) func(*http.Request) {
			return func(r *http.Request) { r.Header.Set("X-Tenant", id) }
		}

		if rr := do(h, "POST", "/x", "k", "same-body", tenant("A")); rr.Code != http.StatusCreated {
			t.Fatalf("tenant A first request = %d, want 201", rr.Code)
		}
		rr := do(h, "POST", "/x", "k", "same-body", tenant("B"))
		if rr.Code != http.StatusCreated || rr.Header().Get(httpidem.HeaderIdempotencyReplayed) != "" {
			t.Fatalf("tenant B must execute independently with the same key and fingerprint (§12-11), got %d", rr.Code)
		}
		if count.Load() != 2 {
			t.Fatalf("handler ran %d times, want 2 (one per scope)", count.Load())
		}
		if rr := do(h, "POST", "/x", "k", "same-body", tenant("A")); rr.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
			t.Fatal("within one scope the key must replay normally")
		}
		if count.Load() != 2 {
			t.Fatalf("handler ran %d times, want still 2", count.Load())
		}
	})

	t.Run("StoreFailureOnBegin", func(t *testing.T) {
		t.Run("FailClosed503", func(t *testing.T) {
			fs := &flakyStore{inner: newStore(t)}
			fs.setErrs(errors.New("store down"), nil, nil)
			var count atomic.Int32
			h := httpidem.New(fs, httpidem.Logger(quietLogger()))(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { count.Add(1) }))
			rr := do(h, "POST", "/x", "k", "b")
			if rr.Code != http.StatusServiceUnavailable || count.Load() != 0 {
				t.Fatalf("code=%d handler=%d, want 503 and no execution (§4.6 fail-closed)", rr.Code, count.Load())
			}
		})
		t.Run("FailOpenPassesThrough", func(t *testing.T) {
			fs := &flakyStore{inner: newStore(t)}
			fs.setErrs(errors.New("store down"), nil, nil)
			var count atomic.Int32
			h := httpidem.New(fs, httpidem.FailOpen(true), httpidem.Logger(quietLogger()))(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					count.Add(1)
					w.WriteHeader(http.StatusCreated)
				}))
			if rr := do(h, "POST", "/x", "k", "b"); rr.Code != http.StatusCreated || count.Load() != 1 {
				t.Fatalf("code=%d handler=%d, want the handler to run without idempotency", rr.Code, count.Load())
			}
			fs.setErrs(nil, nil, nil)
			do(h, "POST", "/x", "k", "b")
			if count.Load() != 2 {
				t.Fatalf("fail-open responses must not be stored (handler ran %d times)", count.Load())
			}
		})
	})

	t.Run("StoreFailureOnComplete", func(t *testing.T) {
		t.Run("ResponseReturnedAndReleased", func(t *testing.T) {
			fs := &flakyStore{inner: newStore(t)}
			fs.setErrs(nil, errors.New("boom"), nil)
			var count atomic.Int32
			h := httpidem.New(fs, httpidem.Logger(quietLogger()))(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					count.Add(1)
					w.WriteHeader(http.StatusCreated)
					_, _ = w.Write([]byte("work-done"))
				}))
			rr := do(h, "POST", "/x", "k", "b")
			if rr.Code != http.StatusCreated || rr.Body.String() != "work-done" {
				t.Fatalf("the captured response must reach the client despite the Complete failure (code=%d body=%q)", rr.Code, rr.Body.String())
			}
			fs.setErrs(nil, nil, nil)
			do(h, "POST", "/x", "k", "b")
			if count.Load() != 2 {
				t.Fatalf("the best-effort Release must make the key re-executable (handler ran %d times)", count.Load())
			}
		})
		t.Run("ReleaseAlsoFailsKeepsReservation", func(t *testing.T) {
			fs := &flakyStore{inner: newStore(t)}
			fs.setErrs(nil, errors.New("boom"), errors.New("boom2"))
			var count atomic.Int32
			h := httpidem.New(fs, httpidem.Logger(quietLogger()))(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					count.Add(1)
					w.WriteHeader(http.StatusCreated)
				}))
			if rr := do(h, "POST", "/x", "k", "b"); rr.Code != http.StatusCreated {
				t.Fatalf("code = %d, want 201", rr.Code)
			}
			if rr := do(h, "POST", "/x", "k", "b"); rr.Code != http.StatusConflict {
				t.Fatalf("retry while still reserved = %d, want 409 until lease expiry (§4.6)", rr.Code)
			}
		})
	})

	t.Run("StoreFailureOnRelease", func(t *testing.T) {
		fs := &flakyStore{inner: newStore(t)}
		fs.setErrs(nil, nil, errors.New("boom"))
		var count atomic.Int32
		h := httpidem.New(fs, httpidem.Logger(quietLogger()))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				w.WriteHeader(http.StatusInternalServerError) // 500 → Discard → Release
			}))
		if rr := do(h, "POST", "/x", "k", "b"); rr.Code != http.StatusInternalServerError {
			t.Fatalf("code = %d, want the handler's 500", rr.Code)
		}
		if rr := do(h, "POST", "/x", "k", "b"); rr.Code != http.StatusConflict {
			t.Fatalf("retry while still reserved = %d, want 409 until lease expiry (§4.6)", rr.Code)
		}
	})

	t.Run("LeaseExpirySurvivor", func(t *testing.T) {
		const leaseTTL = 500 * time.Millisecond
		store := newStore(t)
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logBuf, nil))

		var count atomic.Int32
		entered := make(chan struct{})
		release := make(chan struct{})
		h := httpidem.New(store, httpidem.LeaseTTL(leaseTTL), httpidem.Logger(logger))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if count.Add(1) == 1 {
					close(entered)
					<-release // A outlives its lease
					w.WriteHeader(http.StatusCreated)
					_, _ = w.Write([]byte("result-of-A"))
					return
				}
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte("result-of-B"))
			}))

		// A wins the lease, then stalls past its expiry.
		t0 := time.Now()
		aDone := make(chan *httptest.ResponseRecorder, 1)
		go func() { aDone <- do(h, "POST", "/x", "k", "body") }()
		<-entered
		time.Sleep(time.Until(t0.Add(leaseTTL + 150*time.Millisecond)))

		// B re-reserves the expired key and completes normally.
		b := do(h, "POST", "/x", "k", "body")
		if b.Code != http.StatusCreated || b.Body.String() != "result-of-B" {
			t.Fatalf("B = (%d, %q), want a fresh execution", b.Code, b.Body.String())
		}
		if b.Header().Get(httpidem.HeaderIdempotencyReplayed) != "" {
			t.Fatal("B must be a fresh execution, not a replay")
		}

		// A finishes late: its response still reaches its client (§3.3)...
		close(release)
		a := <-aDone
		if a.Code != http.StatusCreated || a.Body.String() != "result-of-A" {
			t.Fatalf("A = (%d, %q), want A's own completed work returned", a.Code, a.Body.String())
		}
		// ...with a lease_lost warning, and without disturbing B's record.
		if !strings.Contains(logBuf.String(), "lease_lost") {
			t.Fatalf("expected a lease_lost warning in the logs, got:\n%s", logBuf.String())
		}
		replay := do(h, "POST", "/x", "k", "body")
		if replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" || replay.Body.String() != "result-of-B" {
			t.Fatalf("replay = (%q, replayed=%q), want only B's result stored (§3.3)",
				replay.Body.String(), replay.Header().Get(httpidem.HeaderIdempotencyReplayed))
		}

		// Storage state: the completed record holds B's payload.
		rec, err := store.Get(context.Background(), "k")
		if err != nil || rec == nil || rec.State != idemlease.StateCompleted {
			t.Fatalf("Get = (%+v, %v), want a completed record", rec, err)
		}
		var sr httpidem.StoredResponse
		if err := sr.UnmarshalBinary(rec.Payload); err != nil {
			t.Fatalf("decoding stored payload: %v", err)
		}
		if string(sr.Body) != "result-of-B" {
			t.Fatalf("stored body = %q, want B's result only", sr.Body)
		}
		if count.Load() != 2 {
			t.Fatalf("handler ran %d times, want 2", count.Load())
		}
	})

	t.Run("ConcurrentSingleExecution", func(t *testing.T) {
		var count atomic.Int32
		h := httpidem.New(newStore(t), httpidem.Logger(quietLogger()))(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte("winner"))
			}))

		const n = 100
		results := make([]*httptest.ResponseRecorder, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i] = do(h, "POST", "/x", "same-key", "same-body")
			}(i)
		}
		wg.Wait()

		if got := count.Load(); got != 1 {
			t.Fatalf("handler ran %d times, want exactly 1 within the lease (§10)", got)
		}
		var fresh, replayed, conflicts int
		for i, rr := range results {
			switch {
			case rr.Code == http.StatusCreated && rr.Header().Get(httpidem.HeaderIdempotencyReplayed) == "true":
				replayed++
			case rr.Code == http.StatusCreated:
				fresh++
			case rr.Code == http.StatusConflict:
				conflicts++
			default:
				t.Fatalf("#%d: unexpected status %d", i, rr.Code)
			}
		}
		if fresh != 1 {
			t.Fatalf("fresh executions = %d (replayed=%d conflicts=%d), want exactly 1", fresh, replayed, conflicts)
		}
	})
}
