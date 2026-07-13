package ginadapter_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/repenguin22/idemlease"
	"github.com/repenguin22/idemlease/ginadapter"
	"github.com/repenguin22/idemlease/httpidem"
	"github.com/repenguin22/idemlease/memstore"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newEngine(store idemlease.Store, handler gin.HandlerFunc, opts ...ginadapter.Option) *gin.Engine {
	opts = append(opts, ginadapter.Logger(quietLogger()))
	e := gin.New()
	e.Use(ginadapter.New(store, opts...))
	e.POST("/orders", handler)
	return e
}

func do(e *gin.Engine, method, target, key, body string, mutate ...func(*http.Request)) *httptest.ResponseRecorder {
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
	e.ServeHTTP(rr, req)
	return rr
}

// TestLifecycle drives fresh execution → replay → 422 through Gin's
// context and writer (c.JSON path).
func TestLifecycle(t *testing.T) {
	var count atomic.Int32
	e := newEngine(memstore.New(), func(c *gin.Context) {
		count.Add(1)
		c.Header("Location", "/orders/42")
		c.JSON(http.StatusCreated, gin.H{"id": 42})
	})

	first := do(e, "POST", "/orders?a=1", "k1", `{"n":1}`)
	if first.Code != http.StatusCreated || first.Header().Get(httpidem.HeaderIdempotencyReplayed) != "" {
		t.Fatalf("first = %d (replayed=%q), want a fresh 201", first.Code, first.Header().Get(httpidem.HeaderIdempotencyReplayed))
	}

	replay := do(e, "POST", "/orders?a=1", "k1", `{"n":1}`)
	if replay.Code != http.StatusCreated || replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
		t.Fatalf("replay = %d (replayed=%q), want a replayed 201", replay.Code, replay.Header().Get(httpidem.HeaderIdempotencyReplayed))
	}
	if replay.Body.String() != first.Body.String() {
		t.Fatalf("replay body = %q, want %q", replay.Body.String(), first.Body.String())
	}
	if replay.Header().Get("Location") != "/orders/42" || !strings.HasPrefix(replay.Header().Get("Content-Type"), "application/json") {
		t.Fatal("allowlisted headers must be replayed through Gin's writer wrapper")
	}
	if count.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", count.Load())
	}

	if rr := do(e, "POST", "/orders?a=1", "k1", `{"n":2}`); rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("different payload = %d, want 422", rr.Code)
	}
}

// TestWriteStringCapture pins the c.String path (gin uses WriteString).
func TestWriteStringCapture(t *testing.T) {
	var count atomic.Int32
	e := newEngine(memstore.New(), func(c *gin.Context) {
		count.Add(1)
		c.String(http.StatusCreated, "plain-result")
	})
	do(e, "POST", "/orders", "k", "b")
	replay := do(e, "POST", "/orders", "k", "b")
	if replay.Body.String() != "plain-result" || replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
		t.Fatalf("replay = (%q, replayed=%q), want the WriteString-captured body", replay.Body.String(), replay.Header().Get(httpidem.HeaderIdempotencyReplayed))
	}
	if count.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", count.Load())
	}
}

// TestGatekeeping covers pass-through and 400 rejections.
func TestGatekeeping(t *testing.T) {
	t.Run("missing key passes through by default", func(t *testing.T) {
		var count atomic.Int32
		e := newEngine(memstore.New(), func(c *gin.Context) { count.Add(1); c.Status(200) })
		if rr := do(e, "POST", "/orders", "", "b"); rr.Code != 200 || count.Load() != 1 {
			t.Fatalf("code=%d handler=%d, want pass-through", rr.Code, count.Load())
		}
	})
	t.Run("missing key with Require is 400 and aborts", func(t *testing.T) {
		var count atomic.Int32
		e := newEngine(memstore.New(), func(c *gin.Context) { count.Add(1) }, ginadapter.Require(true))
		rr := do(e, "POST", "/orders", "", "b")
		if rr.Code != 400 || count.Load() != 0 {
			t.Fatalf("code=%d handler=%d, want 400 and no execution", rr.Code, count.Load())
		}
		if ct := rr.Header().Get("Content-Type"); ct != "application/problem+json" {
			t.Fatalf("Content-Type = %q, want problem+json (httpidem.DefaultErrorWriter)", ct)
		}
	})
	t.Run("invalid key is 400", func(t *testing.T) {
		var count atomic.Int32
		e := newEngine(memstore.New(), func(c *gin.Context) { count.Add(1) })
		if rr := do(e, "POST", "/orders", "bad\x1fkey", "b"); rr.Code != 400 || count.Load() != 0 {
			t.Fatalf("code=%d handler=%d, want 400 and no execution", rr.Code, count.Load())
		}
	})
	t.Run("GET passes through", func(t *testing.T) {
		e := gin.New()
		e.Use(ginadapter.New(memstore.New(), ginadapter.Logger(quietLogger())))
		var count atomic.Int32
		e.GET("/orders", func(c *gin.Context) { count.Add(1); c.Status(200) })
		if rr := do(e, "GET", "/orders", "k", ""); rr.Code != 200 || count.Load() != 1 {
			t.Fatalf("code=%d handler=%d, want pass-through", rr.Code, count.Load())
		}
	})
}

// TestInFlight409 seeds a live reservation and checks the 409 mapping
// with Retry-After.
func TestInFlight409(t *testing.T) {
	store := memstore.New()
	if _, err := store.Reserve(context.Background(), idemlease.Record{
		Key: "k", State: idemlease.StateReserved, Token: "t",
		LeaseExpiresAt: time.Now().Add(30 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	e := newEngine(store, func(c *gin.Context) { t.Error("handler must not run") })
	rr := do(e, "POST", "/orders", "k", "b")
	if rr.Code != http.StatusConflict {
		t.Fatalf("code = %d, want 409", rr.Code)
	}
	if ra, err := strconv.Atoi(rr.Header().Get("Retry-After")); err != nil || ra < 1 {
		t.Fatalf("Retry-After = %q, want an integer >= 1", rr.Header().Get("Retry-After"))
	}
}

// TestKeyScope pins per-tenant isolation using a Gin-context scope.
func TestKeyScope(t *testing.T) {
	var count atomic.Int32
	e := newEngine(memstore.New(), func(c *gin.Context) {
		count.Add(1)
		c.Status(http.StatusCreated)
	}, ginadapter.KeyScope(func(c *gin.Context) string { return c.GetHeader("X-Tenant") }))
	tenant := func(id string) func(*http.Request) {
		return func(r *http.Request) { r.Header.Set("X-Tenant", id) }
	}

	do(e, "POST", "/orders", "k", "same", tenant("A"))
	if rr := do(e, "POST", "/orders", "k", "same", tenant("B")); rr.Header().Get(httpidem.HeaderIdempotencyReplayed) != "" {
		t.Fatal("tenant B must execute independently")
	}
	if count.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2", count.Load())
	}
	if rr := do(e, "POST", "/orders", "k", "same", tenant("A")); rr.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
		t.Fatal("tenant A retry must replay within its scope")
	}
}

// TestReplayPolicyAndSetError pins the §5.1 defaults and the
// ErrorChannel plumbing through Gin.
func TestReplayPolicyAndSetError(t *testing.T) {
	t.Run("500 is discarded", func(t *testing.T) {
		var count atomic.Int32
		e := newEngine(memstore.New(), func(c *gin.Context) {
			count.Add(1)
			c.Status(http.StatusInternalServerError)
		})
		do(e, "POST", "/orders", "k", "b")
		do(e, "POST", "/orders", "k", "b")
		if count.Load() != 2 {
			t.Fatalf("handler ran %d times, want 2", count.Load())
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
		e := newEngine(memstore.New(), func(c *gin.Context) {
			count.Add(1)
			httpidem.SetError(c.Request.Context(), errors.New("transient"))
			c.Status(http.StatusOK)
		}, ginadapter.Policy(policy))
		do(e, "POST", "/orders", "k", "b")
		do(e, "POST", "/orders", "k", "b")
		if count.Load() != 2 {
			t.Fatalf("handler ran %d times, want 2 (SetError must force Discard)", count.Load())
		}
	})
}

// TestFlushNotStored pins streaming detection through gin.ResponseWriter.
func TestFlushNotStored(t *testing.T) {
	var count atomic.Int32
	e := newEngine(memstore.New(), func(c *gin.Context) {
		count.Add(1)
		_, _ = c.Writer.Write([]byte("chunk"))
		c.Writer.Flush()
	})
	if rr := do(e, "POST", "/orders", "k", "b"); rr.Body.String() != "chunk" || !rr.Flushed {
		t.Fatalf("streamed response must reach the client (body=%q flushed=%v)", rr.Body.String(), rr.Flushed)
	}
	do(e, "POST", "/orders", "k", "b")
	if count.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2 (streamed responses are not stored)", count.Load())
	}
}

// TestPanicDiscardsAndRepanics: without gin.Recovery the panic must
// propagate, and the reservation must be released.
func TestPanicDiscardsAndRepanics(t *testing.T) {
	var count atomic.Int32
	e := newEngine(memstore.New(), func(c *gin.Context) {
		if count.Add(1) == 1 {
			panic("boom")
		}
		c.Status(http.StatusCreated)
	})

	func() {
		defer func() {
			if p := recover(); p != "boom" {
				t.Fatalf("recovered %v, want the original panic value", p)
			}
		}()
		do(e, "POST", "/orders", "k", "b")
		t.Fatal("panic must propagate")
	}()

	if rr := do(e, "POST", "/orders", "k", "b"); rr.Code != http.StatusCreated {
		t.Fatalf("after a panic the key must be re-executable, got %d", rr.Code)
	}
}

// TestStoreFailure pins §4.6 fail-closed / fail-open through Gin.
func TestStoreFailure(t *testing.T) {
	t.Run("fail-closed 503", func(t *testing.T) {
		var count atomic.Int32
		e := newEngine(failingStore{}, func(c *gin.Context) { count.Add(1) })
		if rr := do(e, "POST", "/orders", "k", "b"); rr.Code != http.StatusServiceUnavailable || count.Load() != 0 {
			t.Fatalf("code=%d handler=%d, want 503 and no execution", rr.Code, count.Load())
		}
	})
	t.Run("fail-open passes through", func(t *testing.T) {
		var count atomic.Int32
		e := newEngine(failingStore{}, func(c *gin.Context) {
			count.Add(1)
			c.Status(http.StatusCreated)
		}, ginadapter.FailOpen(true))
		if rr := do(e, "POST", "/orders", "k", "b"); rr.Code != http.StatusCreated || count.Load() != 1 {
			t.Fatalf("code=%d handler=%d, want the handler to run without idempotency", rr.Code, count.Load())
		}
	})
}

// failingStore always fails Reserve.
type failingStore struct{}

var _ idemlease.Store = failingStore{}

func (failingStore) Reserve(context.Context, idemlease.Record) (*idemlease.Record, error) {
	return nil, errors.New("store down")
}
func (failingStore) Complete(context.Context, string, string, []byte, time.Duration) error {
	return errors.New("store down")
}
func (failingStore) Release(context.Context, string, string) error { return errors.New("store down") }
func (failingStore) Get(context.Context, string) (*idemlease.Record, error) {
	return nil, errors.New("store down")
}
