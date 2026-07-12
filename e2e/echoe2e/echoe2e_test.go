// Package echoe2e_test pins the documented Echo integration path
// (REQUIREMENTS §2.3, §10): the httpidem middleware embedded with
// echo.WrapMiddleware, including response capture through Echo's own
// writer wrapper (double wrap) and Flush detection.
package echoe2e_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/labstack/echo/v4"
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
	req := httptest.NewRequest("POST", "/orders", rd)
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	if key != "" {
		req.Header.Set(httpidem.HeaderIdempotencyKey, key)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// TestReplayThroughEcho drives the full lifecycle through Echo's writer
// wrapper: fresh execution, replay with headers, and the 422 rejection.
func TestReplayThroughEcho(t *testing.T) {
	var count atomic.Int32
	e := echo.New()
	e.Use(echo.WrapMiddleware(httpidem.New(memstore.New(), httpidem.Logger(quietLogger()))))
	e.POST("/orders", func(c echo.Context) error {
		count.Add(1)
		return c.JSON(http.StatusCreated, map[string]int{"id": 42})
	})

	first := do(e, "k1", `{"n":1}`)
	if first.Code != http.StatusCreated || first.Header().Get(httpidem.HeaderIdempotencyReplayed) != "" {
		t.Fatalf("first = %d (replayed=%q), want a fresh 201", first.Code, first.Header().Get(httpidem.HeaderIdempotencyReplayed))
	}

	replay := do(e, "k1", `{"n":1}`)
	if replay.Code != http.StatusCreated || replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
		t.Fatalf("replay = %d (replayed=%q), want a replayed 201", replay.Code, replay.Header().Get(httpidem.HeaderIdempotencyReplayed))
	}
	if replay.Body.String() != first.Body.String() {
		t.Fatalf("replay body = %q, want %q", replay.Body.String(), first.Body.String())
	}
	if ct := replay.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("replayed Content-Type = %q, want application/json (captured through Echo's wrapper)", ct)
	}
	if count.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", count.Load())
	}

	if rr := do(e, "k1", `{"n":2}`); rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("different payload = %d, want 422", rr.Code)
	}
}

// TestFlushDetectionThroughEcho verifies that a Flush issued via Echo's
// Response wrapper reaches the capture writer and voids persistence.
func TestFlushDetectionThroughEcho(t *testing.T) {
	var count atomic.Int32
	e := echo.New()
	e.Use(echo.WrapMiddleware(httpidem.New(memstore.New(), httpidem.Logger(quietLogger()))))
	e.POST("/orders", func(c echo.Context) error {
		count.Add(1)
		c.Response().WriteHeader(http.StatusOK)
		if _, err := c.Response().Write([]byte("chunk-1")); err != nil {
			return err
		}
		c.Response().Flush() // streaming through Echo's double wrap
		_, err := c.Response().Write([]byte("chunk-2"))
		return err
	})

	first := do(e, "k1", `{"n":1}`)
	if first.Body.String() != "chunk-1chunk-2" || !first.Flushed {
		t.Fatalf("streamed response must reach the client (body=%q flushed=%v)", first.Body.String(), first.Flushed)
	}
	do(e, "k1", `{"n":1}`)
	if count.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2 (flushed responses are not stored)", count.Load())
	}
}
