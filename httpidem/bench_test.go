package httpidem_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/repenguin22/idemlease/httpidem"
	"github.com/repenguin22/idemlease/memstore"
)

var benchBody = []byte(`{"amount":42}`)

func benchHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":42}`))
	})
}

// BenchmarkHandlerBaseline is the same handler without the middleware;
// the delta against the other benchmarks is the middleware overhead.
func BenchmarkHandlerBaseline(b *testing.B) {
	h := benchHandler()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("POST", "/orders", bytes.NewReader(benchBody))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
	}
}

// BenchmarkFirstExecution is the no-replay path the README quotes:
// key parse + fingerprint + Begin + capture + Finish(Persist), with a
// fresh key every iteration (memstore).
func BenchmarkFirstExecution(b *testing.B) {
	h := httpidem.New(memstore.New(), httpidem.Logger(quietLogger()))(benchHandler())
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("POST", "/orders", bytes.NewReader(benchBody))
		req.Header.Set(httpidem.HeaderIdempotencyKey, strconv.Itoa(i))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
	}
}

// BenchmarkReplay serves every request from the stored record.
func BenchmarkReplay(b *testing.B) {
	h := httpidem.New(memstore.New(), httpidem.Logger(quietLogger()))(benchHandler())
	prime := httptest.NewRequest("POST", "/orders", bytes.NewReader(benchBody))
	prime.Header.Set(httpidem.HeaderIdempotencyKey, "replay-key")
	h.ServeHTTP(httptest.NewRecorder(), prime)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("POST", "/orders", bytes.NewReader(benchBody))
		req.Header.Set(httpidem.HeaderIdempotencyKey, "replay-key")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
	}
}

// BenchmarkNoKeyPassThrough measures the gate cost for requests that
// carry no Idempotency-Key (Require(false), the default).
func BenchmarkNoKeyPassThrough(b *testing.B) {
	h := httpidem.New(memstore.New(), httpidem.Logger(quietLogger()))(benchHandler())
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("POST", "/orders", bytes.NewReader(benchBody))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
	}
}
