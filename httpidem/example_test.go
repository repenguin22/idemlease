package httpidem_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/repenguin22/idemlease/httpidem"
	"github.com/repenguin22/idemlease/memstore"
)

// ExampleNew shows the default integration: wrap any http.Handler and
// retries with the same Idempotency-Key are served from the stored
// response instead of re-executing the handler.
func ExampleNew() {
	mux := http.NewServeMux()
	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"id":42}`)
	})

	// memstore is for development; use redistore in production.
	mw := httpidem.New(memstore.New(), httpidem.Require(true))
	srv := httptest.NewServer(mw(mux))
	defer srv.Close()

	post := func() {
		req, _ := http.NewRequest("POST", srv.URL+"/orders", strings.NewReader(`{"amount":100}`))
		req.Header.Set("Idempotency-Key", "order-42")
		res, _ := http.DefaultClient.Do(req)
		res.Body.Close()
		fmt.Printf("%d replayed=%q\n", res.StatusCode, res.Header.Get("Idempotency-Replayed"))
	}
	post() // first request executes the handler
	post() // the retry is answered from the stored response

	// Output:
	// 201 replayed=""
	// 201 replayed="true"
}
