package pgstore_test

// Acceptance tests for the transactional-join success/failure matrix
// T1-T10 in docs/design/pgstore-txjoin.md. Each subtest pins the four
// observations the design demands: record state, business rows, client
// observation, and retry behavior.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/repenguin22/idemlease"
	"github.com/repenguin22/idemlease/httpidem"
	"github.com/repenguin22/idemlease/pgstore"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTxHarness returns a Store on a fresh records table plus a fresh
// business table ("orders"): id serial, note text with a CHECK that
// rejects the value "bad" (used to force business failures).
func newTxHarness(t *testing.T) (store *pgstore.Store, db *sql.DB, orders string) {
	t.Helper()
	db, err := sql.Open("pgx", os.Getenv("PG_DSN"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	n := tableSeq.Add(1)
	recTable := fmt.Sprintf("idemlease_tx_%d_%d", os.Getpid(), n)
	orders = fmt.Sprintf("orders_tx_%d_%d", os.Getpid(), n)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, pgstore.Schema(recTable)); err != nil {
		t.Fatalf("create records table: %v", err)
	}
	// Deliberately no unique constraint on note: T9 must show the CAS
	// serialization alone prevents duplicate business commits.
	if _, err := db.ExecContext(ctx,
		"CREATE TABLE "+orders+" (id serial PRIMARY KEY, note text NOT NULL CHECK (note <> 'bad'))"); err != nil {
		t.Fatalf("create orders table: %v", err)
	}
	t.Cleanup(func() {
		cctx := context.Background()
		_, _ = db.ExecContext(cctx, "DROP TABLE IF EXISTS "+recTable)
		_, _ = db.ExecContext(cctx, "DROP TABLE IF EXISTS "+orders)
		_ = db.Close()
	})
	return pgstore.New(db, pgstore.Table(recTable)), db, orders
}

func orderCount(t *testing.T, db *sql.DB, orders string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), "SELECT count(*) FROM "+orders).Scan(&n); err != nil {
		t.Fatalf("count orders: %v", err)
	}
	return n
}

// storedBody fetches the completed record and decodes its payload body.
func storedBody(t *testing.T, store *pgstore.Store, key string) []byte {
	t.Helper()
	rec, err := store.Get(context.Background(), key)
	if err != nil || rec == nil || rec.State != idemlease.StateCompleted {
		t.Fatalf("Get(%q) = (%+v, %v), want a completed record", key, rec, err)
	}
	var sr httpidem.StoredResponse
	if err := sr.UnmarshalBinary(rec.Payload); err != nil {
		t.Fatalf("decode stored payload: %v", err)
	}
	return sr.Body
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

// canonicalHandler is the documented tx-join flow: business insert,
// CompleteTx, commit, MarkFinished, WriteStored.
func canonicalHandler(t *testing.T, store *pgstore.Store, db *sql.DB, orders string,
	count *atomic.Int32, body func(n int32) []byte) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		n := count.Add(1)
		rsv, ok := httpidem.ReservationFromContext(r.Context())
		if !ok {
			http.Error(w, "no reservation", http.StatusInternalServerError)
			return
		}
		note, _ := io.ReadAll(r.Body)
		ctx := r.Context()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer tx.Rollback()

		if _, err := tx.ExecContext(ctx, "INSERT INTO "+orders+" (note) VALUES ($1)", string(note)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		sr := httpidem.StoredResponse{
			StatusCode: http.StatusCreated,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       body(n),
		}
		payload, err := sr.MarshalBinary()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		leaseLost, err := store.CompleteTx(ctx, tx, rsv.Key, rsv.Token, payload, rsv.Options.RecordTTL)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if leaseLost {
			// T3/T4: MUST roll back (the deferred Rollback does it).
			w.Header().Set("Retry-After", "1")
			http.Error(w, "another execution owns this key", http.StatusConflict)
			return
		}
		if err := tx.Commit(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		httpidem.MarkFinished(r.Context())
		_ = httpidem.WriteStored(w, sr)
	}
}

// T1: happy path — one business row, replay serves the stored response.
func TestTxJoin_T1_HappyPath(t *testing.T) {
	requirePG(t)
	store, db, orders := newTxHarness(t)
	var count atomic.Int32
	h := httpidem.New(store, httpidem.Logger(quietLogger()))(
		canonicalHandler(t, store, db, orders, &count, func(int32) []byte { return []byte(`{"ok":true}`) }))

	first := do(h, "k", "note-1")
	if first.Code != http.StatusCreated || first.Body.String() != `{"ok":true}` {
		t.Fatalf("first = (%d, %q), want the committed 201", first.Code, first.Body.String())
	}
	if got := string(storedBody(t, store, "k")); got != `{"ok":true}` {
		t.Fatalf("stored body = %q, want the handler's response", got)
	}
	if n := orderCount(t, db, orders); n != 1 {
		t.Fatalf("orders = %d, want 1", n)
	}

	replay := do(h, "k", "note-1")
	if replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" || replay.Body.String() != `{"ok":true}` {
		t.Fatalf("replay = (%q, replayed=%q), want the stored response",
			replay.Body.String(), replay.Header().Get(httpidem.HeaderIdempotencyReplayed))
	}
	if n := orderCount(t, db, orders); n != 1 || count.Load() != 1 {
		t.Fatalf("orders=%d handler=%d after replay, want 1 and 1", n, count.Load())
	}
}

// T2: business failure rolls everything back; the key is immediately
// re-executable after the middleware's Release.
func TestTxJoin_T2_BusinessErrorRollsBack(t *testing.T) {
	requirePG(t)
	store, db, orders := newTxHarness(t)
	var count atomic.Int32
	h := httpidem.New(store, httpidem.Logger(quietLogger()))(
		canonicalHandler(t, store, db, orders, &count, func(int32) []byte { return []byte("x") }))

	// "bad" violates the CHECK constraint → 500 → policy Discard.
	if rr := do(h, "k", "bad"); rr.Code != http.StatusInternalServerError {
		t.Fatalf("code = %d, want 500", rr.Code)
	}
	if n := orderCount(t, db, orders); n != 0 {
		t.Fatalf("orders = %d, want 0 (rolled back)", n)
	}
	if rec, err := store.Get(context.Background(), "k"); err != nil || rec != nil {
		t.Fatalf("Get = (%+v, %v), want (nil, nil): the reservation must be released", rec, err)
	}
	// Immediately re-executable (with a good payload this time... same
	// fingerprint required, so retry the same body and watch it re-run).
	if rr := do(h, "k", "bad"); rr.Code != http.StatusInternalServerError || count.Load() != 2 {
		t.Fatalf("retry = %d handler=%d, want an immediate re-execution", rr.Code, count.Load())
	}
}

// T3+T9: a survivor racing a re-reservation commits at most one
// business row; the loser rolls back and answers 409.
func TestTxJoin_T3_T9_SurvivorCommitsAtMostOnce(t *testing.T) {
	requirePG(t)
	store, db, orders := newTxHarness(t)
	const leaseTTL = 500 * time.Millisecond

	var count atomic.Int32
	var firstArrived atomic.Bool
	entered := make(chan struct{})
	release := make(chan struct{})
	inner := canonicalHandler(t, store, db, orders, &count, func(n int32) []byte {
		return []byte(fmt.Sprintf("result-%d", n))
	})
	h := httpidem.New(store, httpidem.LeaseTTL(leaseTTL), httpidem.Logger(quietLogger()))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if firstArrived.CompareAndSwap(false, true) {
				close(entered)
				<-release // A stalls past its lease before doing any work
			}
			inner(w, r)
		}))

	t0 := time.Now()
	aDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { aDone <- do(h, "k", "same-body") }()
	<-entered
	time.Sleep(time.Until(t0.Add(leaseTTL + 150*time.Millisecond)))

	// B re-reserves the expired key and commits; it reaches the inner
	// handler first, so its body is deterministically result-1.
	b := do(h, "k", "same-body")
	if b.Code != http.StatusCreated || b.Body.String() != "result-1" {
		t.Fatalf("B = (%d, %q), want a fresh committed execution (result-1)", b.Code, b.Body.String())
	}
	bBody := b.Body.String()

	// A wakes, loses the CAS, rolls back, answers 409.
	close(release)
	a := <-aDone
	if a.Code != http.StatusConflict {
		t.Fatalf("A = %d, want 409 (lease lost, transaction rolled back)", a.Code)
	}

	if n := orderCount(t, db, orders); n != 1 {
		t.Fatalf("orders = %d, want exactly 1 (T9: at most one business commit)", n)
	}
	if got := string(storedBody(t, store, "k")); got != bBody {
		t.Fatalf("stored body = %q, want the winner's %q", got, bBody)
	}
	if replay := do(h, "k", "same-body"); replay.Body.String() != bBody ||
		replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
		t.Fatalf("replay = (%q, replayed=%q), want the winner's result",
			replay.Body.String(), replay.Header().Get(httpidem.HeaderIdempotencyReplayed))
	}
}

// T4: lease expired with no new owner — CompleteTx reports leaseLost
// and the rolled-back transaction leaves nothing behind.
func TestTxJoin_T4_ExpiredUnowned(t *testing.T) {
	requirePG(t)
	store, db, orders := newTxHarness(t)
	ctx := context.Background()

	out, err := idemlease.Begin(ctx, store, "k", []byte("fp"), idemlease.Options{LeaseTTL: 300 * time.Millisecond})
	if err != nil || out.Action != idemlease.Proceed {
		t.Fatalf("Begin = (%+v, %v), want Proceed", out, err)
	}
	time.Sleep(500 * time.Millisecond) // past the lease, nobody re-reserves

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+orders+" (note) VALUES ('n')"); err != nil {
		t.Fatal(err)
	}
	leaseLost, err := store.CompleteTx(ctx, tx, "k", out.Token, []byte("p"), time.Hour)
	if err != nil || !leaseLost {
		t.Fatalf("CompleteTx = (%v, %v), want (true, nil)", leaseLost, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n := orderCount(t, db, orders); n != 0 {
		t.Fatalf("orders = %d, want 0", n)
	}
	if rec, err := store.Get(ctx, "k"); err != nil || rec != nil {
		t.Fatalf("Get = (%+v, %v), want (nil, nil)", rec, err)
	}
}

// T5: a crash before commit leaves the reservation intact; the key is
// blocked until lease expiry, then re-executable (§9.3: rollback joins
// the lease-expiry path).
func TestTxJoin_T5_CrashBeforeCommit(t *testing.T) {
	requirePG(t)
	store, db, orders := newTxHarness(t)
	ctx := context.Background()
	const leaseTTL = 2 * time.Second

	t0 := time.Now()
	out, err := idemlease.Begin(ctx, store, "k", []byte("fp"), idemlease.Options{LeaseTTL: leaseTTL})
	if err != nil || out.Action != idemlease.Proceed {
		t.Fatalf("Begin = (%+v, %v), want Proceed", out, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+orders+" (note) VALUES ('n')"); err != nil {
		t.Fatal(err)
	}
	if leaseLost, err := store.CompleteTx(ctx, tx, "k", out.Token, []byte("p"), time.Hour); err != nil || leaseLost {
		t.Fatalf("CompleteTx = (%v, %v), want (false, nil)", leaseLost, err)
	}
	_ = tx.Rollback() // simulated crash: the transaction never commits

	rec, err := store.Get(ctx, "k")
	if err != nil || rec == nil || rec.State != idemlease.StateReserved || rec.Token != out.Token {
		t.Fatalf("Get = (%+v, %v), want the untouched reservation", rec, err)
	}
	if n := orderCount(t, db, orders); n != 0 {
		t.Fatalf("orders = %d, want 0", n)
	}
	if retry, err := idemlease.Begin(ctx, store, "k", []byte("fp"), idemlease.Options{LeaseTTL: leaseTTL}); err != nil || retry.Action != idemlease.RejectInFlight {
		t.Fatalf("Begin during lease = (%+v, %v), want RejectInFlight", retry, err)
	}
	time.Sleep(time.Until(t0.Add(leaseTTL + 150*time.Millisecond)))
	if again, err := idemlease.Begin(ctx, store, "k", []byte("fp"), idemlease.Options{LeaseTTL: leaseTTL}); err != nil || again.Action != idemlease.Proceed {
		t.Fatalf("Begin after expiry = (%+v, %v), want Proceed", again, err)
	}
}

// T6: commit succeeded but the response was never written — the retry
// replays the stored response (the core value of the join).
func TestTxJoin_T6_CommitThenNoResponse(t *testing.T) {
	requirePG(t)
	store, db, orders := newTxHarness(t)
	sr := httpidem.StoredResponse{StatusCode: 201, Header: http.Header{}, Body: []byte("committed-result")}
	var count atomic.Int32
	h := httpidem.New(store, httpidem.Logger(quietLogger()))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			rsv, _ := httpidem.ReservationFromContext(r.Context())
			ctx := r.Context()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Error(err)
				return
			}
			defer tx.Rollback()
			if _, err := tx.ExecContext(ctx, "INSERT INTO "+orders+" (note) VALUES ('n')"); err != nil {
				t.Error(err)
				return
			}
			payload, _ := sr.MarshalBinary()
			if leaseLost, err := store.CompleteTx(ctx, tx, rsv.Key, rsv.Token, payload, rsv.Options.RecordTTL); err != nil || leaseLost {
				t.Errorf("CompleteTx = (%v, %v)", leaseLost, err)
				return
			}
			if err := tx.Commit(); err != nil {
				t.Error(err)
				return
			}
			httpidem.MarkFinished(r.Context())
			// Simulated death before writing the response.
		}))

	do(h, "k", "b") // the client never sees the result
	if n := orderCount(t, db, orders); n != 1 {
		t.Fatalf("orders = %d, want 1 (committed)", n)
	}
	replay := do(h, "k", "b")
	if replay.Code != 201 || replay.Body.String() != "committed-result" ||
		replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" {
		t.Fatalf("replay = (%d, %q, replayed=%q), want the committed result — this is the point of the join",
			replay.Code, replay.Body.String(), replay.Header().Get(httpidem.HeaderIdempotencyReplayed))
	}
	if count.Load() != 1 || orderCount(t, db, orders) != 1 {
		t.Fatalf("handler=%d orders=%d, want 1 and 1", count.Load(), orderCount(t, db, orders))
	}
}

// T7: MarkFinished forgotten — one spurious lease_lost warning, record
// and business data intact, replay works.
func TestTxJoin_T7_MarkFinishedForgotten(t *testing.T) {
	requirePG(t)
	store, db, orders := newTxHarness(t)
	var logBuf strings.Builder
	sr := httpidem.StoredResponse{StatusCode: 201, Header: http.Header{}, Body: []byte("joined")}
	var count atomic.Int32
	h := httpidem.New(store, httpidem.Logger(slog.New(slog.NewTextHandler(&logBuf, nil))))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			rsv, _ := httpidem.ReservationFromContext(r.Context())
			ctx := r.Context()
			tx, _ := db.BeginTx(ctx, nil)
			defer tx.Rollback()
			_, _ = tx.ExecContext(ctx, "INSERT INTO "+orders+" (note) VALUES ('n')")
			payload, _ := sr.MarshalBinary()
			_, _ = store.CompleteTx(ctx, tx, rsv.Key, rsv.Token, payload, rsv.Options.RecordTTL)
			_ = tx.Commit()
			// MarkFinished forgotten.
			_ = httpidem.WriteStored(w, sr)
		}))

	if rr := do(h, "k", "b"); rr.Code != 201 {
		t.Fatalf("first = %d, want 201", rr.Code)
	}
	if !strings.Contains(logBuf.String(), "lease_lost") {
		t.Fatalf("want the spurious lease_lost warning (T7), log:\n%s", logBuf.String())
	}
	if n := orderCount(t, db, orders); n != 1 {
		t.Fatalf("orders = %d, want 1", n)
	}
	replay := do(h, "k", "b")
	if replay.Body.String() != "joined" || replay.Header().Get(httpidem.HeaderIdempotencyReplayed) != "true" || count.Load() != 1 {
		t.Fatalf("replay = (%q, replayed=%q, handler=%d), want the intact record",
			replay.Body.String(), replay.Header().Get(httpidem.HeaderIdempotencyReplayed), count.Load())
	}
}

// T8: a double CompleteTx in the same transaction is detected as
// ErrAlreadyCompleted, not misreported as lease loss.
func TestTxJoin_T8_DoubleCompleteTx(t *testing.T) {
	requirePG(t)
	store, db, orders := newTxHarness(t)
	ctx := context.Background()

	out, err := idemlease.Begin(ctx, store, "k", []byte("fp"), idemlease.Options{})
	if err != nil || out.Action != idemlease.Proceed {
		t.Fatalf("Begin = (%+v, %v), want Proceed", out, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+orders+" (note) VALUES ('n')"); err != nil {
		t.Fatal(err)
	}
	if leaseLost, err := store.CompleteTx(ctx, tx, "k", out.Token, []byte("p"), time.Hour); err != nil || leaseLost {
		t.Fatalf("first CompleteTx = (%v, %v), want (false, nil)", leaseLost, err)
	}
	leaseLost, err := store.CompleteTx(ctx, tx, "k", out.Token, []byte("p"), time.Hour)
	if leaseLost {
		t.Fatal("double call must not be misreported as lease loss (would trigger a rollback)")
	}
	if !errors.Is(err, pgstore.ErrAlreadyCompleted) {
		t.Fatalf("err = %v, want ErrAlreadyCompleted", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := string(storedBody(t, store, "k")); got == "" {
		t.Fatal("record must be completed after commit")
	}
	if n := orderCount(t, db, orders); n != 1 {
		t.Fatalf("orders = %d, want 1", n)
	}
}

// T10: after the record TTL expires, the same request executes (and
// commits) again — by design, the at-most-once guarantee spans the
// completed record's validity only.
func TestTxJoin_T10_RecordTTLExpiryReexecutes(t *testing.T) {
	requirePG(t)
	store, db, orders := newTxHarness(t)
	var count atomic.Int32
	h := httpidem.New(store, httpidem.RecordTTL(400*time.Millisecond), httpidem.Logger(quietLogger()))(
		canonicalHandler(t, store, db, orders, &count, func(n int32) []byte {
			return []byte(fmt.Sprintf("result-%d", n))
		}))

	if rr := do(h, "k", "b"); rr.Code != 201 {
		t.Fatalf("first = %d, want 201", rr.Code)
	}
	time.Sleep(600 * time.Millisecond) // past the record TTL

	second := do(h, "k", "b")
	if second.Code != 201 || second.Header().Get(httpidem.HeaderIdempotencyReplayed) == "true" {
		t.Fatalf("second = (%d, replayed=%q), want a fresh execution", second.Code, second.Header().Get(httpidem.HeaderIdempotencyReplayed))
	}
	if n := orderCount(t, db, orders); n != 2 {
		t.Fatalf("orders = %d, want 2 (re-execution is the documented behavior)", n)
	}
	if count.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2", count.Load())
	}
}
