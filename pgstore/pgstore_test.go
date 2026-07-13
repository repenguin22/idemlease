package pgstore_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/repenguin22/idemlease"
	"github.com/repenguin22/idemlease/httpidem/httpidemtest"
	"github.com/repenguin22/idemlease/idemleasetest"
	"github.com/repenguin22/idemlease/pgstore"
)

func requirePG(t *testing.T) {
	t.Helper()
	if os.Getenv("PG_DSN") == "" {
		t.Skip("PG_DSN is not set; skipping PostgreSQL conformance tests")
	}
}

var tableSeq atomic.Int64

// newStore returns a Store backed by a freshly-created, uniquely-named
// table that is dropped (and the connection closed) when the (sub)test
// ends.
func newStore(t *testing.T) idemlease.Store {
	t.Helper()
	db, err := sql.Open("pgx", os.Getenv("PG_DSN"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	table := fmt.Sprintf("idemlease_test_%d_%d", os.Getpid(), tableSeq.Add(1))
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, pgstore.Schema(table)); err != nil {
		_ = db.Close()
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table)
		_ = db.Close()
	})
	return pgstore.New(db, pgstore.Table(table))
}

func TestConformance(t *testing.T) {
	requirePG(t)
	idemleasetest.RunStoreTests(t, newStore)
}

func TestStateMachine(t *testing.T) {
	requirePG(t)
	idemleasetest.RunStateMachineTests(t, newStore)
}

func TestHTTP(t *testing.T) {
	requirePG(t)
	httpidemtest.RunHTTPTests(t, newStore)
}

// TestSweep verifies that Sweep physically removes only logically
// expired rows and leaves live ones untouched.
func TestSweep(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	db, err := sql.Open("pgx", os.Getenv("PG_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	table := fmt.Sprintf("idemlease_sweep_%d_%d", os.Getpid(), tableSeq.Add(1))
	if _, err := db.ExecContext(ctx, pgstore.Schema(table)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table)
		_ = db.Close()
	})
	store := pgstore.New(db, pgstore.Table(table))

	// A live reservation and a short-lived one that we let expire.
	if _, err := store.Reserve(ctx, idemlease.Record{
		Key: "live", State: idemlease.StateReserved, Token: "t1",
		LeaseExpiresAt: time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Reserve(ctx, idemlease.Record{
		Key: "dead", State: idemlease.StateReserved, Token: "t2",
		LeaseExpiresAt: time.Now().Add(120 * time.Millisecond),
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	deleted, err := store.Sweep(ctx, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("Sweep deleted %d rows, want 1 (only the expired one)", deleted)
	}

	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("%d rows remain, want 1 (the live reservation)", count)
	}
	if rec, err := store.Get(ctx, "live"); err != nil || rec == nil {
		t.Fatalf("live reservation must survive Sweep: Get = (%+v, %v)", rec, err)
	}
}
