package redistore_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/repenguin22/idemlease"
	"github.com/repenguin22/idemlease/httpidem/httpidemtest"
	"github.com/repenguin22/idemlease/idemleasetest"
	"github.com/repenguin22/idemlease/redistore"
)

func requireRedis(t *testing.T) {
	t.Helper()
	if os.Getenv("REDIS_ADDR") == "" {
		t.Skip("REDIS_ADDR is not set; skipping Redis conformance tests")
	}
}

var prefixSeq atomic.Int64

// newStore returns a Store isolated under a unique key prefix; the
// prefix is swept and the client closed when the (sub)test ends.
func newStore(t *testing.T) idemlease.Store {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")})
	prefix := fmt.Sprintf("idemlease-test:%d:%d:", os.Getpid(), prefixSeq.Add(1))
	t.Cleanup(func() {
		ctx := context.Background()
		iter := client.Scan(ctx, 0, prefix+"*", 100).Iterator()
		for iter.Next(ctx) {
			client.Del(ctx, iter.Val())
		}
		_ = client.Close()
	})
	return redistore.New(client, redistore.KeyPrefix(prefix))
}

func TestConformance(t *testing.T) {
	requireRedis(t)
	idemleasetest.RunStoreTests(t, newStore)
}

func TestStateMachine(t *testing.T) {
	requireRedis(t)
	idemleasetest.RunStateMachineTests(t, newStore)
}

func TestHTTP(t *testing.T) {
	requireRedis(t)
	httpidemtest.RunHTTPTests(t, newStore)
}

// TestClockSkewDoesNotCorruptExpiry pins the fix for the v1.0.0 review
// finding M1: Redis's TTL is the single expiry authority. A record
// whose informational lease_exp_ms was written by a node with a badly
// skewed clock must still read as live for exactly as long as Redis
// keeps the key, with a sane Retry-After.
func TestClockSkewDoesNotCorruptExpiry(t *testing.T) {
	requireRedis(t)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")})
	prefix := fmt.Sprintf("idemlease-skew:%d:%d:", os.Getpid(), prefixSeq.Add(1))
	redisKey := prefix + "k"
	t.Cleanup(func() {
		client.Del(ctx, redisKey)
		_ = client.Close()
	})
	store := redistore.New(client, redistore.KeyPrefix(prefix))

	// Simulate a reservation written by a node whose clock is an hour
	// behind: its absolute lease_exp_ms is already "in the past", but
	// Redis's TTL (the authority) says the lease lives for a minute.
	staleMs := strconv.FormatInt(time.Now().Add(-time.Hour).UnixMilli(), 10)
	if err := client.HSet(ctx, redisKey,
		"state", "1", "token", "tok-a", "fp", "fp", "lease_exp_ms", staleMs,
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.PExpire(ctx, redisKey, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	rec, err := store.Get(ctx, "k")
	if err != nil || rec == nil {
		t.Fatalf("Get = (%+v, %v), want the live record", rec, err)
	}
	if rec.Expired(time.Now()) {
		t.Fatalf("record reads as expired despite a live Redis TTL (LeaseExpiresAt=%v)", rec.LeaseExpiresAt)
	}
	if until := time.Until(rec.LeaseExpiresAt); until < 50*time.Second || until > 61*time.Second {
		t.Fatalf("LeaseExpiresAt = %v from now, want ~1m (PTTL-derived)", until)
	}

	out, err := idemlease.Begin(ctx, store, "k", []byte("fp"), idemlease.Options{LeaseTTL: time.Minute})
	if err != nil || out.Action != idemlease.RejectInFlight {
		t.Fatalf("Begin = (%+v, %v), want RejectInFlight", out, err)
	}
	if out.RetryAfter < 50*time.Second || out.RetryAfter > 61*time.Second {
		t.Fatalf("RetryAfter = %v, want ~1m, not a sticky near-zero value", out.RetryAfter)
	}
}
