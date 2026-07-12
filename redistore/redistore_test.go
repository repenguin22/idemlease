package redistore_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

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
