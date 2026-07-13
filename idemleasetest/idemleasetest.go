// Package idemleasetest provides conformance test suites for
// idemlease.Store implementations and for the Begin/Finish state
// machine running on top of them.
//
// Store implementations must pass RunStoreTests; RunStateMachineTests
// additionally drives idemlease.Begin/Finish end to end against the
// store. Expiry behavior is exercised with real waits (a few hundred
// milliseconds per expiry subtest) so both suites work against
// out-of-process stores such as Redis.
//
// The package deliberately depends on the standard library only, so
// that store implementors do not inherit extra dependencies.
package idemleasetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/repenguin22/idemlease"
)

const (
	// validTTL comfortably outlives a test; subtests never wait for it.
	validTTL = time.Minute
	// shortTTL is used only for records the suite waits past. No subtest
	// asserts anything while a short lease is still valid, so slow
	// runners cannot make the suite flaky.
	shortTTL = 100 * time.Millisecond
	// timeTolerance absorbs serialization and clock granularity when
	// comparing round-tripped expiry instants.
	timeTolerance = time.Second
)

func reservedRecord(key, token string, fingerprint []byte, ttl time.Duration) idemlease.Record {
	return idemlease.Record{
		Key:            key,
		Fingerprint:    fingerprint,
		State:          idemlease.StateReserved,
		Token:          token,
		LeaseExpiresAt: time.Now().Add(ttl),
	}
}

func mustReserveNew(t *testing.T, st idemlease.Store, rec idemlease.Record) {
	t.Helper()
	existing, err := st.Reserve(context.Background(), rec)
	if err != nil || existing != nil {
		t.Fatalf("Reserve(%q) = (%+v, %v), want (nil, nil)", rec.Key, existing, err)
	}
}

func waitUntilExpired(deadline time.Time) {
	time.Sleep(time.Until(deadline) + 150*time.Millisecond)
}

func assertTimeClose(t *testing.T, name string, got, want time.Time) {
	t.Helper()
	d := got.Sub(want)
	if d < 0 {
		d = -d
	}
	if d > timeTolerance {
		t.Errorf("%s = %v, want within %v of %v", name, got, timeTolerance, want)
	}
}

// assertStaleRejected checks the contract for stale-token Complete and
// Release: either sentinel is acceptable (REQUIREMENTS §3.2).
func assertStaleRejected(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, idemlease.ErrTokenMismatch) && !errors.Is(err, idemlease.ErrNotFound) {
		t.Fatalf("err = %v, want ErrTokenMismatch or ErrNotFound", err)
	}
}

// RunStoreTests verifies that Stores produced by newStore satisfy the
// idemlease.Store semantics (REQUIREMENTS §3.2): single atomic Reserve
// with overwrite of expired records, token compare-and-set on Complete
// and Release, sentinel errors, and verbatim token persistence.
//
// newStore is called once per subtest and must return an empty store
// ready for use.
func RunStoreTests(t *testing.T, newStore func(t *testing.T) idemlease.Store) {
	ctx := context.Background()

	t.Run("ReserveOnEmpty", func(t *testing.T) {
		st := newStore(t)
		fp := []byte("fp")
		rec := reservedRecord("k", "tok-first", fp, validTTL)
		mustReserveNew(t, st, rec)

		got, err := st.Get(ctx, "k")
		if err != nil || got == nil {
			t.Fatalf("Get = (%+v, %v), want the reserved record", got, err)
		}
		if got.Key != "k" {
			t.Errorf("Key = %q, want %q", got.Key, "k")
		}
		if got.State != idemlease.StateReserved {
			t.Errorf("State = %v, want StateReserved", got.State)
		}
		if got.Token != "tok-first" {
			t.Errorf("Token = %q, want %q (stores must persist tokens verbatim)", got.Token, "tok-first")
		}
		if !bytes.Equal(got.Fingerprint, fp) {
			t.Errorf("Fingerprint = %q, want %q", got.Fingerprint, fp)
		}
		assertTimeClose(t, "LeaseExpiresAt", got.LeaseExpiresAt, rec.LeaseExpiresAt)
	})

	t.Run("ReserveConflictReturnsExisting", func(t *testing.T) {
		st := newStore(t)
		first := reservedRecord("k", "tok-first", []byte("fp-first"), validTTL)
		mustReserveNew(t, st, first)

		existing, err := st.Reserve(ctx, reservedRecord("k", "tok-second", []byte("fp-second"), validTTL))
		if !errors.Is(err, idemlease.ErrAlreadyExists) {
			t.Fatalf("err = %v, want ErrAlreadyExists", err)
		}
		if existing == nil {
			t.Fatal("existing = nil, want the first record")
		}
		if existing.Token != "tok-first" {
			t.Errorf("existing.Token = %q, want %q (verbatim persistence)", existing.Token, "tok-first")
		}
		if existing.State != idemlease.StateReserved {
			t.Errorf("existing.State = %v, want StateReserved", existing.State)
		}
		if !bytes.Equal(existing.Fingerprint, []byte("fp-first")) {
			t.Errorf("existing.Fingerprint = %q, want %q", existing.Fingerprint, "fp-first")
		}
		assertTimeClose(t, "existing.LeaseExpiresAt", existing.LeaseExpiresAt, first.LeaseExpiresAt)
		if existing.Expired(time.Now()) {
			t.Error("existing must be a live record: expired records are never returned (§3.2)")
		}

		got, err := st.Get(ctx, "k")
		if err != nil || got == nil || got.Token != "tok-first" {
			t.Fatalf("a losing Reserve must not modify the stored record: Get = (%+v, %v)", got, err)
		}
	})

	t.Run("ReserveConflictOnCompleted", func(t *testing.T) {
		st := newStore(t)
		fp := []byte("fp")
		payload := []byte("payload-1")
		mustReserveNew(t, st, reservedRecord("k", "tok-1", fp, validTTL))
		if err := st.Complete(ctx, "k", "tok-1", payload, validTTL); err != nil {
			t.Fatalf("Complete: %v", err)
		}

		existing, err := st.Reserve(ctx, reservedRecord("k", "tok-2", fp, validTTL))
		if !errors.Is(err, idemlease.ErrAlreadyExists) || existing == nil {
			t.Fatalf("Reserve = (%+v, %v), want (completed record, ErrAlreadyExists)", existing, err)
		}
		if existing.State != idemlease.StateCompleted {
			t.Errorf("existing.State = %v, want StateCompleted", existing.State)
		}
		if !bytes.Equal(existing.Payload, payload) {
			t.Errorf("existing.Payload = %q, want %q", existing.Payload, payload)
		}
		if !bytes.Equal(existing.Fingerprint, fp) {
			t.Errorf("existing.Fingerprint = %q, want %q", existing.Fingerprint, fp)
		}
		if existing.Expired(time.Now()) {
			t.Error("existing must be a live record: expired records are never returned (§3.2)")
		}
	})

	t.Run("ConcurrentReserveSingleWinner", func(t *testing.T) {
		st := newStore(t)
		const n = 100
		existings := make([]*idemlease.Record, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				existings[i], errs[i] = st.Reserve(ctx, reservedRecord("k", fmt.Sprintf("tok-%d", i), []byte("fp"), validTTL))
			}(i)
		}
		wg.Wait()

		winner := -1
		for i := 0; i < n; i++ {
			switch {
			case errs[i] == nil:
				if winner != -1 {
					t.Fatalf("multiple winners: #%d and #%d", winner, i)
				}
				winner = i
			case errors.Is(errs[i], idemlease.ErrAlreadyExists):
				if existings[i] == nil {
					t.Fatalf("#%d: ErrAlreadyExists without the existing record", i)
				}
			default:
				t.Fatalf("#%d: unexpected error %v", i, errs[i])
			}
		}
		if winner == -1 {
			t.Fatal("no Reserve succeeded")
		}
		winnerToken := fmt.Sprintf("tok-%d", winner)
		for i, ex := range existings {
			if ex != nil && ex.Token != winnerToken {
				t.Fatalf("#%d observed token %q, want the winner's %q", i, ex.Token, winnerToken)
			}
		}
		got, err := st.Get(ctx, "k")
		if err != nil || got == nil || got.Token != winnerToken {
			t.Fatalf("Get = (%+v, %v), want the winner's record", got, err)
		}
	})

	t.Run("TokenPersistedVerbatim", func(t *testing.T) {
		st := newStore(t)
		token := "tok-α/β+γ=0123456789abcdef" // stores must not normalize or re-encode
		mustReserveNew(t, st, reservedRecord("k", token, nil, validTTL))

		got, err := st.Get(ctx, "k")
		if err != nil || got == nil || got.Token != token {
			t.Fatalf("Get token = (%+v, %v), want %q verbatim", got, err, token)
		}
		existing, err := st.Reserve(ctx, reservedRecord("k", "tok-other", nil, validTTL))
		if !errors.Is(err, idemlease.ErrAlreadyExists) || existing == nil || existing.Token != token {
			t.Fatalf("existing token = (%+v, %v), want %q verbatim", existing, err, token)
		}
	})

	t.Run("ReserveAfterLeaseExpiry", func(t *testing.T) {
		st := newStore(t)
		old := reservedRecord("k", "tok-old", []byte("fp-old"), shortTTL)
		mustReserveNew(t, st, old)
		waitUntilExpired(old.LeaseExpiresAt)

		existing, err := st.Reserve(ctx, reservedRecord("k", "tok-new", []byte("fp-new"), validTTL))
		if err != nil {
			t.Fatalf("Reserve over an expired lease = %v, want success (expired records are absent)", err)
		}
		if existing != nil {
			t.Fatalf("existing = %+v, want nil (expired records must never be returned)", existing)
		}
		got, err := st.Get(ctx, "k")
		if err != nil || got == nil || got.Token != "tok-new" || got.State != idemlease.StateReserved {
			t.Fatalf("Get = (%+v, %v), want the fresh reservation", got, err)
		}
	})

	t.Run("ReserveAfterRecordTTLExpiry", func(t *testing.T) {
		st := newStore(t)
		mustReserveNew(t, st, reservedRecord("k", "tok-1", []byte("fp"), validTTL))
		if err := st.Complete(ctx, "k", "tok-1", []byte("payload"), shortTTL); err != nil {
			t.Fatalf("Complete: %v", err)
		}
		waitUntilExpired(time.Now().Add(shortTTL))

		existing, err := st.Reserve(ctx, reservedRecord("k", "tok-2", []byte("fp"), validTTL))
		if err != nil || existing != nil {
			t.Fatalf("Reserve over an expired record = (%+v, %v), want (nil, nil)", existing, err)
		}
		got, err := st.Get(ctx, "k")
		if err != nil || got == nil || got.Token != "tok-2" || got.State != idemlease.StateReserved {
			t.Fatalf("Get = (%+v, %v), want the fresh reservation", got, err)
		}
	})

	// ConcurrentReserveOverExpired catches GET-then-SET implementations:
	// overwriting an expired record must be as atomic as first Reserve.
	t.Run("ConcurrentReserveOverExpired", func(t *testing.T) {
		st := newStore(t)
		old := reservedRecord("k", "tok-old", []byte("fp"), shortTTL)
		mustReserveNew(t, st, old)
		waitUntilExpired(old.LeaseExpiresAt)

		const n = 100
		existings := make([]*idemlease.Record, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				existings[i], errs[i] = st.Reserve(ctx, reservedRecord("k", fmt.Sprintf("tok-%d", i), []byte("fp"), validTTL))
			}(i)
		}
		wg.Wait()

		winner := -1
		for i := 0; i < n; i++ {
			switch {
			case errs[i] == nil:
				if winner != -1 {
					t.Fatalf("multiple winners over an expired record: #%d and #%d (non-atomic overwrite?)", winner, i)
				}
				winner = i
			case errors.Is(errs[i], idemlease.ErrAlreadyExists):
				if existings[i] == nil {
					t.Fatalf("#%d: ErrAlreadyExists without the existing record", i)
				}
				if existings[i].Token == "tok-old" {
					t.Fatalf("#%d: observed the expired record as existing", i)
				}
			default:
				t.Fatalf("#%d: unexpected error %v", i, errs[i])
			}
		}
		if winner == -1 {
			t.Fatal("no Reserve succeeded over the expired record")
		}
	})

	t.Run("CompleteStoresPayload", func(t *testing.T) {
		st := newStore(t)
		payload := []byte(`{"result":"ok"}`)
		mustReserveNew(t, st, reservedRecord("k", "tok-1", []byte("fp"), validTTL))

		before := time.Now()
		if err := st.Complete(ctx, "k", "tok-1", payload, validTTL); err != nil {
			t.Fatalf("Complete: %v", err)
		}
		got, err := st.Get(ctx, "k")
		if err != nil || got == nil {
			t.Fatalf("Get = (%+v, %v), want the completed record", got, err)
		}
		if got.State != idemlease.StateCompleted {
			t.Errorf("State = %v, want StateCompleted", got.State)
		}
		if !bytes.Equal(got.Payload, payload) {
			t.Errorf("Payload = %q, want %q", got.Payload, payload)
		}
		assertTimeClose(t, "RecordExpiresAt", got.RecordExpiresAt, before.Add(validTTL))
	})

	t.Run("CompleteWrongToken", func(t *testing.T) {
		st := newStore(t)
		mustReserveNew(t, st, reservedRecord("k", "tok-1", []byte("fp"), validTTL))

		if err := st.Complete(ctx, "k", "tok-evil", []byte("payload"), validTTL); !errors.Is(err, idemlease.ErrTokenMismatch) {
			t.Fatalf("err = %v, want ErrTokenMismatch", err)
		}
		got, err := st.Get(ctx, "k")
		if err != nil || got == nil || got.State != idemlease.StateReserved || got.Token != "tok-1" {
			t.Fatalf("a rejected Complete must not modify the record: Get = (%+v, %v)", got, err)
		}
	})

	t.Run("CompleteMissingKey", func(t *testing.T) {
		st := newStore(t)
		if err := st.Complete(ctx, "absent", "tok-1", []byte("payload"), validTTL); !errors.Is(err, idemlease.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})

	t.Run("CompleteStaleTokenAfterExpiry", func(t *testing.T) {
		st := newStore(t)
		old := reservedRecord("k", "tok-old", []byte("fp"), shortTTL)
		mustReserveNew(t, st, old)
		waitUntilExpired(old.LeaseExpiresAt)

		assertStaleRejected(t, st.Complete(ctx, "k", "tok-old", []byte("payload"), validTTL))
		// The stale Complete must not have resurrected the record.
		existing, err := st.Reserve(ctx, reservedRecord("k", "tok-new", []byte("fp"), validTTL))
		if err != nil || existing != nil {
			t.Fatalf("Reserve after stale Complete = (%+v, %v), want (nil, nil)", existing, err)
		}
	})

	t.Run("CompleteStaleTokenAfterReclaim", func(t *testing.T) {
		st := newStore(t)
		old := reservedRecord("k", "tok-a", []byte("fp"), shortTTL)
		mustReserveNew(t, st, old)
		waitUntilExpired(old.LeaseExpiresAt)
		mustReserveNew(t, st, reservedRecord("k", "tok-b", []byte("fp"), validTTL))

		assertStaleRejected(t, st.Complete(ctx, "k", "tok-a", []byte("payload-a"), validTTL))
		got, err := st.Get(ctx, "k")
		if err != nil || got == nil || got.State != idemlease.StateReserved || got.Token != "tok-b" {
			t.Fatalf("the stale Complete must not disturb the new owner: Get = (%+v, %v)", got, err)
		}

		if err := st.Complete(ctx, "k", "tok-b", []byte("payload-b"), validTTL); err != nil {
			t.Fatalf("the new owner's Complete must succeed: %v", err)
		}
		got, err = st.Get(ctx, "k")
		if err != nil || got == nil || !bytes.Equal(got.Payload, []byte("payload-b")) {
			t.Fatalf("Get = (%+v, %v), want the new owner's payload", got, err)
		}
	})

	t.Run("ReleaseDeletesReservation", func(t *testing.T) {
		st := newStore(t)
		mustReserveNew(t, st, reservedRecord("k", "tok-1", []byte("fp"), validTTL))

		if err := st.Release(ctx, "k", "tok-1"); err != nil {
			t.Fatalf("Release: %v", err)
		}
		got, err := st.Get(ctx, "k")
		if err != nil || got != nil {
			t.Fatalf("Get after Release = (%+v, %v), want (nil, nil)", got, err)
		}
		mustReserveNew(t, st, reservedRecord("k", "tok-2", []byte("fp"), validTTL))
	})

	t.Run("ReleaseWrongToken", func(t *testing.T) {
		st := newStore(t)
		mustReserveNew(t, st, reservedRecord("k", "tok-1", []byte("fp"), validTTL))

		if err := st.Release(ctx, "k", "tok-evil"); !errors.Is(err, idemlease.ErrTokenMismatch) {
			t.Fatalf("err = %v, want ErrTokenMismatch", err)
		}
		got, err := st.Get(ctx, "k")
		if err != nil || got == nil || got.Token != "tok-1" {
			t.Fatalf("a rejected Release must not modify the record: Get = (%+v, %v)", got, err)
		}
	})

	t.Run("ReleaseMissingKey", func(t *testing.T) {
		st := newStore(t)
		if err := st.Release(ctx, "absent", "tok-1"); !errors.Is(err, idemlease.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})

	t.Run("ReleaseStaleTokenAfterReclaim", func(t *testing.T) {
		st := newStore(t)
		old := reservedRecord("k", "tok-a", []byte("fp"), shortTTL)
		mustReserveNew(t, st, old)
		waitUntilExpired(old.LeaseExpiresAt)
		mustReserveNew(t, st, reservedRecord("k", "tok-b", []byte("fp"), validTTL))

		assertStaleRejected(t, st.Release(ctx, "k", "tok-a"))
		got, err := st.Get(ctx, "k")
		if err != nil || got == nil || got.Token != "tok-b" {
			t.Fatalf("the stale Release must not disturb the new owner: Get = (%+v, %v)", got, err)
		}
	})

	t.Run("GetMissingKey", func(t *testing.T) {
		st := newStore(t)
		got, err := st.Get(ctx, "absent")
		if err != nil || got != nil {
			t.Fatalf("Get = (%+v, %v), want (nil, nil)", got, err)
		}
	})

	t.Run("GetExpiredRecord", func(t *testing.T) {
		st := newStore(t)
		old := reservedRecord("k", "tok-old", []byte("fp"), shortTTL)
		mustReserveNew(t, st, old)
		waitUntilExpired(old.LeaseExpiresAt)

		got, err := st.Get(ctx, "k")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		// (nil, nil) is the documented behavior; returning the expired
		// record is tolerated as long as it reads as expired.
		if got != nil && !got.Expired(time.Now()) {
			t.Fatalf("Get returned a live record %+v, want nil or an expired record", got)
		}
	})
}
