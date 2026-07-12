package idemleasetest

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/repenguin22/idemlease"
)

func mustProceed(t *testing.T, st idemlease.Store, key string, fingerprint []byte, o idemlease.Options) idemlease.Outcome {
	t.Helper()
	out, err := idemlease.Begin(context.Background(), st, key, fingerprint, o)
	if err != nil || out.Action != idemlease.Proceed {
		t.Fatalf("Begin(%q) = (%+v, %v), want Proceed", key, out, err)
	}
	return out
}

// RunStateMachineTests drives idemlease.Begin and idemlease.Finish end
// to end against Stores produced by newStore, covering the decision
// table (REQUIREMENTS §3.1), the Proceed-without-Finish contract
// (§2.2), and lease expiry racing a surviving execution (§3.3).
//
// newStore is called once per subtest and must return an empty store
// ready for use.
func RunStateMachineTests(t *testing.T, newStore func(t *testing.T) idemlease.Store) {
	ctx := context.Background()
	opts := idemlease.Options{LeaseTTL: validTTL, RecordTTL: validTTL}
	shortLease := idemlease.Options{LeaseTTL: shortTTL, RecordTTL: validTTL}

	t.Run("ProceedPersistReplay", func(t *testing.T) {
		st := newStore(t)
		fp := []byte("fp")
		payload := []byte(`{"result":"ok"}`)

		out := mustProceed(t, st, "k", fp, opts)
		leaseLost, err := idemlease.Finish(ctx, st, "k", out.Token, idemlease.Persist, payload, opts)
		if err != nil || leaseLost {
			t.Fatalf("Finish = (%v, %v), want (false, nil)", leaseLost, err)
		}

		replay, err := idemlease.Begin(ctx, st, "k", fp, opts)
		if err != nil || replay.Action != idemlease.Replay {
			t.Fatalf("Begin after Persist = (%+v, %v), want Replay", replay, err)
		}
		if !bytes.Equal(replay.Payload, payload) {
			t.Errorf("replayed payload = %q, want %q", replay.Payload, payload)
		}

		mismatch, err := idemlease.Begin(ctx, st, "k", []byte("other-fp"), opts)
		if err != nil || mismatch.Action != idemlease.RejectFingerprintMismatch {
			t.Fatalf("Begin with another fingerprint = (%+v, %v), want RejectFingerprintMismatch", mismatch, err)
		}
	})

	t.Run("InFlightRejection", func(t *testing.T) {
		st := newStore(t)
		fp := []byte("fp")
		mustProceed(t, st, "k", fp, opts)

		same, err := idemlease.Begin(ctx, st, "k", fp, opts)
		if err != nil || same.Action != idemlease.RejectInFlight {
			t.Fatalf("Begin during lease = (%+v, %v), want RejectInFlight", same, err)
		}
		if same.RetryAfter <= 0 || same.RetryAfter > validTTL {
			t.Errorf("RetryAfter = %v, want within (0, %v]", same.RetryAfter, validTTL)
		}
		// Decision 7: while reserved, even a different fingerprint waits.
		other, err := idemlease.Begin(ctx, st, "k", []byte("other-fp"), opts)
		if err != nil || other.Action != idemlease.RejectInFlight {
			t.Fatalf("Begin with another fingerprint = (%+v, %v), want RejectInFlight", other, err)
		}
	})

	t.Run("DiscardAllowsReexecution", func(t *testing.T) {
		st := newStore(t)
		fp := []byte("fp")

		out := mustProceed(t, st, "k", fp, opts)
		leaseLost, err := idemlease.Finish(ctx, st, "k", out.Token, idemlease.Discard, nil, opts)
		if err != nil || leaseLost {
			t.Fatalf("Finish(Discard) = (%v, %v), want (false, nil)", leaseLost, err)
		}
		again := mustProceed(t, st, "k", fp, opts)
		if again.Token == out.Token {
			t.Error("re-execution must hold a fresh token")
		}
	})

	t.Run("ProceedWithoutFinish", func(t *testing.T) {
		st := newStore(t)
		fp := []byte("fp")

		// The caller "forgets" Finish; lease expiry is the only
		// reclamation path. (In-flight rejection during a valid lease is
		// covered by InFlightRejection with a long lease.)
		first := mustProceed(t, st, "k", fp, shortLease)
		waitUntilExpired(time.Now().Add(shortTTL))

		again := mustProceed(t, st, "k", fp, opts)
		if again.Token == first.Token {
			t.Error("re-execution after lease expiry must hold a fresh token")
		}
	})

	t.Run("LeaseExpirySurvivor", func(t *testing.T) {
		st := newStore(t)
		fp := []byte("fp")
		payloadA := []byte("result-of-A")
		payloadB := []byte("result-of-B")

		a := mustProceed(t, st, "k", fp, shortLease)
		waitUntilExpired(time.Now().Add(shortTTL))
		b := mustProceed(t, st, "k", fp, opts)
		if b.Token == a.Token {
			t.Fatal("B must hold a distinct token")
		}

		// A survives its lease and finishes late: lease lost, no error,
		// and B's reservation is untouched.
		for _, d := range []idemlease.Decision{idemlease.Persist, idemlease.Discard} {
			leaseLost, err := idemlease.Finish(ctx, st, "k", a.Token, d, payloadA, opts)
			if err != nil || !leaseLost {
				t.Fatalf("Finish(A, %v) = (%v, %v), want (true, nil)", d, leaseLost, err)
			}
		}

		leaseLost, err := idemlease.Finish(ctx, st, "k", b.Token, idemlease.Persist, payloadB, opts)
		if err != nil || leaseLost {
			t.Fatalf("Finish(B) = (%v, %v), want (false, nil)", leaseLost, err)
		}
		replay, err := idemlease.Begin(ctx, st, "k", fp, opts)
		if err != nil || replay.Action != idemlease.Replay || !bytes.Equal(replay.Payload, payloadB) {
			t.Fatalf("Begin after B = (%+v, %v), want Replay with B's payload only", replay, err)
		}
	})

	t.Run("RecordTTLExpiryAllowsReexecution", func(t *testing.T) {
		st := newStore(t)
		fp := []byte("fp")
		shortRecord := idemlease.Options{LeaseTTL: validTTL, RecordTTL: shortTTL}

		out := mustProceed(t, st, "k", fp, shortRecord)
		if _, err := idemlease.Finish(ctx, st, "k", out.Token, idemlease.Persist, []byte("v1"), shortRecord); err != nil {
			t.Fatalf("Finish: %v", err)
		}
		waitUntilExpired(time.Now().Add(shortTTL))

		mustProceed(t, st, "k", fp, opts)
	})

	t.Run("ConcurrentBeginSingleWinner", func(t *testing.T) {
		st := newStore(t)
		fp := []byte("fp")
		const n = 100

		actions := make([]idemlease.Action, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				out, err := idemlease.Begin(ctx, st, "k", fp, opts)
				actions[i], errs[i] = out.Action, err
			}(i)
		}
		wg.Wait()

		var proceeds, inflights int
		for i := 0; i < n; i++ {
			if errs[i] != nil {
				t.Fatalf("Begin #%d: %v", i, errs[i])
			}
			switch actions[i] {
			case idemlease.Proceed:
				proceeds++
			case idemlease.RejectInFlight:
				inflights++
			default:
				t.Fatalf("Begin #%d: unexpected action %v", i, actions[i])
			}
		}
		if proceeds != 1 || inflights != n-1 {
			t.Fatalf("proceeds = %d, inflights = %d; want 1 and %d", proceeds, inflights, n-1)
		}
	})
}
