package idemlease_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/repenguin22/idemlease"
)

// TestProceedWithoutFinish pins the §2.2 contract for callers that never
// call Finish: the key stays rejected in-flight until the lease expires,
// then becomes re-executable. The core never reclaims on its own.
func TestProceedWithoutFinish(t *testing.T) {
	ctx := context.Background()
	st := newFakeStore()
	fp := []byte("fp")

	first, err := idemlease.Begin(ctx, st, "k", fp, testOpts)
	if err != nil || first.Action != idemlease.Proceed {
		t.Fatalf("Begin = (%+v, %v), want Proceed", first, err)
	}
	// The caller "forgets" Finish. Retries keep being rejected while the
	// lease is valid, regardless of fingerprint (decision 7).
	retry, err := idemlease.Begin(ctx, st, "k", fp, testOpts)
	if err != nil || retry.Action != idemlease.RejectInFlight {
		t.Fatalf("Begin during lease = (%+v, %v), want RejectInFlight", retry, err)
	}
	if retry.RetryAfter <= 0 || retry.RetryAfter > testOpts.LeaseTTL {
		t.Errorf("RetryAfter = %v, want within (0, %v]", retry.RetryAfter, testOpts.LeaseTTL)
	}
	otherFP, err := idemlease.Begin(ctx, st, "k", []byte("other-fp"), testOpts)
	if err != nil || otherFP.Action != idemlease.RejectInFlight {
		t.Fatalf("Begin with other fingerprint = (%+v, %v), want RejectInFlight", otherFP, err)
	}

	// Lease expiry is the only reclamation path.
	st.expireLease("k")
	again, err := idemlease.Begin(ctx, st, "k", fp, testOpts)
	if err != nil || again.Action != idemlease.Proceed {
		t.Fatalf("Begin after lease expiry = (%+v, %v), want Proceed", again, err)
	}
	if again.Token == first.Token {
		t.Error("re-execution must hold a fresh token")
	}
}

// TestLeaseExpirySurvivorExecution pins §3.3: the original execution
// outlives its lease while a new execution takes over the key. The
// original's Finish reports leaseLost and must not disturb the new
// owner; only the new execution's result is persisted and replayed.
func TestLeaseExpirySurvivorExecution(t *testing.T) {
	ctx := context.Background()
	st := newFakeStore()
	fp := []byte("fp")
	payloadA := []byte("result-of-A")
	payloadB := []byte("result-of-B")

	// Execution A wins the lease, then its lease expires mid-flight.
	a, err := idemlease.Begin(ctx, st, "k", fp, testOpts)
	if err != nil || a.Action != idemlease.Proceed {
		t.Fatalf("Begin(A) = (%+v, %v), want Proceed", a, err)
	}
	st.expireLease("k")

	// Execution B re-reserves the key.
	b, err := idemlease.Begin(ctx, st, "k", fp, testOpts)
	if err != nil || b.Action != idemlease.Proceed {
		t.Fatalf("Begin(B) = (%+v, %v), want Proceed", b, err)
	}
	if b.Token == a.Token {
		t.Fatal("B must hold a distinct token")
	}

	// A finishes late: lease lost, no error, and B's reservation is untouched.
	for _, d := range []idemlease.Decision{idemlease.Persist, idemlease.Discard} {
		leaseLost, err := idemlease.Finish(ctx, st, "k", a.Token, d, payloadA, testOpts)
		if err != nil || !leaseLost {
			t.Fatalf("Finish(A, %v) = (%v, %v), want (true, nil)", d, leaseLost, err)
		}
	}
	rec, ok := st.get("k")
	if !ok || rec.State != idemlease.StateReserved || rec.Token != b.Token {
		t.Fatalf("after A's late Finish the store must still hold B's reservation, got (%+v, %v)", rec, ok)
	}

	// B persists normally; only B's result is stored and replayed.
	leaseLost, err := idemlease.Finish(ctx, st, "k", b.Token, idemlease.Persist, payloadB, testOpts)
	if err != nil || leaseLost {
		t.Fatalf("Finish(B) = (%v, %v), want (false, nil)", leaseLost, err)
	}
	replay, err := idemlease.Begin(ctx, st, "k", fp, testOpts)
	if err != nil || replay.Action != idemlease.Replay {
		t.Fatalf("Begin after B = (%+v, %v), want Replay", replay, err)
	}
	if !bytes.Equal(replay.Payload, payloadB) {
		t.Fatalf("replayed payload = %q, want B's result %q", replay.Payload, payloadB)
	}
}

// TestRecordTTLExpiryAllowsReexecution verifies that a completed record
// past its record TTL is treated as absent (§3.1).
func TestRecordTTLExpiryAllowsReexecution(t *testing.T) {
	ctx := context.Background()
	st := newFakeStore()
	fp := []byte("fp")

	out, err := idemlease.Begin(ctx, st, "k", fp, testOpts)
	if err != nil || out.Action != idemlease.Proceed {
		t.Fatalf("Begin = (%+v, %v), want Proceed", out, err)
	}
	if _, err := idemlease.Finish(ctx, st, "k", out.Token, idemlease.Persist, []byte("v1"), testOpts); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	st.expireRecord("k")
	again, err := idemlease.Begin(ctx, st, "k", fp, testOpts)
	if err != nil || again.Action != idemlease.Proceed {
		t.Fatalf("Begin after record TTL expiry = (%+v, %v), want Proceed", again, err)
	}
}

// TestOptionsDefaults verifies that zero Options fall back to
// DefaultLeaseTTL / DefaultRecordTTL.
func TestOptionsDefaults(t *testing.T) {
	ctx := context.Background()
	st := newFakeStore()

	before := time.Now()
	out, err := idemlease.Begin(ctx, st, "k", nil, idemlease.Options{})
	after := time.Now()
	if err != nil || out.Action != idemlease.Proceed {
		t.Fatalf("Begin = (%+v, %v), want Proceed", out, err)
	}
	rec, _ := st.get("k")
	lo, hi := before.Add(idemlease.DefaultLeaseTTL), after.Add(idemlease.DefaultLeaseTTL)
	if rec.LeaseExpiresAt.Before(lo) || rec.LeaseExpiresAt.After(hi) {
		t.Errorf("LeaseExpiresAt = %v, want within [%v, %v] (DefaultLeaseTTL)", rec.LeaseExpiresAt, lo, hi)
	}

	before = time.Now()
	if _, err := idemlease.Finish(ctx, st, "k", out.Token, idemlease.Persist, nil, idemlease.Options{}); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	after = time.Now()
	rec, _ = st.get("k")
	lo, hi = before.Add(idemlease.DefaultRecordTTL), after.Add(idemlease.DefaultRecordTTL)
	if rec.RecordExpiresAt.Before(lo) || rec.RecordExpiresAt.After(hi) {
		t.Errorf("RecordExpiresAt = %v, want within [%v, %v] (DefaultRecordTTL)", rec.RecordExpiresAt, lo, hi)
	}
}
