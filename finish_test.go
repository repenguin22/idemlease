package idemlease_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/repenguin22/idemlease"
)

// TestFinishPersist walks the happy path: Proceed → Finish(Persist) →
// Replay, including the fingerprint-mismatch rejection afterwards.
func TestFinishPersist(t *testing.T) {
	ctx := context.Background()
	st := newFakeStore()
	fp := []byte("fp")
	payload := []byte(`{"order":"ok"}`)

	out, err := idemlease.Begin(ctx, st, "k", fp, testOpts)
	if err != nil || out.Action != idemlease.Proceed {
		t.Fatalf("Begin = (%+v, %v), want Proceed", out, err)
	}

	before := time.Now()
	leaseLost, err := idemlease.Finish(ctx, st, "k", out.Token, idemlease.Persist, payload, testOpts)
	after := time.Now()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if leaseLost {
		t.Fatal("leaseLost = true, want false")
	}

	rec, ok := st.get("k")
	if !ok || rec.State != idemlease.StateCompleted {
		t.Fatalf("stored record = (%+v, %v), want completed", rec, ok)
	}
	if !bytes.Equal(rec.Payload, payload) {
		t.Errorf("stored payload = %q, want %q", rec.Payload, payload)
	}
	lo, hi := before.Add(testOpts.RecordTTL), after.Add(testOpts.RecordTTL)
	if rec.RecordExpiresAt.Before(lo) || rec.RecordExpiresAt.After(hi) {
		t.Errorf("RecordExpiresAt = %v, want within [%v, %v] (o.RecordTTL must be used)", rec.RecordExpiresAt, lo, hi)
	}

	replay, err := idemlease.Begin(ctx, st, "k", fp, testOpts)
	if err != nil || replay.Action != idemlease.Replay || !bytes.Equal(replay.Payload, payload) {
		t.Fatalf("Begin after Persist = (%+v, %v), want Replay with stored payload", replay, err)
	}

	mismatch, err := idemlease.Begin(ctx, st, "k", []byte("other-fp"), testOpts)
	if err != nil || mismatch.Action != idemlease.RejectFingerprintMismatch {
		t.Fatalf("Begin with other fingerprint = (%+v, %v), want RejectFingerprintMismatch", mismatch, err)
	}
}

// TestFinishDiscard verifies Release: the record is gone and the key is
// immediately re-executable.
func TestFinishDiscard(t *testing.T) {
	ctx := context.Background()
	st := newFakeStore()
	fp := []byte("fp")

	out, err := idemlease.Begin(ctx, st, "k", fp, testOpts)
	if err != nil || out.Action != idemlease.Proceed {
		t.Fatalf("Begin = (%+v, %v), want Proceed", out, err)
	}

	leaseLost, err := idemlease.Finish(ctx, st, "k", out.Token, idemlease.Discard, nil, testOpts)
	if err != nil || leaseLost {
		t.Fatalf("Finish(Discard) = (%v, %v), want (false, nil)", leaseLost, err)
	}
	if _, ok := st.get("k"); ok {
		t.Fatal("record must be deleted after Discard")
	}

	again, err := idemlease.Begin(ctx, st, "k", fp, testOpts)
	if err != nil || again.Action != idemlease.Proceed {
		t.Fatalf("Begin after Discard = (%+v, %v), want Proceed", again, err)
	}
	if again.Token == out.Token {
		t.Error("re-execution must get a fresh token")
	}
}

// TestFinishLeaseLost verifies the normalization contract (§2.2, §3.3):
// ErrTokenMismatch / ErrNotFound → (true, nil), wrapped or not.
func TestFinishLeaseLost(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrTokenMismatch", idemlease.ErrTokenMismatch},
		{"ErrNotFound", idemlease.ErrNotFound},
		{"wrapped ErrTokenMismatch", fmt.Errorf("redis: %w", idemlease.ErrTokenMismatch)},
		{"wrapped ErrNotFound", fmt.Errorf("redis: %w", idemlease.ErrNotFound)},
	}
	for _, tt := range tests {
		for _, d := range []idemlease.Decision{idemlease.Persist, idemlease.Discard} {
			t.Run(fmt.Sprintf("%s/%v", tt.name, d), func(t *testing.T) {
				st := newFakeStore()
				st.completeErr = tt.err
				st.releaseErr = tt.err
				leaseLost, err := idemlease.Finish(context.Background(), st, "k", "token", d, nil, testOpts)
				if err != nil {
					t.Fatalf("err = %v, want nil (normalized)", err)
				}
				if !leaseLost {
					t.Fatal("leaseLost = false, want true")
				}
			})
		}
	}
}

// TestFinishStoreFailure verifies that other store errors pass through
// unchanged with leaseLost=false.
func TestFinishStoreFailure(t *testing.T) {
	boom := errors.New("store down")
	for _, d := range []idemlease.Decision{idemlease.Persist, idemlease.Discard} {
		t.Run(fmt.Sprintf("%v", d), func(t *testing.T) {
			st := newFakeStore()
			st.completeErr = boom
			st.releaseErr = boom
			leaseLost, err := idemlease.Finish(context.Background(), st, "k", "token", d, nil, testOpts)
			if !errors.Is(err, boom) {
				t.Fatalf("err = %v, want the injected store error", err)
			}
			if leaseLost {
				t.Fatal("leaseLost = true, want false on store failure")
			}
		})
	}
}

// TestFinishInvalidDecision verifies that an unknown Decision is a
// caller bug reported as an error, without touching the store.
func TestFinishInvalidDecision(t *testing.T) {
	st := newFakeStore()
	leaseLost, err := idemlease.Finish(context.Background(), st, "k", "token", idemlease.Decision(99), nil, testOpts)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if leaseLost {
		t.Fatal("leaseLost = true, want false")
	}
}
