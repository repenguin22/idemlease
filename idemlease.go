package idemlease

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// Action tells the caller how to proceed after Begin.
type Action int

const (
	// Proceed means no valid record existed: the caller now holds the
	// lease. Execute the operation, then call Finish with Outcome.Token.
	Proceed Action = iota
	// Replay means a completed record with a matching fingerprint
	// exists. Serve Outcome.Payload instead of executing.
	Replay
	// RejectInFlight means another execution holds a valid lease.
	// Outcome.RetryAfter carries the remaining lease.
	RejectInFlight
	// RejectFingerprintMismatch means a completed record exists but was
	// created by a request with a different fingerprint.
	RejectFingerprintMismatch
)

// Outcome is the result of Begin. Only the fields relevant to Action are
// set; on a non-nil error from Begin the Outcome is meaningless.
type Outcome struct {
	Action     Action
	Token      string        // set on Proceed; pass to Finish
	Payload    []byte        // set on Replay; the stored payload
	RetryAfter time.Duration // set on RejectInFlight; remaining lease (>= 0)
}

// Decision selects the trailing state transition performed by Finish.
type Decision int

const (
	// Persist stores the payload for replay (Store.Complete).
	Persist Decision = iota
	// Discard drops the reservation and allows re-execution
	// (Store.Release). It does not guarantee that no side effects
	// happened; see the guarantee statement in the README.
	Discard
)

// Defaults applied when the corresponding Options field is zero or
// negative.
const (
	DefaultLeaseTTL  = 30 * time.Second
	DefaultRecordTTL = 24 * time.Hour
)

// Options carries the TTLs for one Begin/Finish pair. Callers should
// pass the same Options value to both calls.
type Options struct {
	// LeaseTTL bounds a single in-flight execution. While the lease is
	// valid, concurrent executions of the same key are rejected; after
	// it expires the key becomes re-executable.
	LeaseTTL time.Duration
	// RecordTTL bounds how long a completed payload is replayed. It is
	// passed to Store.Complete by Finish when the decision is Persist.
	RecordTTL time.Duration
}

func (o Options) leaseTTL() time.Duration {
	if o.LeaseTTL > 0 {
		return o.LeaseTTL
	}
	return DefaultLeaseTTL
}

func (o Options) recordTTL() time.Duration {
	if o.RecordTTL > 0 {
		return o.RecordTTL
	}
	return DefaultRecordTTL
}

// Begin inspects the record stored under key and performs the leading
// state transition in a single Reserve call:
//
//	no record, or expired record only → reserve the key and Proceed
//	valid reserved (any fingerprint)  → RejectInFlight + remaining lease
//	valid completed, matching print   → Replay + stored payload
//	valid completed, different print  → RejectFingerprintMismatch
//
// The reservation token is generated here (crypto/rand, 128 bits) and
// carried to the Store on the Record; stores never generate or alter it.
//
// On Proceed the caller must execute the operation and then call Finish
// with Outcome.Token. If Finish is never called — a caller bug — the
// record stays reserved until the lease expires: the key keeps being
// rejected in-flight until then and becomes re-executable afterwards.
// The core never reclaims reservations on its own.
//
// A non-nil error means infrastructure failed (the Store, or in the
// extreme case token generation) before any idempotency decision was
// made; interpreting it as fail-open or fail-closed is the caller's
// responsibility.
//
// Begin also defends against stores that break the §3.2 contract:
// receiving an already-expired record as existing is retried — the
// record may have expired legitimately in the instants after the
// store's atomic check — and reported as a store bug if it persists.
func Begin(ctx context.Context, s Store, key string, fingerprint []byte, o Options) (Outcome, error) {
	token, err := newToken()
	if err != nil {
		return Outcome{}, err
	}
	const maxAttempts = 3
	for attempt := 1; ; attempt++ {
		existing, err := s.Reserve(ctx, Record{
			Key:            key,
			Fingerprint:    bytes.Clone(fingerprint),
			State:          StateReserved,
			Token:          token,
			LeaseExpiresAt: time.Now().Add(o.leaseTTL()),
		})
		if err == nil {
			return Outcome{Action: Proceed, Token: token}, nil
		}
		if !errors.Is(err, ErrAlreadyExists) {
			return Outcome{}, err
		}
		if existing == nil {
			return Outcome{}, fmt.Errorf("idemlease: store returned ErrAlreadyExists without the existing record")
		}
		now := time.Now()
		if existing.Expired(now) {
			if attempt < maxAttempts {
				continue // it just expired; the next Reserve should claim it
			}
			return Outcome{}, fmt.Errorf("idemlease: store keeps returning an expired record as existing (contract violation)")
		}
		switch existing.State {
		case StateReserved:
			return Outcome{Action: RejectInFlight, RetryAfter: existing.LeaseExpiresAt.Sub(now)}, nil
		case StateCompleted:
			if bytes.Equal(existing.Fingerprint, fingerprint) {
				return Outcome{Action: Replay, Payload: existing.Payload}, nil
			}
			return Outcome{Action: RejectFingerprintMismatch}, nil
		default:
			return Outcome{}, fmt.Errorf("idemlease: store returned existing record with invalid state %d", existing.State)
		}
	}
}

// Finish performs the trailing state transition for a Proceed outcome:
// Persist stores payload via Complete using o.RecordTTL, Discard drops
// the reservation via Release (payload is ignored).
//
// ErrTokenMismatch and ErrNotFound from the store are normalized to
// (leaseLost=true, nil): the lease was lost to lease expiry or another
// execution, so the caller's result was not persisted and will not be
// replayed. Any other error is returned as-is.
func Finish(ctx context.Context, s Store, key, token string, d Decision, payload []byte, o Options) (leaseLost bool, err error) {
	switch d {
	case Persist:
		err = s.Complete(ctx, key, token, payload, o.recordTTL())
	case Discard:
		err = s.Release(ctx, key, token)
	default:
		return false, fmt.Errorf("idemlease: invalid decision %d", d)
	}
	if err == nil {
		return false, nil
	}
	if errors.Is(err, ErrTokenMismatch) || errors.Is(err, ErrNotFound) {
		return true, nil
	}
	return false, err
}

// newToken returns a 128-bit random reservation token in hex.
func newToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("idemlease: generate reservation token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
