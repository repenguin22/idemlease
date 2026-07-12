package idemlease

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors that Store implementations must return (wrapped or not)
// so that callers can match them with errors.Is.
var (
	// ErrAlreadyExists is returned by Reserve, together with the existing
	// record, when a valid (non-expired) record already holds the key.
	ErrAlreadyExists = errors.New("idemlease: record already exists")

	// ErrTokenMismatch is returned by Complete and Release when a record
	// exists for the key but is held under a different reservation token.
	ErrTokenMismatch = errors.New("idemlease: reservation token mismatch")

	// ErrNotFound is returned by Complete and Release when no record
	// exists for the key (for example, expired and removed).
	ErrNotFound = errors.New("idemlease: record not found")
)

// Store persists idempotency records. Implementations must follow the
// semantics documented on each method; the conformance suite in
// idemleasetest verifies them.
//
// Stores never generate or alter reservation tokens: they persist the
// token carried by the Record passed to Reserve and compare it verbatim
// in Complete and Release. Token generation is Begin's job.
type Store interface {
	// Reserve atomically claims the key in the reserved state.
	//
	// If a valid (non-expired) record already exists, Reserve returns it
	// as existing together with ErrAlreadyExists. If only an expired
	// record exists, Reserve must claim the key atomically as if no
	// record existed (overwrite-reserve); it must never return an
	// expired record as existing. A two-step GET-then-SET implementation
	// is not allowed: use a single atomic operation such as SETNX or
	// INSERT ... ON CONFLICT.
	Reserve(ctx context.Context, rec Record) (existing *Record, err error)

	// Complete transitions the reserved record to completed, storing
	// payload and setting the record expiry to now+recordTTL. It must be
	// a compare-and-set on the token: a record held under a different
	// token yields ErrTokenMismatch; a missing (or expired) record
	// yields ErrNotFound.
	Complete(ctx context.Context, key, token string, payload []byte, recordTTL time.Duration) error

	// Release deletes the reserved record, allowing re-execution. Like
	// Complete it is a compare-and-set on the token: ErrTokenMismatch on
	// a token mismatch, ErrNotFound when no record exists.
	Release(ctx context.Context, key, token string) error

	// Get returns the record stored under key, for operations,
	// debugging, adapters, and conformance tests. It is not used on the
	// request path (Reserve's existing return value covers it). Expired
	// records may be reported as (nil, nil).
	Get(ctx context.Context, key string) (*Record, error)
}
