// Package memstore provides an in-memory idemlease.Store for
// development and testing.
//
// It is strictly single-process: records live in process memory, so
// instances behind a load balancer each see their own records and the
// idempotency guarantee does not hold across them. Use a shared store
// such as redistore in multi-instance deployments.
//
// Expiry is evaluated lazily: expired records are treated as absent and
// are deleted the next time their key is accessed. Memory therefore
// grows with the number of distinct keys that are never touched again,
// which is acceptable for development and tests but is another reason
// not to use this store in production.
package memstore

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/repenguin22/idemlease"
)

// Store is an in-memory idemlease.Store. It is safe for concurrent use.
// The zero value is not usable; call New.
type Store struct {
	mu   sync.Mutex
	recs map[string]idemlease.Record
}

var _ idemlease.Store = (*Store)(nil)

// New returns an empty Store.
func New() *Store {
	return &Store{recs: make(map[string]idemlease.Record)}
}

func clone(r idemlease.Record) idemlease.Record {
	r.Fingerprint = bytes.Clone(r.Fingerprint)
	r.Payload = bytes.Clone(r.Payload)
	return r
}

// lookup returns the live record for key, lazily deleting it when
// expired. The caller must hold s.mu.
func (s *Store) lookup(key string, now time.Time) (idemlease.Record, bool) {
	rec, ok := s.recs[key]
	if !ok {
		return idemlease.Record{}, false
	}
	if rec.Expired(now) {
		delete(s.recs, key)
		return idemlease.Record{}, false
	}
	return rec, true
}

// Reserve implements idemlease.Store. The check-and-claim runs under a
// single mutex acquisition, which makes overwriting an expired record
// as atomic as claiming a free key.
func (s *Store) Reserve(ctx context.Context, rec idemlease.Record) (*idemlease.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.lookup(rec.Key, time.Now()); ok {
		cp := clone(existing)
		return &cp, idemlease.ErrAlreadyExists
	}
	s.recs[rec.Key] = clone(rec)
	return nil, nil
}

// Complete implements idemlease.Store.
func (s *Store) Complete(ctx context.Context, key, token string, payload []byte, recordTTL time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	rec, ok := s.lookup(key, now)
	if !ok {
		return idemlease.ErrNotFound
	}
	if rec.Token != token {
		return idemlease.ErrTokenMismatch
	}
	if rec.State != idemlease.StateReserved {
		return idemlease.ErrNotFound // no reservation left to complete
	}
	rec.State = idemlease.StateCompleted
	rec.Payload = bytes.Clone(payload)
	rec.RecordExpiresAt = now.Add(recordTTL)
	s.recs[key] = rec
	return nil
}

// Release implements idemlease.Store.
func (s *Store) Release(ctx context.Context, key, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.lookup(key, time.Now())
	if !ok {
		return idemlease.ErrNotFound
	}
	if rec.Token != token {
		return idemlease.ErrTokenMismatch
	}
	if rec.State != idemlease.StateReserved {
		return idemlease.ErrNotFound // only reserved records are releasable
	}
	delete(s.recs, key)
	return nil
}

// Get implements idemlease.Store. Expired records are reported as
// (nil, nil).
func (s *Store) Get(ctx context.Context, key string) (*idemlease.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.lookup(key, time.Now())
	if !ok {
		return nil, nil
	}
	cp := clone(rec)
	return &cp, nil
}
