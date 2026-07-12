package idemlease_test

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/repenguin22/idemlease"
)

var _ idemlease.Store = (*fakeStore)(nil)

// fakeStore is an in-memory reference implementation of the Store
// semantics (REQUIREMENTS §3.2) with fault injection and expiry control
// for tests. It is the model for memstore (M2), where it will be
// promoted into the idemleasetest conformance suite.
type fakeStore struct {
	mu   sync.Mutex
	recs map[string]idemlease.Record

	reserveErr  error // returned by Reserve when non-nil
	completeErr error // returned by Complete when non-nil
	releaseErr  error // returned by Release when non-nil
}

func newFakeStore() *fakeStore {
	return &fakeStore{recs: make(map[string]idemlease.Record)}
}

func cloneRecord(r idemlease.Record) idemlease.Record {
	r.Fingerprint = bytes.Clone(r.Fingerprint)
	r.Payload = bytes.Clone(r.Payload)
	return r
}

// put seeds a record directly, bypassing Reserve.
func (f *fakeStore) put(rec idemlease.Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recs[rec.Key] = cloneRecord(rec)
}

// get returns the raw stored record regardless of expiry.
func (f *fakeStore) get(key string) (idemlease.Record, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.recs[key]
	return cloneRecord(rec), ok
}

// expireLease backdates the record's lease so the store treats it as
// logically absent, simulating lease expiry without sleeping.
func (f *fakeStore) expireLease(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec := f.recs[key]
	rec.LeaseExpiresAt = time.Now().Add(-time.Second)
	f.recs[key] = rec
}

// expireRecord backdates the record's record TTL, simulating expiry of a
// completed record.
func (f *fakeStore) expireRecord(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec := f.recs[key]
	rec.RecordExpiresAt = time.Now().Add(-time.Second)
	f.recs[key] = rec
}

func (f *fakeStore) Reserve(ctx context.Context, rec idemlease.Record) (*idemlease.Record, error) {
	if f.reserveErr != nil {
		return nil, f.reserveErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, ok := f.recs[rec.Key]; ok && !existing.Expired(time.Now()) {
		cp := cloneRecord(existing)
		return &cp, idemlease.ErrAlreadyExists
	}
	f.recs[rec.Key] = cloneRecord(rec)
	return nil, nil
}

func (f *fakeStore) Complete(ctx context.Context, key, token string, payload []byte, recordTTL time.Duration) error {
	if f.completeErr != nil {
		return f.completeErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	rec, ok := f.recs[key]
	if !ok || rec.Expired(now) {
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
	f.recs[key] = rec
	return nil
}

func (f *fakeStore) Release(ctx context.Context, key, token string) error {
	if f.releaseErr != nil {
		return f.releaseErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.recs[key]
	if !ok || rec.Expired(time.Now()) {
		return idemlease.ErrNotFound
	}
	if rec.Token != token {
		return idemlease.ErrTokenMismatch
	}
	if rec.State != idemlease.StateReserved {
		return idemlease.ErrNotFound // only reserved records are releasable
	}
	delete(f.recs, key)
	return nil
}

func (f *fakeStore) Get(ctx context.Context, key string) (*idemlease.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.recs[key]
	if !ok || rec.Expired(time.Now()) {
		return nil, nil
	}
	cp := cloneRecord(rec)
	return &cp, nil
}

var _ idemlease.Store = (*stubStore)(nil)

// stubStore returns canned Reserve responses, for exercising Begin
// against misbehaving stores.
type stubStore struct {
	existing *idemlease.Record
	err      error
}

func (s *stubStore) Reserve(context.Context, idemlease.Record) (*idemlease.Record, error) {
	return s.existing, s.err
}

func (s *stubStore) Complete(context.Context, string, string, []byte, time.Duration) error {
	panic("stubStore: unexpected Complete")
}

func (s *stubStore) Release(context.Context, string, string) error {
	panic("stubStore: unexpected Release")
}

func (s *stubStore) Get(context.Context, string) (*idemlease.Record, error) {
	panic("stubStore: unexpected Get")
}
