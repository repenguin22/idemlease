// Package redistore provides a Redis-backed idemlease.Store for
// multi-instance deployments.
//
// Each record is a Redis HASH under prefix+key (default prefix
// "idemlease:") whose lifetime is bound to the record's lease or record
// TTL via PEXPIRE, so an expired record is absent by construction —
// exactly the §3.2 semantics. All mutations run as server-side Lua
// scripts, which Redis executes atomically: Reserve is a single
// check-and-claim (no GET-then-SET race), and Complete/Release are
// token compare-and-set operations. Tokens, fingerprints, and payloads
// are stored as raw binary hash fields, never generated or altered here
// (§2.2).
//
// Expiry TTLs are computed as durations on the application side and
// applied relative to the Redis server clock, so moderate clock skew
// between application and Redis does not corrupt lease semantics.
// Requires Redis 6.0 or later.
package redistore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/repenguin22/idemlease"
)

// Hash field values for Record.State.
const (
	stateReserved  = "1"
	stateCompleted = "2"
)

// reserveScript atomically claims the key: if a live record exists it
// is returned as the HGETALL field list; otherwise the reservation is
// written with its lease TTL.
//
//	KEYS[1] = record key
//	ARGV[1] = state ("1"), ARGV[2] = token, ARGV[3] = fingerprint,
//	ARGV[4] = lease deadline (unix ms, informational),
//	ARGV[5] = lease TTL (ms, relative)
var reserveScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  return redis.call('HGETALL', KEYS[1])
end
redis.call('HSET', KEYS[1], 'state', ARGV[1], 'token', ARGV[2], 'fp', ARGV[3], 'lease_exp_ms', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return 'OK'
`)

// completeScript is the reserved→completed transition with a token CAS.
//
//	KEYS[1] = record key
//	ARGV[1] = token, ARGV[2] = payload,
//	ARGV[3] = record deadline (unix ms, informational),
//	ARGV[4] = record TTL (ms, relative)
var completeScript = redis.NewScript(`
local token = redis.call('HGET', KEYS[1], 'token')
if not token then
  return 'NOT_FOUND'
end
if token ~= ARGV[1] then
  return 'TOKEN_MISMATCH'
end
if redis.call('HGET', KEYS[1], 'state') ~= '1' then
  return 'NOT_FOUND'
end
redis.call('HSET', KEYS[1], 'state', '2', 'payload', ARGV[2], 'rec_exp_ms', ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return 'OK'
`)

// releaseScript deletes the reservation with a token CAS.
//
//	KEYS[1] = record key, ARGV[1] = token
var releaseScript = redis.NewScript(`
local token = redis.call('HGET', KEYS[1], 'token')
if not token then
  return 'NOT_FOUND'
end
if token ~= ARGV[1] then
  return 'TOKEN_MISMATCH'
end
if redis.call('HGET', KEYS[1], 'state') ~= '1' then
  return 'NOT_FOUND'
end
redis.call('DEL', KEYS[1])
return 'OK'
`)

// Store is a Redis-backed idemlease.Store. It is safe for concurrent
// use; atomicity is provided by Redis itself.
type Store struct {
	client redis.UniversalClient
	prefix string
}

var _ idemlease.Store = (*Store)(nil)

// Option configures a Store.
type Option func(*Store)

// KeyPrefix replaces the "idemlease:" prefix put in front of record
// keys in Redis.
func KeyPrefix(p string) Option {
	return func(s *Store) { s.prefix = p }
}

// New returns a Store on top of client, which may be a single-node
// client, a cluster client, or a ring.
func New(client redis.UniversalClient, opts ...Option) *Store {
	s := &Store{client: client, prefix: "idemlease:"}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *Store) redisKey(key string) string { return s.prefix + key }

// pxUntil converts an absolute deadline to a relative TTL in
// milliseconds for PEXPIRE, clamped to at least 1ms.
func pxUntil(deadline time.Time) int64 {
	ms := time.Until(deadline).Milliseconds()
	if ms < 1 {
		ms = 1
	}
	return ms
}

// Reserve implements idemlease.Store.
func (s *Store) Reserve(ctx context.Context, rec idemlease.Record) (*idemlease.Record, error) {
	res, err := reserveScript.Run(ctx, s.client, []string{s.redisKey(rec.Key)},
		stateReserved,
		rec.Token,
		string(rec.Fingerprint),
		strconv.FormatInt(rec.LeaseExpiresAt.UnixMilli(), 10),
		strconv.FormatInt(pxUntil(rec.LeaseExpiresAt), 10),
	).Result()
	if err != nil {
		return nil, fmt.Errorf("redistore: reserve: %w", err)
	}
	switch v := res.(type) {
	case string: // "OK": the key was claimed
		return nil, nil
	case []interface{}: // HGETALL of the live existing record
		fields := make(map[string]string, len(v)/2)
		for i := 0; i+1 < len(v); i += 2 {
			name, _ := v[i].(string)
			value, _ := v[i+1].(string)
			fields[name] = value
		}
		existing, err := recordFromFields(rec.Key, fields)
		if err != nil {
			return nil, err
		}
		return existing, idemlease.ErrAlreadyExists
	default:
		return nil, fmt.Errorf("redistore: reserve: unexpected script result %T", res)
	}
}

// Complete implements idemlease.Store.
func (s *Store) Complete(ctx context.Context, key, token string, payload []byte, recordTTL time.Duration) error {
	pxMs := recordTTL.Milliseconds()
	if pxMs < 1 {
		pxMs = 1
	}
	res, err := completeScript.Run(ctx, s.client, []string{s.redisKey(key)},
		token,
		string(payload),
		strconv.FormatInt(time.Now().Add(recordTTL).UnixMilli(), 10),
		strconv.FormatInt(pxMs, 10),
	).Result()
	if err != nil {
		return fmt.Errorf("redistore: complete: %w", err)
	}
	return statusToErr(res, "complete")
}

// Release implements idemlease.Store.
func (s *Store) Release(ctx context.Context, key, token string) error {
	res, err := releaseScript.Run(ctx, s.client, []string{s.redisKey(key)}, token).Result()
	if err != nil {
		return fmt.Errorf("redistore: release: %w", err)
	}
	return statusToErr(res, "release")
}

// Get implements idemlease.Store. Expired records are reported as
// (nil, nil) because Redis has already removed them.
func (s *Store) Get(ctx context.Context, key string) (*idemlease.Record, error) {
	fields, err := s.client.HGetAll(ctx, s.redisKey(key)).Result()
	if err != nil {
		return nil, fmt.Errorf("redistore: get: %w", err)
	}
	if len(fields) == 0 {
		return nil, nil
	}
	return recordFromFields(key, fields)
}

func statusToErr(res interface{}, op string) error {
	status, _ := res.(string)
	switch status {
	case "OK":
		return nil
	case "NOT_FOUND":
		return idemlease.ErrNotFound
	case "TOKEN_MISMATCH":
		return idemlease.ErrTokenMismatch
	default:
		return fmt.Errorf("redistore: %s: unexpected script result %v", op, res)
	}
}

func recordFromFields(key string, fields map[string]string) (*idemlease.Record, error) {
	rec := &idemlease.Record{
		Key:         key,
		Token:       fields["token"],
		Fingerprint: []byte(fields["fp"]),
	}
	switch fields["state"] {
	case stateReserved:
		rec.State = idemlease.StateReserved
	case stateCompleted:
		rec.State = idemlease.StateCompleted
	default:
		return nil, fmt.Errorf("redistore: corrupted record %q: state %q", key, fields["state"])
	}
	if v, ok := fields["lease_exp_ms"]; ok {
		ms, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("redistore: corrupted record %q: lease_exp_ms %q", key, v)
		}
		rec.LeaseExpiresAt = time.UnixMilli(ms)
	}
	if v, ok := fields["rec_exp_ms"]; ok {
		ms, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("redistore: corrupted record %q: rec_exp_ms %q", key, v)
		}
		rec.RecordExpiresAt = time.UnixMilli(ms)
	}
	if v, ok := fields["payload"]; ok {
		rec.Payload = []byte(v)
	}
	return rec, nil
}
