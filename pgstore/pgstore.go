// Package pgstore provides a PostgreSQL-backed idemlease.Store for
// multi-instance deployments, plus (in v1.1) transactional join via
// CompleteTx.
//
// Records live in a single table (default "idemlease_records"). The
// PostgreSQL server clock is the single expiry authority: every
// deadline is set and compared with the database's now(), and the
// deadlines reported back to the core are reconstructed from the
// remaining time, so neither app-to-DB nor app-to-app clock skew can
// corrupt lease semantics. Reserve is a single atomic upsert that
// overwrites only logically-expired records (no GET-then-SET race);
// Complete and Release are token compare-and-set statements.
//
// PostgreSQL has no per-row TTL, so expired rows remain physically
// present (but are treated as absent by every query) until overwritten
// by a later Reserve or removed by Sweep.
//
// The package depends only on database/sql; the driver is chosen by the
// caller (for example github.com/jackc/pgx/v5/stdlib).
package pgstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/repenguin22/idemlease"
)

// DefaultTable is the table name used when no Table option is given.
const DefaultTable = "idemlease_records"

// State values as stored in the smallint "state" column.
const (
	stateReserved  = 1
	stateCompleted = 2
)

var identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

// Schema returns the CREATE TABLE statement for a records table of the
// given name. Run it once (idempotent) as part of your migrations; pass
// pgstore.DefaultTable unless you configured a custom name with Table.
func Schema(table string) string {
	mustValidIdentifier(table)
	// key is bytea, not text: idempotency keys are opaque byte strings
	// and KeyScope composes them with a NUL separator, which a text
	// column cannot store.
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    key               bytea       PRIMARY KEY,
    state             smallint    NOT NULL,
    token             text        NOT NULL,
    fingerprint       bytea       NOT NULL DEFAULT ''::bytea,
    payload           bytea,
    lease_expires_at  timestamptz NOT NULL,
    record_expires_at timestamptz,
    created_at        timestamptz NOT NULL DEFAULT now()
);`, table)
}

func mustValidIdentifier(table string) {
	if !identifierRE.MatchString(table) {
		panic(fmt.Sprintf("pgstore: invalid table name %q", table))
	}
}

// ErrAlreadyCompleted is returned by CompleteTx when the record was
// already completed under the caller's own token — a double call within
// the same transaction (a caller bug, design matrix T8). It is distinct
// from lease loss so the caller does not roll back a healthy
// transaction by mistake.
var ErrAlreadyCompleted = errors.New("pgstore: record already completed with this token")

// DBTX is the subset of *sql.DB / *sql.Tx that CompleteTx needs. Pass
// the *sql.Tx that carries your business writes.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Store is a PostgreSQL-backed idemlease.Store. It is safe for
// concurrent use; atomicity is provided by PostgreSQL.
type Store struct {
	db    *sql.DB
	table string

	qReserve       string
	qReserveExists string
	qComplete      string
	qClassify      string
	qClassifyTx    string
	qRelease       string
	qGet           string
	qSweep         string
}

var _ idemlease.Store = (*Store)(nil)

// Option configures a Store.
type Option func(*Store)

// Table sets the records table name (default DefaultTable). It must
// match the name passed to Schema.
func Table(name string) Option {
	return func(s *Store) { s.table = name }
}

// New returns a Store over db. The table (see Table / DefaultTable) must
// already exist; create it with Schema.
func New(db *sql.DB, opts ...Option) *Store {
	s := &Store{db: db, table: DefaultTable}
	for _, o := range opts {
		o(s)
	}
	mustValidIdentifier(s.table)
	t := s.table
	// A record is logically live while this holds; expired rows are
	// treated as absent by every statement.
	live := fmt.Sprintf(`((state = %d AND lease_expires_at > now()) OR (state = %d AND record_expires_at > now()))`,
		stateReserved, stateCompleted)
	expired := fmt.Sprintf(`((state = %d AND lease_expires_at <= now()) OR (state = %d AND record_expires_at <= now()))`,
		stateReserved, stateCompleted)
	// Same predicate, but column references qualified with the table
	// name: inside ON CONFLICT DO UPDATE ... WHERE, bare column names are
	// ambiguous between the existing row and EXCLUDED (the proposed row).
	expiredExisting := fmt.Sprintf(`((%s.state = %d AND %s.lease_expires_at <= now()) OR (%s.state = %d AND %s.record_expires_at <= now()))`,
		t, stateReserved, t, t, stateCompleted, t)
	leaseMs := `(extract(epoch from (lease_expires_at - now())) * 1000)::bigint`
	recordMs := `CASE WHEN record_expires_at IS NULL THEN NULL ELSE (extract(epoch from (record_expires_at - now())) * 1000)::bigint END`

	// Reserve: claim the key iff no live record exists. On conflict with
	// an expired row the DO UPDATE overwrites it; on conflict with a live
	// row the WHERE is false, 0 rows are returned, and the caller reads
	// the existing record separately.
	s.qReserve = fmt.Sprintf(`
INSERT INTO %s (key, state, token, fingerprint, lease_expires_at, record_expires_at, payload, created_at)
VALUES ($1, %d, $2, $3, now() + interval '1 millisecond' * $4::double precision, NULL, NULL, now())
ON CONFLICT (key) DO UPDATE
  SET state = %d, token = EXCLUDED.token, fingerprint = EXCLUDED.fingerprint,
      lease_expires_at = EXCLUDED.lease_expires_at, record_expires_at = NULL,
      payload = NULL, created_at = now()
  WHERE %s
RETURNING %s`, t, stateReserved, stateReserved, expiredExisting, leaseMs)

	s.qReserveExists = fmt.Sprintf(`
SELECT state, token, fingerprint, payload, %s, %s
FROM %s WHERE key = $1 AND %s`, leaseMs, recordMs, t, live)

	s.qComplete = fmt.Sprintf(`
UPDATE %s SET state = %d, payload = $3, record_expires_at = now() + interval '1 millisecond' * $4::double precision
WHERE key = $1 AND token = $2 AND state = %d AND lease_expires_at > now()`,
		t, stateCompleted, stateReserved)

	// Classifies a failed Complete/Release CAS: a live reserved row with
	// a different token means token mismatch, otherwise not found.
	s.qClassify = fmt.Sprintf(`
SELECT token FROM %s WHERE key = $1 AND state = %d AND lease_expires_at > now()`, t, stateReserved)

	// Classifies a failed CompleteTx CAS inside the caller's tx: reads
	// any live row (own uncommitted writes included) to distinguish a
	// double call (completed under our token) from a lost lease.
	s.qClassifyTx = fmt.Sprintf(`
SELECT state, token FROM %s WHERE key = $1 AND %s`, t, live)

	s.qRelease = fmt.Sprintf(`
DELETE FROM %s WHERE key = $1 AND token = $2 AND state = %d AND lease_expires_at > now()`,
		t, stateReserved)

	s.qGet = fmt.Sprintf(`
SELECT state, token, fingerprint, payload, %s, %s
FROM %s WHERE key = $1 AND %s`, leaseMs, recordMs, t, live)

	s.qSweep = fmt.Sprintf(`
DELETE FROM %s WHERE key IN (SELECT key FROM %s WHERE %s LIMIT $1)`, t, t, expired)

	return s
}

func leaseTTLMillis(rec idemlease.Record) int64 {
	ms := time.Until(rec.LeaseExpiresAt).Milliseconds()
	if ms < 1 {
		ms = 1
	}
	return ms
}

func recordTTLMillis(d time.Duration) int64 {
	ms := d.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	return ms
}

// Reserve implements idemlease.Store.
func (s *Store) Reserve(ctx context.Context, rec idemlease.Record) (*idemlease.Record, error) {
	fp := rec.Fingerprint
	if fp == nil {
		fp = []byte{}
	}
	// The claim is a single atomic statement; the rare "lost, then the
	// blocking row vanished" race (a concurrent Release) is retried.
	keyBytes := []byte(rec.Key)
	for attempt := 0; attempt < 3; attempt++ {
		var leaseMs int64
		err := s.db.QueryRowContext(ctx, s.qReserve, keyBytes, rec.Token, fp, leaseTTLMillis(rec)).Scan(&leaseMs)
		if err == nil {
			return nil, nil // claimed
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("pgstore: reserve: %w", err)
		}
		// Lost the claim: a live record blocks us. Read it.
		existing, err := s.scanRecord(s.db.QueryRowContext(ctx, s.qReserveExists, keyBytes), rec.Key)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue // the blocking row expired/was released; retry
			}
			return nil, fmt.Errorf("pgstore: reserve read existing: %w", err)
		}
		return existing, idemlease.ErrAlreadyExists
	}
	return nil, fmt.Errorf("pgstore: reserve: claim kept losing to a vanishing record")
}

// Complete implements idemlease.Store.
func (s *Store) Complete(ctx context.Context, key, token string, payload []byte, recordTTL time.Duration) error {
	res, err := s.db.ExecContext(ctx, s.qComplete, []byte(key), token, payload, recordTTLMillis(recordTTL))
	if err != nil {
		return fmt.Errorf("pgstore: complete: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return nil
	}
	return s.classifyFailure(ctx, key, token, "complete")
}

// Release implements idemlease.Store.
func (s *Store) Release(ctx context.Context, key, token string) error {
	res, err := s.db.ExecContext(ctx, s.qRelease, []byte(key), token)
	if err != nil {
		return fmt.Errorf("pgstore: release: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return nil
	}
	return s.classifyFailure(ctx, key, token, "release")
}

// classifyFailure turns a 0-row Complete/Release into the right
// sentinel: a live reserved row held under another token is a token
// mismatch; anything else (absent, expired, completed) is not found.
// Either sentinel is contract-valid for records lost to expiry (§3.2).
func (s *Store) classifyFailure(ctx context.Context, key, token, op string) error {
	var current string
	err := s.db.QueryRowContext(ctx, s.qClassify, []byte(key)).Scan(&current)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return idemlease.ErrNotFound
	case err != nil:
		return fmt.Errorf("pgstore: %s classify: %w", op, err)
	case current != token:
		return idemlease.ErrTokenMismatch
	default:
		return idemlease.ErrNotFound
	}
}

// Get implements idemlease.Store. Expired records are reported as
// (nil, nil).
func (s *Store) Get(ctx context.Context, key string) (*idemlease.Record, error) {
	rec, err := s.scanRecord(s.db.QueryRowContext(ctx, s.qGet, []byte(key)), key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pgstore: get: %w", err)
	}
	return rec, nil
}

// CompleteTx performs the reserved→completed transition inside the
// caller's transaction, so the business writes and the idempotency
// record commit or roll back as one unit (§9.3). Take key and token
// from httpidem.ReservationFromContext, and after a successful commit
// call httpidem.MarkFinished and respond via httpidem.WriteStored with
// the same StoredResponse whose MarshalBinary produced payload.
//
// Return values follow the idemlease.Finish vocabulary:
//
//   - (false, nil): the transition is part of tx; committing tx makes
//     the record and the business writes durable together.
//   - (true, nil): the lease was lost — expired, or the key is owned by
//     another execution. The caller MUST roll back tx: committing would
//     risk duplicating the business effects the new owner is producing.
//   - (false, ErrAlreadyCompleted): CompleteTx already succeeded under
//     this token in this transaction (a double call); the transaction
//     itself is healthy.
//   - (false, err): infrastructure failure; roll back.
//
// The CAS UPDATE takes the record's row lock, so racing executions of
// the same key serialize here: the loser sees zero rows after the
// winner commits and must roll back — this is what upgrades the
// guarantee to "at most one business commit per completed record".
func (s *Store) CompleteTx(ctx context.Context, tx DBTX, key, token string, payload []byte, recordTTL time.Duration) (leaseLost bool, err error) {
	res, err := tx.ExecContext(ctx, s.qComplete, []byte(key), token, payload, recordTTLMillis(recordTTL))
	if err != nil {
		return false, fmt.Errorf("pgstore: complete tx: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return false, nil
	}
	var (
		state   int
		current string
	)
	err = tx.QueryRowContext(ctx, s.qClassifyTx, []byte(key)).Scan(&state, &current)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return true, nil // absent or expired: the lease is gone
	case err != nil:
		return false, fmt.Errorf("pgstore: complete tx classify: %w", err)
	case state == stateCompleted && current == token:
		return false, ErrAlreadyCompleted
	default:
		return true, nil // another execution owns the record
	}
}

// Sweep physically deletes up to limit logically-expired rows and
// returns how many were removed. It is optional housekeeping — expired
// rows are already treated as absent — suitable for a periodic job.
func (s *Store) Sweep(ctx context.Context, limit int) (int, error) {
	res, err := s.db.ExecContext(ctx, s.qSweep, limit)
	if err != nil {
		return 0, fmt.Errorf("pgstore: sweep: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// scanRecord decodes a row of (state, token, fingerprint, payload,
// lease_ms, record_ms). The deadlines are rebuilt from the DB-reported
// remaining milliseconds on the local clock, keeping the DB the single
// expiry authority.
func (s *Store) scanRecord(row *sql.Row, key string) (*idemlease.Record, error) {
	var (
		state   int
		token   string
		fp      []byte
		payload []byte
		leaseMs int64
		recMs   sql.NullInt64
	)
	if err := row.Scan(&state, &token, &fp, &payload, &leaseMs, &recMs); err != nil {
		return nil, err
	}
	rec := &idemlease.Record{Key: key, Token: token, Fingerprint: fp, Payload: payload}
	now := time.Now()
	switch state {
	case stateReserved:
		rec.State = idemlease.StateReserved
		rec.LeaseExpiresAt = now.Add(time.Duration(leaseMs) * time.Millisecond)
	case stateCompleted:
		rec.State = idemlease.StateCompleted
		if recMs.Valid {
			rec.RecordExpiresAt = now.Add(time.Duration(recMs.Int64) * time.Millisecond)
		}
	default:
		return nil, fmt.Errorf("pgstore: record %q has invalid state %d", key, state)
	}
	return rec, nil
}
