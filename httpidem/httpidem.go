// Package httpidem adds Idempotency-Key semantics to net/http handlers,
// backed by the idemlease state machine.
//
// The middleware returned by New is the default entry point:
//
//	mw := httpidem.New(store,
//		httpidem.Require(true),
//		httpidem.KeyScope(scopeFromAuth),
//	)
//	handler := mw(mux)
//
// It parses the Idempotency-Key header (ParseKey / KeyFromHeader),
// fingerprints the request (Fingerprint), drives idemlease.Begin and
// idemlease.Finish, captures the response (Recorder), and replays
// stored responses with the Idempotency-Replayed header. Every building
// block is exported so framework adapters can assemble the same flow on
// non-net/http stacks (§6.2); the middleware itself is a thin
// composition of those parts (§12-13).
package httpidem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/repenguin22/idemlease"
)

// Default capture limits (§4.2, §6.1).
const (
	DefaultMaxRequestBody  int64 = 1 << 20
	DefaultMaxResponseBody int   = 1 << 20
)

// maxStoredResponseOverhead bounds the non-body bytes (status, headers,
// framing) accepted when decoding a stored payload: anything larger
// than MaxResponseBody plus this margin cannot have been written by the
// capture path and is refused instead of allocated.
const maxStoredResponseOverhead = 64 << 10

type config struct {
	require         bool
	leaseTTL        time.Duration
	recordTTL       time.Duration
	maxRequestBody  int64
	maxResponseBody int
	bypassLargeBody bool
	policy          ReplayPolicy
	errorWriter     ErrorWriter
	failOpen        bool
	methods         map[string]bool
	storeHeaders    []string
	keyScope        func(*http.Request) string
	keyValidator    func(string) bool
	logger          *slog.Logger
	hashKeysInLogs  bool
	replayedHeader  bool
}

// Option configures the middleware returned by New.
type Option func(*config)

// Require controls requests without an Idempotency-Key header: true
// rejects them with 400, false (the default) passes them through
// without idempotency handling.
func Require(v bool) Option { return func(c *config) { c.require = v } }

// LeaseTTL bounds a single in-flight execution. Zero uses
// idemlease.DefaultLeaseTTL. The value is passed transparently to the
// core in both Begin and Finish.
func LeaseTTL(d time.Duration) Option { return func(c *config) { c.leaseTTL = d } }

// RecordTTL bounds how long a stored response is replayed. Zero uses
// idemlease.DefaultRecordTTL.
func RecordTTL(d time.Duration) Option { return func(c *config) { c.recordTTL = d } }

// MaxRequestBody caps how many request body bytes are buffered for
// fingerprinting (default 1 MiB). Larger requests get 413, or bypass
// idempotency entirely with BypassLargeBody(true).
func MaxRequestBody(n int64) Option { return func(c *config) { c.maxRequestBody = n } }

// MaxResponseBody caps how many response body bytes are captured for
// replay (default 1 MiB). Larger responses are still sent to the client
// but are discarded instead of persisted, with a warning log.
func MaxResponseBody(n int) Option { return func(c *config) { c.maxResponseBody = n } }

// BypassLargeBody makes requests whose body exceeds MaxRequestBody skip
// idempotency handling entirely (no fingerprint, no Begin) with a
// warning log, instead of being rejected with 413.
func BypassLargeBody(v bool) Option { return func(c *config) { c.bypassLargeBody = v } }

// Policy replaces the status-driven default ReplayPolicy (§5.1).
func Policy(p ReplayPolicy) Option { return func(c *config) { c.policy = p } }

// Errors replaces the default RFC 9457 ErrorWriter.
func Errors(ew ErrorWriter) Option { return func(c *config) { c.errorWriter = ew } }

// FailOpen controls Begin-time store failures only (§4.6): false (the
// default) rejects with 503, true passes the request through without
// idempotency and logs a warning.
func FailOpen(v bool) Option { return func(c *config) { c.failOpen = v } }

// Methods replaces the set of methods subject to idempotency handling
// (default POST and PATCH). Other methods pass through untouched.
func Methods(methods ...string) Option {
	return func(c *config) {
		c.methods = make(map[string]bool, len(methods))
		for _, m := range methods {
			c.methods[strings.ToUpper(m)] = true
		}
	}
}

// StoreHeaders replaces the allowlist of response headers captured for
// replay (default Content-Type, Content-Language, Content-Encoding,
// Content-Disposition, Location). Headers the client needs to interpret
// the body — Content-Encoding in particular — must stay allowlisted, or
// replays of, say, gzip-written bodies arrive unlabelled and corrupt.
// The always-excluded headers — Set-Cookie, Authorization, and
// hop-by-hop headers — are dropped even if listed, to prevent session
// leakage across clients (§4.4).
func StoreHeaders(names ...string) Option {
	return func(c *config) { c.storeHeaders = append([]string(nil), names...) }
}

// KeyScope namespaces keys by caller (§4.5): the returned scope
// (typically an authenticated tenant ID) is joined with the client key
// as scope + "\x00" + key before it reaches the store, so equal keys
// from different callers stay independent. An empty scope means global.
// Strongly recommended for multi-tenant public APIs: without it one
// client can replay another client's stored responses by guessing keys.
func KeyScope(fn func(*http.Request) string) Option {
	return func(c *config) { c.keyScope = fn }
}

// KeyValidator adds validation after grammar parsing, e.g. restricting
// keys to UUIDs. Rejected keys get 400. Default: no extra validation.
func KeyValidator(fn func(string) bool) Option {
	return func(c *config) { c.keyValidator = fn }
}

// Logger replaces slog.Default() for the middleware's warning logs.
func Logger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// HashKeysInLogs logs SHA-256 digests instead of raw keys and scopes,
// for deployments where keys may carry secrets (§6.1).
func HashKeysInLogs(v bool) Option { return func(c *config) { c.hashKeysInLogs = v } }

// ReplayedHeader controls the Idempotency-Replayed: true marker on
// replayed responses (default on, following Stripe practice).
func ReplayedHeader(v bool) Option { return func(c *config) { c.replayedHeader = v } }

// New builds the idempotency middleware around store. See the package
// documentation for the behavior table; configuration is applied with
// the Option functions in this package.
func New(store idemlease.Store, opts ...Option) func(http.Handler) http.Handler {
	cfg := &config{
		maxRequestBody:  DefaultMaxRequestBody,
		maxResponseBody: DefaultMaxResponseBody,
		policy:          DefaultPolicy,
		errorWriter:     problemWriter{},
		methods:         map[string]bool{http.MethodPost: true, http.MethodPatch: true},
		storeHeaders:    []string{"Content-Type", "Content-Language", "Content-Encoding", "Content-Disposition", "Location"},
		logger:          slog.Default(),
		replayedHeader:  true,
	}
	for _, o := range opts {
		o(cfg)
	}
	return func(next http.Handler) http.Handler {
		return &middleware{store: store, cfg: cfg, next: next}
	}
}

type middleware struct {
	store idemlease.Store
	cfg   *config
	next  http.Handler
}

func (m *middleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cfg := m.cfg
	if !cfg.methods[strings.ToUpper(r.Method)] {
		m.next.ServeHTTP(w, r)
		return
	}

	key, err := KeyFromHeader(r.Header)
	if err != nil {
		if errors.Is(err, ErrKeyMissing) && !cfg.require {
			m.next.ServeHTTP(w, r)
			return
		}
		cfg.errorWriter.WriteError(w, r, http.StatusBadRequest, err)
		return
	}
	if cfg.keyValidator != nil && !cfg.keyValidator(key) {
		cfg.errorWriter.WriteError(w, r, http.StatusBadRequest,
			fmt.Errorf("%w: rejected by KeyValidator", ErrKeyInvalid))
		return
	}

	body, overflow, err := readBody(r, cfg.maxRequestBody)
	if err != nil {
		cfg.errorWriter.WriteError(w, r, http.StatusBadRequest,
			fmt.Errorf("httpidem: reading request body: %w", err))
		return
	}
	if overflow {
		if !cfg.bypassLargeBody {
			cfg.errorWriter.WriteError(w, r, http.StatusRequestEntityTooLarge, ErrRequestBodyTooLarge)
			return
		}
		cfg.logger.Warn("httpidem: request body exceeds MaxRequestBody; bypassing idempotency",
			m.keyAttrs("", key)...)
		r.Body = readCloser{io.MultiReader(bytes.NewReader(body), r.Body), r.Body}
		m.next.ServeHTTP(w, r)
		return
	}
	if r.Body != nil {
		_ = r.Body.Close() // fully read; the buffered copy below replaces it
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))

	fingerprint := Fingerprint(r.Method, r.URL, body)
	scope := ""
	storeKey := key
	if cfg.keyScope != nil {
		// "\x00" cannot occur in a parsed key (§4.1 rejects control
		// characters), so scoped keys never collide with raw ones.
		if scope = cfg.keyScope(r); scope != "" {
			storeKey = scope + "\x00" + key
		}
	}
	o := idemlease.Options{LeaseTTL: cfg.leaseTTL, RecordTTL: cfg.recordTTL}

	out, err := idemlease.Begin(r.Context(), m.store, storeKey, fingerprint, o)
	if err != nil {
		if cfg.failOpen {
			cfg.logger.Warn("httpidem: store unavailable; proceeding without idempotency (FailOpen)",
				append(m.keyAttrs(scope, key), slog.Any("error", err))...)
			m.next.ServeHTTP(w, r)
			return
		}
		cfg.errorWriter.WriteError(w, r, http.StatusServiceUnavailable,
			fmt.Errorf("%w: %w", ErrStoreUnavailable, err))
		return
	}

	switch out.Action {
	case idemlease.Replay:
		m.writeReplay(w, r, scope, key, out.Payload)
		return
	case idemlease.RejectInFlight:
		w.Header().Set("Retry-After", retryAfterSeconds(out.RetryAfter))
		cfg.errorWriter.WriteError(w, r, http.StatusConflict, ErrInFlight)
		return
	case idemlease.RejectFingerprintMismatch:
		cfg.errorWriter.WriteError(w, r, http.StatusUnprocessableEntity, ErrFingerprintMismatch)
		return
	}

	// Proceed: run the handler under capture, then Finish.
	rec := NewRecorder(cfg.maxResponseBody, cfg.storeHeaders)
	cw := &captureWriter{ResponseWriter: w, rec: rec}
	box := &errBox{}
	ctx := context.WithValue(r.Context(), errBoxKey{}, box)
	// Finish must reach the store even when the client disconnects
	// mid-request: the work happened, so the record must reflect it.
	finishCtx := context.WithoutCancel(ctx)

	defer func() {
		if p := recover(); p != nil {
			// §5.1: panic → Discard and re-panic; recovery into an HTTP
			// response is the responsibility of outer middleware.
			if _, ferr := idemlease.Finish(finishCtx, m.store, storeKey, out.Token, idemlease.Discard, nil, o); ferr != nil {
				cfg.logger.Warn("httpidem: releasing reservation after handler panic failed",
					append(m.keyAttrs(scope, key), slog.Any("error", ferr))...)
			}
			panic(p)
		}
	}()
	m.next.ServeHTTP(cw, r.WithContext(ctx))
	cw.finalize()

	decision := cfg.policy.Decide(rec.Status(), box.get())
	if decision == idemlease.Persist {
		switch {
		case rec.Abandoned():
			decision = idemlease.Discard
			cfg.logger.Warn("httpidem: response was streamed (Flush/Hijack); discarding instead of persisting",
				m.keyAttrs(scope, key)...)
		case rec.Overflowed():
			decision = idemlease.Discard
			cfg.logger.Warn("httpidem: response exceeds MaxResponseBody; discarding instead of persisting",
				m.keyAttrs(scope, key)...)
		}
	}

	var payload []byte
	if decision == idemlease.Persist {
		sr, _ := rec.StoredResponse()
		payload, err = sr.MarshalBinary()
		if err != nil {
			decision = idemlease.Discard
			cfg.logger.Warn("httpidem: encoding stored response failed; discarding",
				append(m.keyAttrs(scope, key), slog.Any("error", err))...)
		}
	}

	leaseLost, err := idemlease.Finish(finishCtx, m.store, storeKey, out.Token, decision, payload, o)
	switch {
	case err != nil && decision == idemlease.Persist:
		// §4.6: the response already reached the client. Try to free the
		// reservation so retries need not wait out the lease; if that
		// also fails, the key stays reserved (409) until lease expiry.
		cfg.logger.Warn("httpidem: persisting response failed; idempotency temporarily weakened",
			append(m.keyAttrs(scope, key), slog.Any("error", err))...)
		if _, rerr := idemlease.Finish(finishCtx, m.store, storeKey, out.Token, idemlease.Discard, nil, o); rerr != nil {
			cfg.logger.Warn("httpidem: best-effort release failed; key stays reserved until lease expiry",
				append(m.keyAttrs(scope, key), slog.Any("error", rerr))...)
		}
	case err != nil:
		// §4.6: Release failure resolves itself at lease expiry.
		cfg.logger.Warn("httpidem: releasing reservation failed; key stays reserved until lease expiry",
			append(m.keyAttrs(scope, key), slog.Any("error", err))...)
	case leaseLost:
		// §3.3: the response was returned but will not be replayed.
		cfg.logger.Warn("httpidem: lease lost during execution; response returned but not stored for replay",
			append(m.keyAttrs(scope, key), slog.String("event", "lease_lost"))...)
	}
}

func (m *middleware) writeReplay(w http.ResponseWriter, r *http.Request, scope, key string, payload []byte) {
	if len(payload) > m.cfg.maxResponseBody+maxStoredResponseOverhead {
		m.cfg.logger.Error("httpidem: stored payload exceeds the configured response limits; refusing to replay",
			append(m.keyAttrs(scope, key), slog.Int("payload_bytes", len(payload)))...)
		m.cfg.errorWriter.WriteError(w, r, http.StatusInternalServerError, errCorruptStoredResponse)
		return
	}
	var sr StoredResponse
	if err := sr.UnmarshalBinary(payload); err != nil {
		m.cfg.logger.Error("httpidem: stored response payload is corrupted",
			append(m.keyAttrs(scope, key), slog.Any("error", err))...)
		m.cfg.errorWriter.WriteError(w, r, http.StatusInternalServerError, err)
		return
	}
	header := w.Header()
	for name, values := range sr.Header {
		header[name] = append([]string(nil), values...)
	}
	if m.cfg.replayedHeader {
		header.Set(HeaderIdempotencyReplayed, "true")
	}
	w.WriteHeader(sr.StatusCode)
	_, _ = w.Write(sr.Body)
}

// keyAttrs builds the log attributes carried by every middleware log
// line: always idempotency_key, plus idempotency_scope when KeyScope is
// in use (§6.1).
func (m *middleware) keyAttrs(scope, key string) []any {
	if m.cfg.hashKeysInLogs {
		key = hashForLog(key)
		if scope != "" {
			scope = hashForLog(scope)
		}
	}
	attrs := []any{slog.String("idempotency_key", key)}
	if scope != "" {
		attrs = append(attrs, slog.String("idempotency_scope", scope))
	}
	return attrs
}

func hashForLog(s string) string {
	sum := sha256.Sum256([]byte(s))
	return "sha256:" + hex.EncodeToString(sum[:8])
}

// readBody reads at most max bytes of the request body. overflow
// reports that the body is larger than max; the returned prefix plus
// the unread remainder of r.Body then hold the full body.
func readBody(r *http.Request, max int64) (body []byte, overflow bool, err error) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, false, nil
	}
	body, err = io.ReadAll(io.LimitReader(r.Body, max+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(body)) > max {
		return body, true, nil
	}
	return body, false, nil
}

// readCloser joins a replacement Reader with the original Body's Closer
// for the large-body bypass path.
type readCloser struct {
	io.Reader
	io.Closer
}

// retryAfterSeconds renders the remaining lease as Retry-After seconds,
// rounded up, minimum 1 (§4.4).
func retryAfterSeconds(d time.Duration) string {
	secs := (d + time.Second - 1) / time.Second
	if secs < 1 {
		secs = 1
	}
	return strconv.FormatInt(int64(secs), 10)
}
