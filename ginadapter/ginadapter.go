// Package ginadapter integrates idemlease with the Gin framework.
//
// It assembles the same flow as httpidem's net/http middleware from the
// exported building blocks — KeyFromHeader, Fingerprint, Recorder,
// StoredResponse, ErrorChannel — plus the core Begin/Finish
// (REQUIREMENTS §9.2, §6.2), so the client-observable behavior matches
// httpidem: replay with Idempotency-Replayed, 409 + Retry-After for
// in-flight duplicates, 422 for payload mismatches, the §4.6 store
// failure semantics, and the §5.1 replay policy.
//
//	r := gin.New()
//	r.Use(ginadapter.New(store, ginadapter.Require(true)))
package ginadapter

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/repenguin22/idemlease"
	"github.com/repenguin22/idemlease/httpidem"
)

// maxStoredResponseOverhead mirrors httpidem: non-body bytes accepted
// on top of MaxResponseBody when decoding a stored payload.
const maxStoredResponseOverhead = 64 << 10

type config struct {
	require         bool
	leaseTTL        time.Duration
	recordTTL       time.Duration
	maxRequestBody  int64
	maxResponseBody int
	bypassLargeBody bool
	policy          httpidem.ReplayPolicy
	errorWriter     httpidem.ErrorWriter
	failOpen        bool
	methods         map[string]bool
	storeHeaders    []string
	keyScope        func(*gin.Context) string
	keyValidator    func(string) bool
	logger          *slog.Logger
	hashKeysInLogs  bool
	replayedHeader  bool
}

// Option configures the middleware returned by New. The options mirror
// httpidem's; see that package for the full semantics.
type Option func(*config)

// Require rejects requests without an Idempotency-Key with 400
// (default false: they pass through).
func Require(v bool) Option { return func(c *config) { c.require = v } }

// LeaseTTL bounds a single in-flight execution (zero: core default).
func LeaseTTL(d time.Duration) Option { return func(c *config) { c.leaseTTL = d } }

// RecordTTL bounds how long a stored response is replayed (zero: core default).
func RecordTTL(d time.Duration) Option { return func(c *config) { c.recordTTL = d } }

// MaxRequestBody caps the buffered request body (default 1 MiB).
func MaxRequestBody(n int64) Option { return func(c *config) { c.maxRequestBody = n } }

// MaxResponseBody caps the captured response body (default 1 MiB).
func MaxResponseBody(n int) Option { return func(c *config) { c.maxResponseBody = n } }

// BypassLargeBody passes oversized requests through without idempotency
// instead of rejecting with 413.
func BypassLargeBody(v bool) Option { return func(c *config) { c.bypassLargeBody = v } }

// Policy replaces the status-driven default ReplayPolicy.
func Policy(p httpidem.ReplayPolicy) Option { return func(c *config) { c.policy = p } }

// Errors replaces the default RFC 9457 ErrorWriter.
func Errors(ew httpidem.ErrorWriter) Option { return func(c *config) { c.errorWriter = ew } }

// FailOpen passes requests through when Begin fails against the store
// (default false: 503).
func FailOpen(v bool) Option { return func(c *config) { c.failOpen = v } }

// Methods replaces the protected method set (default POST and PATCH).
func Methods(methods ...string) Option {
	return func(c *config) {
		c.methods = make(map[string]bool, len(methods))
		for _, m := range methods {
			c.methods[strings.ToUpper(m)] = true
		}
	}
}

// StoreHeaders replaces the replayed-header allowlist (default
// Content-Type, Content-Language, Content-Encoding,
// Content-Disposition, Location). Always-excluded headers stay excluded.
func StoreHeaders(names ...string) Option {
	return func(c *config) { c.storeHeaders = append([]string(nil), names...) }
}

// KeyScope namespaces keys by caller, e.g. an authenticated tenant ID
// taken from the Gin context. Strongly recommended for multi-tenant
// APIs (§4.5).
func KeyScope(fn func(*gin.Context) string) Option {
	return func(c *config) { c.keyScope = fn }
}

// KeyValidator adds validation after grammar parsing (rejects with 400).
func KeyValidator(fn func(string) bool) Option {
	return func(c *config) { c.keyValidator = fn }
}

// Logger replaces slog.Default() for warning logs.
func Logger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// HashKeysInLogs logs SHA-256 digests instead of raw keys and scopes.
func HashKeysInLogs(v bool) Option { return func(c *config) { c.hashKeysInLogs = v } }

// ReplayedHeader controls the Idempotency-Replayed marker (default on).
func ReplayedHeader(v bool) Option { return func(c *config) { c.replayedHeader = v } }

// New builds the idempotency middleware for Gin around store.
func New(store idemlease.Store, opts ...Option) gin.HandlerFunc {
	cfg := &config{
		maxRequestBody:  httpidem.DefaultMaxRequestBody,
		maxResponseBody: httpidem.DefaultMaxResponseBody,
		policy:          httpidem.DefaultPolicy,
		errorWriter:     httpidem.DefaultErrorWriter,
		methods:         map[string]bool{http.MethodPost: true, http.MethodPatch: true},
		storeHeaders:    []string{"Content-Type", "Content-Language", "Content-Encoding", "Content-Disposition", "Location"},
		logger:          slog.Default(),
		replayedHeader:  true,
	}
	for _, o := range opts {
		o(cfg)
	}
	a := &adapter{store: store, cfg: cfg}
	return a.handle
}

type adapter struct {
	store idemlease.Store
	cfg   *config
}

func (a *adapter) reject(c *gin.Context, status int, err error) {
	a.cfg.errorWriter.WriteError(c.Writer, c.Request, status, err)
	c.Abort()
}

func (a *adapter) handle(c *gin.Context) {
	cfg := a.cfg
	r := c.Request
	if !cfg.methods[strings.ToUpper(r.Method)] {
		c.Next()
		return
	}

	key, err := httpidem.KeyFromHeader(r.Header)
	if err != nil {
		if errors.Is(err, httpidem.ErrKeyMissing) && !cfg.require {
			c.Next()
			return
		}
		a.reject(c, http.StatusBadRequest, err)
		return
	}
	if cfg.keyValidator != nil && !cfg.keyValidator(key) {
		a.reject(c, http.StatusBadRequest, fmt.Errorf("%w: rejected by KeyValidator", httpidem.ErrKeyInvalid))
		return
	}

	body, overflow, err := readBody(r, cfg.maxRequestBody)
	if err != nil {
		a.reject(c, http.StatusBadRequest, fmt.Errorf("ginadapter: reading request body: %w", err))
		return
	}
	if overflow {
		if !cfg.bypassLargeBody {
			a.reject(c, http.StatusRequestEntityTooLarge, httpidem.ErrRequestBodyTooLarge)
			return
		}
		cfg.logger.Warn("ginadapter: request body exceeds MaxRequestBody; bypassing idempotency",
			a.keyAttrs("", key)...)
		r.Body = readCloser{io.MultiReader(bytes.NewReader(body), r.Body), r.Body}
		c.Next()
		return
	}
	if r.Body != nil {
		_ = r.Body.Close()
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))

	fingerprint := httpidem.Fingerprint(r.Method, r.URL, body)
	scope, storeKey := "", key
	if cfg.keyScope != nil {
		if scope = cfg.keyScope(c); scope != "" {
			storeKey = scope + "\x00" + key
		}
	}
	o := idemlease.Options{LeaseTTL: cfg.leaseTTL, RecordTTL: cfg.recordTTL}

	out, err := idemlease.Begin(r.Context(), a.store, storeKey, fingerprint, o)
	if err != nil {
		if cfg.failOpen {
			cfg.logger.Warn("ginadapter: store unavailable; proceeding without idempotency (FailOpen)",
				append(a.keyAttrs(scope, key), slog.Any("error", err))...)
			c.Next()
			return
		}
		a.reject(c, http.StatusServiceUnavailable, fmt.Errorf("%w: %w", httpidem.ErrStoreUnavailable, err))
		return
	}

	switch out.Action {
	case idemlease.Replay:
		a.writeReplay(c, scope, key, out.Payload)
		return
	case idemlease.RejectInFlight:
		c.Writer.Header().Set("Retry-After", retryAfterSeconds(out.RetryAfter))
		a.reject(c, http.StatusConflict, httpidem.ErrInFlight)
		return
	case idemlease.RejectFingerprintMismatch:
		a.reject(c, http.StatusUnprocessableEntity, httpidem.ErrFingerprintMismatch)
		return
	}

	// Proceed: run the rest of the chain under capture, then Finish.
	rec := httpidem.NewRecorder(cfg.maxResponseBody, cfg.storeHeaders)
	orig := c.Writer
	cw := &captureWriter{ResponseWriter: orig, rec: rec}
	c.Writer = cw
	ctx, handlerErr := httpidem.ErrorChannel(r.Context())
	ctx = httpidem.ContextWithReservation(ctx, httpidem.Reservation{Key: storeKey, Token: out.Token, Options: o.Effective()})
	ctx, finished := httpidem.FinishChannel(ctx)
	c.Request = r.WithContext(ctx)
	finishCtx := context.WithoutCancel(ctx)

	defer func() {
		if p := recover(); p != nil {
			if !finished() {
				if _, ferr := idemlease.Finish(finishCtx, a.store, storeKey, out.Token, idemlease.Discard, nil, o); ferr != nil {
					cfg.logger.Warn("ginadapter: releasing reservation after handler panic failed",
						append(a.keyAttrs(scope, key), slog.Any("error", ferr))...)
				}
			}
			panic(p)
		}
	}()
	c.Next()
	cw.finalize()
	c.Writer = orig

	if finished() {
		// The handler finalized the record itself (transactional join).
		return
	}

	decision := cfg.policy.Decide(rec.Status(), handlerErr())
	if decision == idemlease.Persist {
		switch {
		case rec.Abandoned():
			decision = idemlease.Discard
			cfg.logger.Warn("ginadapter: response was streamed (Flush/Hijack); discarding instead of persisting",
				a.keyAttrs(scope, key)...)
		case rec.Overflowed():
			decision = idemlease.Discard
			cfg.logger.Warn("ginadapter: response exceeds MaxResponseBody; discarding instead of persisting",
				a.keyAttrs(scope, key)...)
		}
	}

	var payload []byte
	if decision == idemlease.Persist {
		sr, _ := rec.StoredResponse()
		payload, err = sr.MarshalBinary()
		if err != nil {
			decision = idemlease.Discard
			cfg.logger.Warn("ginadapter: encoding stored response failed; discarding",
				append(a.keyAttrs(scope, key), slog.Any("error", err))...)
		}
	}

	leaseLost, err := idemlease.Finish(finishCtx, a.store, storeKey, out.Token, decision, payload, o)
	switch {
	case err != nil && decision == idemlease.Persist:
		cfg.logger.Warn("ginadapter: persisting response failed; idempotency temporarily weakened",
			append(a.keyAttrs(scope, key), slog.Any("error", err))...)
		if _, rerr := idemlease.Finish(finishCtx, a.store, storeKey, out.Token, idemlease.Discard, nil, o); rerr != nil {
			cfg.logger.Warn("ginadapter: best-effort release failed; key stays reserved until lease expiry",
				append(a.keyAttrs(scope, key), slog.Any("error", rerr))...)
		}
	case err != nil:
		cfg.logger.Warn("ginadapter: releasing reservation failed; key stays reserved until lease expiry",
			append(a.keyAttrs(scope, key), slog.Any("error", err))...)
	case leaseLost:
		cfg.logger.Warn("ginadapter: lease lost during execution; response returned but not stored for replay",
			append(a.keyAttrs(scope, key), slog.String("event", "lease_lost"))...)
	}
}

func (a *adapter) writeReplay(c *gin.Context, scope, key string, payload []byte) {
	if len(payload) > a.cfg.maxResponseBody+maxStoredResponseOverhead {
		a.cfg.logger.Error("ginadapter: stored payload exceeds the configured response limits; refusing to replay",
			append(a.keyAttrs(scope, key), slog.Int("payload_bytes", len(payload)))...)
		a.reject(c, http.StatusInternalServerError, errors.New("ginadapter: corrupted stored response payload"))
		return
	}
	var sr httpidem.StoredResponse
	if err := sr.UnmarshalBinary(payload); err != nil {
		a.cfg.logger.Error("ginadapter: stored response payload is corrupted",
			append(a.keyAttrs(scope, key), slog.Any("error", err))...)
		a.reject(c, http.StatusInternalServerError, err)
		return
	}
	header := c.Writer.Header()
	for name, values := range sr.Header {
		header[name] = append([]string(nil), values...)
	}
	if a.cfg.replayedHeader {
		header.Set(httpidem.HeaderIdempotencyReplayed, "true")
	}
	c.Writer.WriteHeader(sr.StatusCode)
	_, _ = c.Writer.Write(sr.Body)
	c.Abort()
}

func (a *adapter) keyAttrs(scope, key string) []any {
	if a.cfg.hashKeysInLogs {
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

type readCloser struct {
	io.Reader
	io.Closer
}

func retryAfterSeconds(d time.Duration) string {
	secs := (d + time.Second - 1) / time.Second
	if secs < 1 {
		secs = 1
	}
	return strconv.FormatInt(int64(secs), 10)
}

// captureWriter feeds the Recorder while delegating to Gin's writer.
// It implements the full gin.ResponseWriter interface via embedding.
//
// Gin defers the actual header write: WriteHeader(code) only records
// the status, and headers stay mutable until the first body write (this
// is how c.JSON sets Content-Type after c.Status). The capture snapshot
// therefore happens at the first body write (or finalize), mirroring
// the moment Gin itself freezes the headers.
type captureWriter struct {
	gin.ResponseWriter
	rec         *httpidem.Recorder
	pending     int
	snapshotted bool
}

func (w *captureWriter) WriteHeader(code int) {
	if !w.snapshotted && code >= 200 {
		w.pending = code // deferred, like Gin: headers may still change
	}
	w.ResponseWriter.WriteHeader(code)
}

// snapshot freezes status and headers into the Recorder.
func (w *captureWriter) snapshot() {
	if w.snapshotted {
		return
	}
	code := w.pending
	if code == 0 {
		code = http.StatusOK
	}
	w.rec.RecordStatus(code, w.Header())
	w.snapshotted = true
}

func (w *captureWriter) Write(p []byte) (int, error) {
	w.snapshot()
	w.rec.RecordBody(p)
	return w.ResponseWriter.Write(p)
}

func (w *captureWriter) WriteString(s string) (int, error) {
	w.snapshot()
	w.rec.RecordBody([]byte(s))
	return w.ResponseWriter.WriteString(s)
}

// WriteHeaderNow is Gin's explicit header flush; the headers are final
// at this point.
func (w *captureWriter) WriteHeaderNow() {
	w.snapshot()
	w.ResponseWriter.WriteHeaderNow()
}

func (w *captureWriter) Flush() {
	w.rec.Abandon() // streaming: the capture is void (§6.1)
	w.ResponseWriter.Flush()
}

func (w *captureWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := w.ResponseWriter.Hijack()
	if err == nil {
		w.rec.Abandon()
	}
	return conn, rw, err
}

// finalize captures header-only responses (no body write happened
// inside the chain).
func (w *captureWriter) finalize() {
	w.snapshot()
}
