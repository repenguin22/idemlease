// Package errtrailadapter integrates idemlease's httpidem middleware
// with the errtrail error library (REQUIREMENTS §9.1).
//
// It provides two plugins for httpidem.New (and the equivalent
// ginadapter options), assembled entirely from httpidem's public
// interfaces:
//
//   - Errors returns an httpidem.ErrorWriter that maps the middleware's
//     rejection sentinels to errtrail codes and responds with
//     problem.Write (RFC 9457).
//   - Policy returns an httpidem.ReplayPolicy that decides persist vs.
//     discard from the errtrail Code a handler reports via
//     httpidem.SetError, falling back to the status-driven default for
//     responses without an errtrail classification.
//
// Wire both at once with Options:
//
//	mw := httpidem.New(store, errtrailadapter.Options()...)
//
// The package registers errtrail codes 1100-1104 (names
// IDEMPOTENCY_*) from an init function; do not register those codes or
// names elsewhere.
package errtrailadapter

import (
	"errors"
	"net/http"

	"github.com/repenguin22/errtrail"
	"github.com/repenguin22/errtrail/problem"
	"github.com/repenguin22/idemlease"
	"github.com/repenguin22/idemlease/httpidem"
)

// Custom errtrail codes for idempotency rejections. errtrail requires
// custom codes to be >= 100 and uniquely named; this package reserves
// the 1100-1104 block and the IDEMPOTENCY_* names. Each is registered
// with the same HTTP status the middleware assigns the corresponding
// sentinel, so problem.Write (which derives status from the code)
// reproduces it exactly.
const (
	CodeKeyMissing      errtrail.Code = 1100
	CodeKeyInvalid      errtrail.Code = 1101
	CodeInFlight        errtrail.Code = 1102
	CodePayloadMismatch errtrail.Code = 1103
	CodeBodyTooLarge    errtrail.Code = 1104
)

func init() {
	errtrail.Register(CodeKeyMissing, "IDEMPOTENCY_KEY_MISSING",
		http.StatusBadRequest, errtrail.InvalidArgument.GRPCCode())
	errtrail.Register(CodeKeyInvalid, "IDEMPOTENCY_KEY_INVALID",
		http.StatusBadRequest, errtrail.InvalidArgument.GRPCCode())
	errtrail.Register(CodeInFlight, "IDEMPOTENCY_IN_FLIGHT",
		http.StatusConflict, errtrail.Aborted.GRPCCode(), errtrail.Retryable())
	errtrail.Register(CodePayloadMismatch, "IDEMPOTENCY_PAYLOAD_MISMATCH",
		http.StatusUnprocessableEntity, errtrail.FailedPrecondition.GRPCCode())
	errtrail.Register(CodeBodyTooLarge, "IDEMPOTENCY_BODY_TOO_LARGE",
		http.StatusRequestEntityTooLarge, errtrail.ResourceExhausted.GRPCCode())
}

// Options bundles Errors and Policy as httpidem options, the usual way
// to enable the adapter:
//
//	mw := httpidem.New(store, errtrailadapter.Options()...)
//
// Append your own options as needed; later options win, so place any
// override after Options()....
func Options() []httpidem.Option {
	return []httpidem.Option{
		httpidem.Errors(Errors()),
		httpidem.Policy(Policy()),
	}
}

// Errors returns an httpidem.ErrorWriter that classifies the middleware
// sentinels into errtrail codes and responds with problem.Write. Its
// public messages are safe to expose to clients; internal detail stays
// server-side (errtrail's public/internal separation).
func Errors() httpidem.ErrorWriter { return errorWriter{} }

type errorWriter struct{}

func (errorWriter) WriteError(w http.ResponseWriter, r *http.Request, status int, err error) {
	_ = problem.Write(w, classify(status, err))
}

// classify maps an httpidem rejection to an errtrail error. The
// registered HTTP status of each code matches the status the middleware
// passes in, so problem.Write reproduces the intended status.
func classify(status int, err error) error {
	switch {
	case errors.Is(err, httpidem.ErrKeyMissing):
		return errtrail.New(CodeKeyMissing, "idempotency key missing").
			WithPublic("The Idempotency-Key header is required.")
	case errors.Is(err, httpidem.ErrKeyInvalid):
		return errtrail.New(CodeKeyInvalid, "idempotency key invalid").
			WithPublic("The Idempotency-Key header is malformed.")
	case errors.Is(err, httpidem.ErrInFlight):
		return errtrail.New(CodeInFlight, "duplicate request in flight").
			WithPublic("A request with this Idempotency-Key is being processed. Retry later.")
	case errors.Is(err, httpidem.ErrFingerprintMismatch):
		return errtrail.New(CodePayloadMismatch, "idempotency payload mismatch").
			WithPublic("This Idempotency-Key was already used with a different request.")
	case errors.Is(err, httpidem.ErrRequestBodyTooLarge):
		return errtrail.New(CodeBodyTooLarge, "request body too large for idempotency").
			WithPublic("The request body is too large.")
	case errors.Is(err, httpidem.ErrStoreUnavailable):
		return errtrail.New(errtrail.Unavailable, "idempotency store unavailable").
			WithPublic("The service is temporarily unavailable. Retry later.")
	default:
		// Not a middleware sentinel (e.g. a corrupted stored payload,
		// reported as 500): keep the intended status via the code that
		// maps to it, and don't leak internal detail.
		code := errtrail.Unknown
		if status >= 400 && status < 500 {
			code = errtrail.InvalidArgument
		}
		e := errtrail.New(code, "idempotency rejection")
		if err != nil {
			return errtrail.Wrap(e, err.Error())
		}
		return e
	}
}

// Policy returns an httpidem.ReplayPolicy driven by the errtrail Code
// the handler reported via httpidem.SetError: retryable codes (per the
// errtrail registry — the decision table) are discarded so a retry
// re-executes, everything else is persisted and replayed. Responses
// without an errtrail classification fall back to the status-driven
// default (httpidem.DefaultPolicy).
func Policy() httpidem.ReplayPolicy {
	return httpidem.ReplayPolicyFunc(func(status int, err error) idemlease.Decision {
		if err == nil {
			return httpidem.DefaultPolicy.Decide(status, nil)
		}
		code := errtrail.CodeOf(err)
		if code == errtrail.Unknown {
			return httpidem.DefaultPolicy.Decide(status, err)
		}
		if code.Retryable() {
			return idemlease.Discard
		}
		return idemlease.Persist
	})
}
