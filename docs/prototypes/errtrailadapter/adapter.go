// Package errtrailadapter is the §9.1 / §12-8 verification prototype:
// it proves that an errtrail integration can be assembled from
// httpidem's public interfaces alone. It is not part of the v1 API
// surface; the production adapter will ship as its own module after v1.
package errtrailadapter

import (
	"errors"
	"net/http"

	"github.com/repenguin22/errtrail"
	"github.com/repenguin22/errtrail/problem"
	"github.com/repenguin22/idemlease"
	"github.com/repenguin22/idemlease/httpidem"
)

// Custom errtrail codes for idempotency rejections (>= 100 per the
// errtrail registry contract; 1100 block picked for this prototype).
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

// Errors returns an httpidem.ErrorWriter that classifies the middleware
// sentinels into errtrail codes and responds with problem.Write.
func Errors() httpidem.ErrorWriter { return errorWriter{} }

type errorWriter struct{}

func (errorWriter) WriteError(w http.ResponseWriter, r *http.Request, status int, err error) {
	_ = problem.Write(w, classify(status, err))
}

// classify maps the httpidem sentinels onto the errtrail taxonomy. The
// registered HTTP statuses mirror the statuses the middleware passes
// in, so problem.Write reproduces them.
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
		// Not one of the middleware sentinels: fall back by status.
		code := errtrail.Unknown
		if status >= 400 && status < 500 {
			code = errtrail.InvalidArgument
		}
		return errtrail.Wrapf(errtrail.New(code, "idempotency rejection"), "%v", err)
	}
}

// Policy returns a ReplayPolicy driven by the errtrail Code the handler
// reported via httpidem.SetError: retryable codes (per the errtrail
// registry — the decision table) are discarded so a retry re-executes,
// everything else is persisted and replayed. Responses without an
// errtrail classification fall back to the status-driven default.
func Policy() httpidem.ReplayPolicy {
	return httpidem.ReplayPolicyFunc(func(status int, err error) idemlease.Decision {
		if err == nil {
			return httpidem.DefaultPolicy.Decide(status, nil)
		}
		code := errtrail.CodeOf(err)
		if code == errtrail.Unknown {
			// No errtrail classification in the chain: status-driven.
			return httpidem.DefaultPolicy.Decide(status, err)
		}
		if code.Retryable() {
			return idemlease.Discard
		}
		return idemlease.Persist
	})
}
