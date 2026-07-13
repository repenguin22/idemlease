package httpidem

import (
	"encoding/json"
	"errors"
	"net/http"
)

// Sentinel errors passed to ErrorWriter; all are matchable with
// errors.Is even when wrapped.
var (
	// ErrKeyMissing: the Idempotency-Key header is absent on a request
	// that requires one (400).
	ErrKeyMissing = errors.New("httpidem: Idempotency-Key header is missing")
	// ErrKeyInvalid: the header violates the key grammar (§4.1) or was
	// rejected by KeyValidator (400).
	ErrKeyInvalid = errors.New("httpidem: Idempotency-Key header is invalid")
	// ErrInFlight: another request with the same key holds a valid
	// lease (409).
	ErrInFlight = errors.New("httpidem: a request with this Idempotency-Key is in flight")
	// ErrFingerprintMismatch: the key was already completed by a request
	// with a different fingerprint (422).
	ErrFingerprintMismatch = errors.New("httpidem: Idempotency-Key was already used with a different request")
	// ErrRequestBodyTooLarge: the request body exceeds MaxRequestBody
	// and BypassLargeBody is off (413).
	ErrRequestBodyTooLarge = errors.New("httpidem: request body exceeds MaxRequestBody")
	// ErrStoreUnavailable: Begin failed against the store and FailOpen
	// is off (503).
	ErrStoreUnavailable = errors.New("httpidem: idempotency store is unavailable")
)

// ErrorWriter writes rejection responses. status is the HTTP status the
// middleware decided on; err is one of the package sentinels (possibly
// wrapped with detail) and can be classified with errors.Is.
type ErrorWriter interface {
	WriteError(w http.ResponseWriter, r *http.Request, status int, err error)
}

// DefaultErrorWriter is the default ErrorWriter: a minimal RFC 9457
// problem document. Adapters can reuse it to match the middleware's
// rejection format.
var DefaultErrorWriter ErrorWriter = problemWriter{}

// problemWriter is the default ErrorWriter: a minimal RFC 9457 problem
// document built with the standard library only.
type problemWriter struct{}

func (problemWriter) WriteError(w http.ResponseWriter, r *http.Request, status int, err error) {
	p := struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Status int    `json:"status"`
		Detail string `json:"detail,omitempty"`
	}{
		Type:   "about:blank",
		Title:  http.StatusText(status),
		Status: status,
	}
	// 5xx errors carry infrastructure detail (store addresses etc.)
	// that must stay server-side.
	if err != nil && status < 500 {
		p.Detail = err.Error()
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(p)
}
