package httpidem

import (
	"context"
	"net/http"
	"sync"

	"github.com/repenguin22/idemlease"
)

// Reservation identifies the lease the middleware acquired for the
// current request (§9.3). Key is the store key — KeyScope-composed, so
// handlers never assemble scopes themselves — and Token is the
// reservation token. Handlers pass both to a transactional completer
// such as pgstore.CompleteTx, together with Options.RecordTTL.
type Reservation struct {
	Key     string
	Token   string
	Options idemlease.Options
}

type reservationKey struct{}

// ContextWithReservation returns a context carrying rsv. The middleware
// installs it automatically on Proceed; framework adapters do the same
// before invoking their handlers.
func ContextWithReservation(ctx context.Context, rsv Reservation) context.Context {
	return context.WithValue(ctx, reservationKey{}, rsv)
}

// ReservationFromContext returns the reservation the middleware (or an
// adapter) acquired for this request. ok is false when the request is
// not running under an idempotency lease — a keyless pass-through, a
// bypassed large body, or a handler outside the middleware.
func ReservationFromContext(ctx context.Context) (rsv Reservation, ok bool) {
	rsv, ok = ctx.Value(reservationKey{}).(Reservation)
	return rsv, ok
}

// MarkFinished records that the handler finalized the idempotency
// record itself — typically by committing pgstore.CompleteTx inside its
// business transaction — so the middleware must not Persist or Discard
// on its behalf. Call it after the transaction commits, before writing
// the response. Forgetting it is harmless but noisy: the middleware's
// own Persist then fails the token CAS and logs a spurious lease_lost
// warning (design matrix T7).
func MarkFinished(ctx context.Context) {
	if b, ok := ctx.Value(finishedKey{}).(*finishedBox); ok {
		b.mark()
	}
}

// FinishChannel returns a derived context under which MarkFinished
// records the handler-side completion, plus a function reporting
// whether it was called. The middleware installs this automatically;
// framework adapters call it before invoking their handlers and skip
// their own Finish when the report function returns true.
func FinishChannel(ctx context.Context) (context.Context, func() bool) {
	box := &finishedBox{}
	return context.WithValue(ctx, finishedKey{}, box), box.done
}

type finishedKey struct{}

type finishedBox struct {
	mu       sync.Mutex
	finished bool
}

func (b *finishedBox) mark() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.finished = true
}

func (b *finishedBox) done() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.finished
}

// WriteStored writes sr to w exactly as a replay would: headers, then
// status, then body. Handlers that persist their response through a
// transactional completer respond with it, so the live response and
// every future replay are byte-identical. It does not filter headers;
// build sr with replay-safe headers only (the Recorder allowlist rules
// apply morally, if not mechanically).
func WriteStored(w http.ResponseWriter, sr StoredResponse) error {
	header := w.Header()
	for name, values := range sr.Header {
		header[name] = append([]string(nil), values...)
	}
	w.WriteHeader(sr.StatusCode)
	_, err := w.Write(sr.Body)
	return err
}
