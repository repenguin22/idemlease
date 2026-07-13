package httpidem

import (
	"context"
	"net/http"
	"sync"

	"github.com/repenguin22/idemlease"
)

// ReplayPolicy decides whether a handler outcome is persisted for
// replay or discarded (§5).
type ReplayPolicy interface {
	// Decide receives the response status (200 when the handler wrote
	// nothing, matching net/http's implicit 200) and the error the
	// handler passed to SetError, or nil if it never did. The middleware
	// does not interpret err; it is a channel for custom policies and
	// error-library adapters.
	Decide(status int, err error) idemlease.Decision
}

// ReplayPolicyFunc adapts a function to ReplayPolicy.
type ReplayPolicyFunc func(status int, err error) idemlease.Decision

// Decide implements ReplayPolicy.
func (f ReplayPolicyFunc) Decide(status int, err error) idemlease.Decision {
	return f(status, err)
}

// DefaultPolicy is the status-driven default policy (§5.1):
//
//	2xx / 3xx            Persist
//	4xx except 429       Persist (retries receive the same error)
//	429                  Discard (a stored 429 would replay forever)
//	5xx                  Discard
//
// It ignores SetError notifications. Custom policies and adapters can
// fall back to it for statuses they do not handle themselves.
var DefaultPolicy ReplayPolicy = ReplayPolicyFunc(defaultDecide)

func defaultDecide(status int, _ error) idemlease.Decision {
	switch {
	case status == http.StatusTooManyRequests:
		return idemlease.Discard
	case status >= 500:
		return idemlease.Discard
	default:
		return idemlease.Persist
	}
}

// SetError notifies the ReplayPolicy of the error behind the response
// being written. Call it from a handler running under the middleware;
// the value is handed to ReplayPolicy.Decide untouched. Outside the
// middleware (or an adapter that installed ErrorChannel) it is a no-op.
func SetError(ctx context.Context, err error) {
	if box, ok := ctx.Value(errBoxKey{}).(*errBox); ok {
		box.set(err)
	}
}

// ErrorChannel returns a derived context under which SetError stores
// the handler's error, and a function that reads it back afterwards.
// The middleware installs this automatically; framework adapters (Gin,
// gRPC, ...) call it before invoking their handlers and pass the read
// error to ReplayPolicy.Decide (§6.2).
func ErrorChannel(ctx context.Context) (context.Context, func() error) {
	box := &errBox{}
	return context.WithValue(ctx, errBoxKey{}, box), box.get
}

type errBoxKey struct{}

type errBox struct {
	mu  sync.Mutex
	err error
}

func (b *errBox) set(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.err = err
}

func (b *errBox) get() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}
