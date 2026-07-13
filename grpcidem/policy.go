package grpcidem

import (
	"github.com/repenguin22/idemlease"
)

// ReplayPolicy decides whether a unary handler's outcome is persisted
// for replay or discarded, from the error it returned (nil on success).
//
// Only successful responses are storable: a gRPC error is a status
// code, not a response message, so an errored call has nothing to
// replay and always re-executes on retry regardless of the policy. The
// policy therefore governs successful responses — return Discard from a
// custom policy to keep a particular success from being cached.
type ReplayPolicy interface {
	Decide(err error) idemlease.Decision
}

// ReplayPolicyFunc adapts a function to ReplayPolicy.
type ReplayPolicyFunc func(err error) idemlease.Decision

// Decide implements ReplayPolicy.
func (f ReplayPolicyFunc) Decide(err error) idemlease.Decision { return f(err) }

// DefaultPolicy persists successful responses and discards everything
// else. Combined with the storable-response rule above, this means a
// successful call replays and any errored call re-executes on retry —
// so transient failures (Unavailable, ResourceExhausted, …) are never
// frozen into a replay, and deterministic ones simply run again.
var DefaultPolicy ReplayPolicy = ReplayPolicyFunc(defaultDecide)

func defaultDecide(err error) idemlease.Decision {
	if err == nil {
		return idemlease.Persist
	}
	return idemlease.Discard
}
