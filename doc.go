// Package idemlease implements a lease-based idempotency state machine
// for retry-safe APIs, with zero dependencies beyond the Go standard
// library (and no net/http in the core).
//
// The core treats idempotency keys, request fingerprints, and stored
// payloads as opaque values. It exposes two entry points, Begin and
// Finish, on top of a pluggable Store. HTTP-specific concerns (header
// grammar, fingerprinting, response capture, middleware) live in the
// httpidem subpackage, whose middleware is the default entry point for
// most users.
//
// Guarantee: for a given key (within a scope), at most one execution
// holds a valid lease at any time. Exactly-once execution is NOT
// guaranteed; see the README for details.
package idemlease
