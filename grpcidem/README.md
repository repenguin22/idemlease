# grpcidem

Idempotency-Key semantics for unary gRPC servers, backed by the
idemlease state machine (module
`github.com/repenguin22/idemlease/grpcidem`).

It shares the core and stores with the HTTP integration but depends only
on `idemlease`, `grpc`, and `protobuf` — never `net/http`.

```go
srv := grpc.NewServer(grpc.UnaryInterceptor(
	grpcidem.UnaryServerInterceptor(store,
		grpcidem.Require(true),
		grpcidem.KeyScope(tenantFromContext),
	),
))
```

- **Key** — from `idempotency-key` request metadata (raw string, 1–255
  bytes, no control bytes). Missing key follows `Require`; malformed key
  → `InvalidArgument`.
- **Fingerprint** — `SHA-256(fullMethod + "\n" + marshaled request)`.
- **Replay** — the response message's proto full name and bytes are
  stored; on replay the type is recovered from the global proto registry
  (no per-method registration), and the response header metadata carries
  `idempotency-replayed: true`.
- **Rejections** — in-flight duplicate → `Aborted`; same key with a
  different request → `FailedPrecondition`; store unavailable →
  `Unavailable` (unless `FailOpen`).

## Replay semantics

Only **successful** responses are stored and replayed. A gRPC error is a
status code, not a response message, so an errored call has nothing to
replay and **re-executes on retry** — transient failures are never
frozen into a replay. A custom `Policy` governs whether a successful
response is persisted.

Recovery from a handler panic is the outer interceptor's job (as in
net/http): grpcidem releases the reservation and re-panics, so chain a
recovery interceptor around it.
