# idemlease

[![CI](https://github.com/repenguin22/idemlease/actions/workflows/ci.yml/badge.svg)](https://github.com/repenguin22/idemlease/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/repenguin22/idemlease.svg)](https://pkg.go.dev/github.com/repenguin22/idemlease)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Idempotency-Key middleware for Go HTTP APIs — clients can safely retry
POST/PATCH requests, duplicates are answered from a stored response.
The state machine at the core is dependency-free (stdlib only, no
`net/http`), storage-agnostic, and shared by every integration.

> **The guarantee.** For a given key (within a scope), **at most one
> execution holds a valid lease at any time**. Exactly-once execution is
> *not* guaranteed: a handler may run more than once after a lease
> expires, or when persisting the result fails and the request is
> retried. For a stronger guarantee, `pgstore` can join your business
> transaction atomically — see Transactional join below.

The semantics follow the pattern proven in production by payment APIs
(Stripe, Adyen, WorldPay): reserve on first sight, replay after
completion, reject concurrent duplicates with 409 and conflicting
payloads with 422.

Japanese version: [docs/README.ja.md](docs/README.ja.md)

## Quick start

```
go get github.com/repenguin22/idemlease
```

```go
package main

import (
	"net/http"

	"github.com/repenguin22/idemlease/httpidem"
	"github.com/repenguin22/idemlease/memstore"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", createOrder)

	mw := httpidem.New(memstore.New(), // dev only — see Stores below
		httpidem.Require(true), // reject POST/PATCH without a key
	)
	http.ListenAndServe(":8080", mw(mux))
}
```

For production, use the Redis-backed store (separate module, works with
Redis 6.0+ and Valkey, cluster-safe):

```
go get github.com/repenguin22/idemlease/redistore
```

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
mw := httpidem.New(redistore.New(client), httpidem.Require(true))
```

## What clients observe

| Request | Response |
|---|---|
| First request with an `Idempotency-Key` | Handler runs; response stored |
| Retry: same key, same request | Stored response + `Idempotency-Replayed: true` — handler does **not** run again |
| Same key while the first is still running | `409 Conflict` + `Retry-After` (regardless of payload) |
| Same key, *different* payload, after completion | `422 Unprocessable Entity` |
| No key with `Require(true)` | `400 Bad Request` (default `Require(false)`: passes through untouched) |
| Malformed key | `400 Bad Request` |
| Idempotency store unreachable | `503 Service Unavailable` (see Failure semantics) |

Rejections are minimal RFC 9457 `application/problem+json` documents;
plug in your own format via `httpidem.Errors(ErrorWriter)`. The
sentinels (`ErrInFlight`, `ErrFingerprintMismatch`, `ErrKeyMissing`,
`ErrKeyInvalid`, …) are matchable with `errors.Is`.

## Keys

The canonical key form is the **raw, unquoted header value**: after
trimming surrounding whitespace, any byte sequence of 1–255 bytes
without control characters — internal spaces and UTF-8 included:

```
Idempotency-Key: 123e4567-e89b-12d3-a456-426614174000
```

RFC 8941 quoted strings (`Idempotency-Key: "abc"`) are accepted for
compatibility with draft-07-style clients and address the **same key**
as the raw form (`abc`). Known trade-off: a raw key that itself starts
with `"` must be sent in the quoted form. Add stricter rules (e.g.
UUID-only) with `httpidem.KeyValidator`.

Two requests are "the same request" when their **fingerprint** matches:
`SHA-256(method + "\n" + path + "?" + query + "\n" + body)`. Host and
headers are deliberately excluded.

## Security: scope your keys

Keys are global by default. On a multi-tenant API that means client A
can send client B's key and **receive B's stored response**, or probe
key usage via 409/422. If your API serves more than one caller,
configuring `KeyScope` is **strongly recommended**:

```go
httpidem.KeyScope(func(r *http.Request) string {
	return tenantIDFromAuth(r) // e.g. the authenticated account ID
})
```

The scope is joined to the key internally (`scope + "\x00" + key`), so
equal keys from different tenants never collide, and each tenant keeps
normal replay/409/422 behavior within its own scope.

## What is stored and replayed

Status code, allowlisted headers, and the body — up to
`MaxResponseBody` (default 1 MiB). The default header allowlist is
`Content-Type`, `Content-Language`, `Content-Encoding`,
`Content-Disposition`, `Location`; change it with `StoreHeaders(...)`,
but keep the headers a client needs to *interpret* the body
(`Content-Encoding` above all) or replays of compressed bodies arrive
unlabelled and corrupt. **`Set-Cookie`, `Authorization`, and hop-by-hop
headers are never replayed, even if you allowlist them** — replaying
one client's session to another would be a credential leak.

Compression: prefer placing compression middleware *outside* idemlease,
so each response (including replays) is compressed per-request. If the
handler writes pre-compressed bodies instead, they are stored and
replayed as-is — note a retry with a different `Accept-Encoding` still
receives the originally stored encoding.

Not stored (delivered to the client, then discarded):

- Streaming responses — the capture is abandoned the moment the handler
  calls `Flush` or `Hijack`
- Responses larger than `MaxResponseBody`
- Whatever the replay policy says to discard (next section)

Handlers must finish writing before they return: like net/http itself,
the capture is not safe against writes from background goroutines that
outlive the handler.

## Replay policy

The default is status-driven:

| Handler result | Decision |
|---|---|
| 2xx / 3xx | Persist |
| 4xx except 429 | Persist — retries get the same error |
| **429** | **Discard** — a stored 429 would replay forever after the limit lifts |
| 5xx | Discard |
| panic | Discard, then re-panic (recovery belongs to outer middleware) |

A handler that writes nothing counts as 200 (net/http's implicit 200).
For decisions based on *why* the handler failed, call
`httpidem.SetError(ctx, err)` in the handler and supply a custom
`httpidem.Policy` — the error is passed through uninterpreted, which is
how error-library adapters hook in.

## Failure semantics — read this before production

- **Reserve fails (store down):** fail-closed `503` by default.
  `FailOpen(true)` passes requests through instead — **without any
  idempotency** — logging a warning. Pick your poison explicitly.
- **Persist fails (store down after the handler ran):** the client
  still receives the handler's response, but it could not be stored.
  **Idempotency is temporarily weakened**: the middleware attempts a
  best-effort release so a retry can re-execute; if that also fails,
  retries see 409 until the lease expires, then re-execute.
- **Discard does not mean "nothing happened".** Releasing a key only
  permits re-execution; any side effects your handler already performed
  are yours. If a retry must not double-charge, make the handler itself
  defensive — or wait for v1.1's transactional store, which closes this
  gap by joining the business transaction.
- **Lease expiry during a long execution:** if the lease expires and a
  retry re-reserves the key, the original execution still returns its
  own response to its own client, but only the *new* execution's result
  is stored and replayed. A `lease_lost` warning is logged. Size
  `LeaseTTL` (default 30s) above your slowest handler.

Logs go to `slog` with an `idempotency_key` attribute on every line
(plus the scope when set); enable `HashKeysInLogs(true)` if keys may
contain sensitive material.

## Stores

| Store | Module | Use |
|---|---|---|
| `memstore` | (this module) | Development and tests. **Single-process only** — instances behind a load balancer each see their own records, so the guarantee does not hold across them. |
| `redistore` | `github.com/repenguin22/idemlease/redistore` | Production. Redis 6.0+ or compatible (Valkey is exercised in CI). Atomic Lua reserve/complete/release, cluster-safe. |
| `pgstore` | `github.com/repenguin22/idemlease/pgstore` | Production. PostgreSQL (database/sql, driver of your choice); DB-clock expiry authority, `Sweep` for expired-row GC, and optional transactional join via `CompleteTx` (below). |

Implementing your own store is supported and encouraged: satisfy
`idemlease.Store` (four methods) and validate with the shipped
conformance suites — `idemleasetest.RunStoreTests`,
`idemleasetest.RunStateMachineTests`, and
`httpidemtest.RunHTTPTests`. They pin atomicity, expiry, token CAS, and
the full client-observable behavior. The suites depend on the standard
library only.

## Transactional join (pgstore)

With the PostgreSQL store, a handler can complete the idempotency
record *inside its own business transaction*, which upgrades the
guarantee:

> With `pgstore` + `CompleteTx`, **the business transaction for a given
> key commits at most once while the completed record is valid**. Racing
> executions serialize on the record's row lock; the loser's transaction
> — business writes included — rolls back. Outside the guarantee remain
> re-execution after the record TTL expires, and side effects that live
> outside the database.

```go
rsv, _ := httpidem.ReservationFromContext(r.Context())
tx, _ := db.BeginTx(ctx, nil)
defer tx.Rollback()

// ... business writes on tx ...

sr := httpidem.StoredResponse{StatusCode: 201, Header: hdr, Body: body}
payload, _ := sr.MarshalBinary()
leaseLost, err := store.CompleteTx(ctx, tx, rsv.Key, rsv.Token, payload, rsv.Options.RecordTTL)
if err != nil { /* 500 */ }
if leaseLost { /* another execution owns the key: keep the rollback, answer 409 */ }
if err := tx.Commit(); err != nil { /* 500 */ }

httpidem.MarkFinished(r.Context()) // the middleware must not Finish again
_ = httpidem.WriteStored(w, sr)    // respond with exactly what replays will serve
```

A crash after commit but before the response reaches the client is
precisely the gap this closes: the retry replays the committed
response. A rollback (or crash before commit) leaves the reservation to
its lease — retries see 409 until it expires, then re-execute. The full
success/failure matrix and its acceptance tests live in
[docs/design/pgstore-txjoin.md](docs/design/pgstore-txjoin.md) (Japanese).

## Framework support

- **net/http, chi, gorilla/mux** — the middleware is a plain
  `func(http.Handler) http.Handler`; use it as-is.
- **Echo** — wrap it: `e.Use(echo.WrapMiddleware(mw))`. Verified end to
  end (capture and Flush detection through Echo's response wrapper) in
  [e2e/echoe2e](e2e/echoe2e).
- **Gin** — dedicated adapter (module
  `github.com/repenguin22/idemlease/ginadapter`), assembled from the
  exported parts plus the core `Begin`/`Finish` and aware of Gin's
  deferred header writes:

  ```go
  r := gin.New()
  r.Use(ginadapter.New(store, ginadapter.Require(true)))
  ```

- **fasthttp / Fiber** — not supported.

Per-route enforcement: `Require` is global by design; to protect only
some routes, apply the middleware to those routes or groups.

## Error-library integration

Rejections and replay decisions are pluggable via
`httpidem.Errors(ErrorWriter)` and `httpidem.Policy(ReplayPolicy)`. The
[errtrail](https://github.com/repenguin22/errtrail) integration ships as
`github.com/repenguin22/idemlease/errtrailadapter`:

```go
mw := httpidem.New(store, errtrailadapter.Options()...) // errtrail problem+json + Code-driven replay
```

It maps the rejection sentinels to errtrail codes (RFC 9457 responses)
and lets a handler steer replay by reporting an errtrail `Code` through
`httpidem.SetError` — retryable codes discard so the retry re-executes.
See [errtrailadapter/](errtrailadapter).

## Performance

Measured on Apple M1 Pro against `memstore`, `httptest` plumbing
included (`httpidem/bench_test.go`; run `go test -bench . ./httpidem/`):

| Path | ns/op | allocs/op |
|---|---|---|
| Handler without middleware | 1,681 | 21 |
| First execution (reserve + capture + persist) | 5,332 | 57 |
| Replay (handler not run) | 3,747 | 47 |
| No key, pass-through | 1,765 | 21 |

The no-replay overhead is ≈ **3.7 µs and +36 allocations** per request
with an in-memory store; with a networked store the round-trips to
Redis dominate. Requests without a key pay ≈ 0.1 µs.

## Relationship to the IETF draft

The `Idempotency-Key` header name and the 400/409/422 status vocabulary
match [draft-ietf-httpapi-idempotency-key-header-07], the shared
industry language. That document is an Internet-Draft that **expired on
2026-04-18 without becoming an RFC**; this library treats it as a
reference, does not claim compliance, and deliberately deviates where
the draft diverges from real-world clients — most notably, raw
(unquoted) keys are the canonical form rather than requiring RFC 8941
quoted strings.

[draft-ietf-httpapi-idempotency-key-header-07]: https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/

## Design and internals

- [DESIGN.md](DESIGN.md) — architecture and the reasoning behind the
  core/HTTP split, the lease model, and the store contract
- [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md) — the requirements
  contract this implementation follows (Japanese; the working name
  `idemtrail` in that document reads as `idemlease`)
- [ROADMAP.md](ROADMAP.md) — milestones and v1.1 plans (Japanese)

## License

[MIT](LICENSE)
