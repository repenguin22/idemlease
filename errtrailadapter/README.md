# errtrailadapter

[errtrail](https://github.com/repenguin22/errtrail) integration for the
idemlease `httpidem` middleware (module
`github.com/repenguin22/idemlease/errtrailadapter`).

It supplies two plugins, built only from httpidem's public interfaces:

- **`Errors()`** — an `httpidem.ErrorWriter` that maps the middleware's
  rejection sentinels (`ErrKeyMissing`, `ErrInFlight`,
  `ErrFingerprintMismatch`, …) to errtrail codes and responds with
  `problem.Write` (RFC 9457), exposing only errtrail public messages.
- **`Policy()`** — an `httpidem.ReplayPolicy` that decides persist vs.
  discard from the errtrail `Code` a handler reports via
  `httpidem.SetError` (retryable codes → discard so a retry
  re-executes), falling back to the status-driven default otherwise.

```go
import (
	"github.com/repenguin22/idemlease/httpidem"
	"github.com/repenguin22/idemlease/errtrailadapter"
)

mw := httpidem.New(store, errtrailadapter.Options()...) // Errors + Policy
```

The same `Errors()` / `Policy()` values also work with the Gin adapter
(`ginadapter.Errors(...)`, `ginadapter.Policy(...)`).

In a handler, classify failures with errtrail and let the policy react:

```go
func createOrder(w http.ResponseWriter, r *http.Request) {
	if depDown {
		httpidem.SetError(r.Context(), errtrail.New(errtrail.Unavailable, "billing down"))
		// errtrail.Unavailable is retryable → the result is discarded,
		// so the client's retry re-executes instead of replaying a 503.
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	// ...
}
```

## Reserved codes

Registers errtrail codes **1100–1104** with names `IDEMPOTENCY_*` from
an `init` function. Do not register those codes or names elsewhere in
your program (errtrail's registry rejects duplicates).
