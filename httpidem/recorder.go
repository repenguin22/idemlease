package httpidem

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"net/http"
	"net/textproto"
)

// alwaysExcludedHeaders are never replayed, even when listed in
// StoreHeaders: session-bearing headers would leak one client's
// credentials to another, and hop-by-hop headers describe the original
// connection, not the replayed one (§4.4).
var alwaysExcludedHeaders = map[string]struct{}{
	"Set-Cookie":          {},
	"Authorization":       {},
	"Connection":          {},
	"Transfer-Encoding":   {},
	"Keep-Alive":          {},
	"Upgrade":             {},
	"Trailer":             {},
	"Te":                  {},
	"Proxy-Authenticate":  {},
	"Proxy-Authorization": {},
}

// Recorder accumulates the replayable parts of a response — status,
// allowlisted headers, and body — independently of any ResponseWriter,
// so framework adapters can drive it from their own writer types
// (§6.2). The httpidem middleware's writer wrapper is a thin skin over
// a Recorder. Methods are not safe for concurrent use.
type Recorder struct {
	maxBody int
	allow   map[string]struct{}

	status     int
	header     http.Header
	body       bytes.Buffer
	overflowed bool
	abandoned  bool
}

// NewRecorder returns a Recorder that captures at most maxBody bytes of
// body and only the headers named in allowHeaders (names are
// canonicalized; always-excluded headers are dropped even if listed).
func NewRecorder(maxBody int, allowHeaders []string) *Recorder {
	allow := make(map[string]struct{}, len(allowHeaders))
	for _, name := range allowHeaders {
		c := textproto.CanonicalMIMEHeaderKey(name)
		if _, excluded := alwaysExcludedHeaders[c]; excluded {
			continue
		}
		allow[c] = struct{}{}
	}
	return &Recorder{maxBody: maxBody, allow: allow}
}

// RecordStatus records the response status and snapshots the
// allowlisted headers from header. Only the first call takes effect,
// matching net/http semantics; informational (1xx) statuses are
// ignored so the final status is the one captured.
func (rec *Recorder) RecordStatus(status int, header http.Header) {
	if rec.status != 0 || (status >= 100 && status < 200) {
		return
	}
	rec.status = status
	rec.header = make(http.Header, len(rec.allow))
	for name := range rec.allow {
		if values := header.Values(name); len(values) > 0 {
			rec.header[name] = append([]string(nil), values...)
		}
	}
}

// RecordBody appends p to the captured body. Once the total exceeds
// maxBody the capture is marked overflowed and the buffer is dropped;
// the response itself is unaffected (the caller keeps writing it to the
// client).
func (rec *Recorder) RecordBody(p []byte) {
	if rec.overflowed || rec.abandoned {
		return
	}
	if rec.body.Len()+len(p) > rec.maxBody {
		rec.overflowed = true
		rec.body.Reset()
		return
	}
	rec.body.Write(p)
}

// Abandon marks the capture unusable. Call it when the response starts
// streaming (Flush or Hijack): streamed responses are not stored
// (§6.1) and the caller must treat the outcome as Discard.
func (rec *Recorder) Abandon() {
	rec.abandoned = true
	rec.body.Reset()
}

// Status returns the recorded status, or 200 when the handler never
// wrote one, matching net/http's implicit 200 (§5.1).
func (rec *Recorder) Status() int {
	if rec.status == 0 {
		return http.StatusOK
	}
	return rec.status
}

// Overflowed reports whether the body exceeded maxBody.
func (rec *Recorder) Overflowed() bool { return rec.overflowed }

// Abandoned reports whether the capture was abandoned via Abandon.
func (rec *Recorder) Abandoned() bool { return rec.abandoned }

// StoredResponse returns the captured response. ok is false when the
// capture overflowed or was abandoned, in which case the outcome must
// be discarded rather than persisted.
func (rec *Recorder) StoredResponse() (sr StoredResponse, ok bool) {
	if rec.overflowed || rec.abandoned {
		return StoredResponse{}, false
	}
	header := rec.header
	if header == nil {
		header = http.Header{}
	}
	return StoredResponse{
		StatusCode: rec.Status(),
		Header:     header,
		Body:       append([]byte(nil), rec.body.Bytes()...),
	}, true
}

// captureWriter is the middleware's thin http.ResponseWriter skin over
// a Recorder: it forwards everything to the underlying writer while
// feeding the Recorder. Flusher/Hijacker/Pusher are passed through via
// http.ResponseController semantics (callers get ErrNotSupported when
// the underlying writer lacks the capability), and using Flush or
// Hijack abandons the capture.
type captureWriter struct {
	http.ResponseWriter
	rec         *Recorder
	wroteHeader bool
}

func (cw *captureWriter) WriteHeader(code int) {
	if !cw.wroteHeader {
		cw.rec.RecordStatus(code, cw.Header())
		if code >= 200 {
			cw.wroteHeader = true
		}
	}
	cw.ResponseWriter.WriteHeader(code)
}

func (cw *captureWriter) Write(p []byte) (int, error) {
	if !cw.wroteHeader {
		cw.WriteHeader(http.StatusOK)
	}
	cw.rec.RecordBody(p)
	return cw.ResponseWriter.Write(p)
}

// finalize captures the implicit 200 (and its headers) for handlers
// that return without writing anything.
func (cw *captureWriter) finalize() {
	if !cw.wroteHeader {
		cw.rec.RecordStatus(http.StatusOK, cw.Header())
		cw.wroteHeader = true
	}
}

// Unwrap supports http.NewResponseController chains.
func (cw *captureWriter) Unwrap() http.ResponseWriter { return cw.ResponseWriter }

func (cw *captureWriter) Flush() { _ = cw.flush() }

func (cw *captureWriter) FlushError() error { return cw.flush() }

func (cw *captureWriter) flush() error {
	err := http.NewResponseController(cw.ResponseWriter).Flush()
	if err == nil || !errors.Is(err, http.ErrNotSupported) {
		cw.rec.Abandon() // streaming began: the capture is void (§6.1)
	}
	return err
}

func (cw *captureWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(cw.ResponseWriter).Hijack()
	if err == nil {
		cw.rec.Abandon()
	}
	return conn, rw, err
}

func (cw *captureWriter) Push(target string, opts *http.PushOptions) error {
	if p, ok := cw.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}
