package httpidem

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"sort"
)

// StoredResponse is the replayable subset of an HTTP response: status,
// allowlisted headers, and body. httpidem owns its serialization to and
// from the opaque core payload (§4.4); the format carries a version
// byte so it can evolve.
type StoredResponse struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// storedResponseVersion is the current serialization format:
//
//	0x01 | status | nNames | (name nValues value*)* | body
//
// where integers are unsigned varints and strings/bytes are
// length-prefixed with an unsigned varint.
const storedResponseVersion = 0x01

var errCorruptStoredResponse = errors.New("httpidem: corrupted stored response payload")

// Decoder sanity limits: a payload written by the capture path holds a
// handful of allowlisted headers, so anything past these counts is
// foreign data and is refused before it can drive allocations.
const (
	maxHeaderNames  = 1024
	maxHeaderValues = 1024
)

func appendString(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

// MarshalBinary encodes the response with a leading version byte.
// Header names are sorted so the encoding is deterministic.
func (sr *StoredResponse) MarshalBinary() ([]byte, error) {
	buf := []byte{storedResponseVersion}
	buf = binary.AppendUvarint(buf, uint64(sr.StatusCode))

	names := make([]string, 0, len(sr.Header))
	for name := range sr.Header {
		names = append(names, name)
	}
	sort.Strings(names)
	buf = binary.AppendUvarint(buf, uint64(len(names)))
	for _, name := range names {
		buf = appendString(buf, name)
		values := sr.Header[name]
		buf = binary.AppendUvarint(buf, uint64(len(values)))
		for _, v := range values {
			buf = appendString(buf, v)
		}
	}
	buf = binary.AppendUvarint(buf, uint64(len(sr.Body)))
	buf = append(buf, sr.Body...)
	return buf, nil
}

// UnmarshalBinary decodes a payload produced by MarshalBinary. Payloads
// with an unknown version byte or corrupted structure are rejected.
func (sr *StoredResponse) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		return errCorruptStoredResponse
	}
	if data[0] != storedResponseVersion {
		return fmt.Errorf("httpidem: unsupported stored response version 0x%02X", data[0])
	}
	d := decoder{buf: data[1:]}
	status := d.uvarint()
	nNames := d.uvarint()
	if nNames > maxHeaderNames {
		return errCorruptStoredResponse
	}
	header := make(http.Header)
	for i := uint64(0); i < nNames && d.err == nil; i++ {
		name := d.string()
		nValues := d.uvarint()
		if nValues > maxHeaderValues {
			return errCorruptStoredResponse
		}
		for j := uint64(0); j < nValues && d.err == nil; j++ {
			header[name] = append(header[name], d.string())
		}
	}
	body := d.bytes()
	if d.err != nil {
		return d.err
	}
	if len(d.buf) != 0 {
		return errCorruptStoredResponse
	}
	sr.StatusCode = int(status)
	sr.Header = header
	sr.Body = body
	return nil
}

// decoder consumes the buffer front to back, latching the first error.
type decoder struct {
	buf []byte
	err error
}

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.buf)
	if n <= 0 {
		d.err = errCorruptStoredResponse
		return 0
	}
	d.buf = d.buf[n:]
	return v
}

func (d *decoder) bytes() []byte {
	n := d.uvarint()
	if d.err != nil {
		return nil
	}
	if n > uint64(len(d.buf)) {
		d.err = errCorruptStoredResponse
		return nil
	}
	b := append([]byte(nil), d.buf[:n]...)
	d.buf = d.buf[n:]
	return b
}

func (d *decoder) string() string { return string(d.bytes()) }
