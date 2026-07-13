package httpidem_test

import (
	"encoding/binary"
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/repenguin22/idemlease/httpidem"
)

func TestStoredResponseRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		sr   httpidem.StoredResponse
	}{
		{
			name: "typical",
			sr: httpidem.StoredResponse{
				StatusCode: 201,
				Header: http.Header{
					"Content-Type": {"application/json"},
					"Location":     {"/orders/42"},
					"X-Multi":      {"a", "b"},
				},
				Body: []byte(`{"id":42}`),
			},
		},
		{
			name: "empty header and body",
			sr:   httpidem.StoredResponse{StatusCode: 204, Header: http.Header{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.sr.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary: %v", err)
			}
			if data[0] != 0x01 {
				t.Fatalf("version byte = 0x%02X, want 0x01", data[0])
			}
			var got httpidem.StoredResponse
			if err := got.UnmarshalBinary(data); err != nil {
				t.Fatalf("UnmarshalBinary: %v", err)
			}
			if diff := cmp.Diff(tt.sr, got, cmpopts.EquateEmpty()); diff != "" {
				t.Fatalf("round trip mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestStoredResponseUnmarshalRejectsBadPayloads(t *testing.T) {
	valid, err := (&httpidem.StoredResponse{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": {"text/plain"}},
		Body:       []byte("hello"),
	}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"unknown version", []byte{0x02, 0xC8, 0x01}},
		{"truncated", valid[:len(valid)/2]},
		{"trailing data", append(append([]byte(nil), valid...), 0x00)},
		{"garbage", []byte{0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sr httpidem.StoredResponse
			if err := sr.UnmarshalBinary(tt.data); err == nil {
				t.Fatalf("UnmarshalBinary accepted %v, want error", tt.data)
			}
		})
	}
}

// TestStoredResponseRejectsExcessiveHeaderCounts pins the decoder caps
// from review finding M3: a syntactically valid payload claiming
// thousands of headers is foreign data and must be refused.
func TestStoredResponseRejectsExcessiveHeaderCounts(t *testing.T) {
	buf := []byte{0x01}
	buf = binary.AppendUvarint(buf, 200)  // status
	buf = binary.AppendUvarint(buf, 2048) // header names: over the cap
	for i := 0; i < 2048; i++ {
		buf = binary.AppendUvarint(buf, 0) // empty name
		buf = binary.AppendUvarint(buf, 0) // zero values
	}
	buf = binary.AppendUvarint(buf, 0) // empty body

	var sr httpidem.StoredResponse
	if err := sr.UnmarshalBinary(buf); err == nil {
		t.Fatal("UnmarshalBinary accepted a payload with 2048 header names, want error")
	}
}
