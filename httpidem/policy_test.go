package httpidem_test

import (
	"context"
	"errors"
	"testing"

	"github.com/repenguin22/idemlease"
	"github.com/repenguin22/idemlease/httpidem"
)

// TestDefaultPolicy is the §5.1 decision table.
func TestDefaultPolicy(t *testing.T) {
	tests := []struct {
		status int
		want   idemlease.Decision
	}{
		{200, idemlease.Persist},
		{201, idemlease.Persist},
		{204, idemlease.Persist},
		{301, idemlease.Persist},
		{302, idemlease.Persist},
		{400, idemlease.Persist},
		{404, idemlease.Persist},
		{409, idemlease.Persist},
		{422, idemlease.Persist},
		{429, idemlease.Discard},
		{500, idemlease.Discard},
		{502, idemlease.Discard},
		{503, idemlease.Discard},
	}
	for _, tt := range tests {
		if got := httpidem.DefaultPolicy.Decide(tt.status, nil); got != tt.want {
			t.Errorf("Decide(%d) = %v, want %v", tt.status, got, tt.want)
		}
	}
	// The default policy is status-driven: SetError content is ignored.
	if got := httpidem.DefaultPolicy.Decide(200, errors.New("x")); got != idemlease.Persist {
		t.Errorf("Decide(200, err) = %v, want Persist", got)
	}
}

// TestSetErrorWithoutMiddleware pins that SetError is a safe no-op on a
// context that did not pass through the middleware.
func TestSetErrorWithoutMiddleware(t *testing.T) {
	httpidem.SetError(context.Background(), errors.New("ignored"))
}
