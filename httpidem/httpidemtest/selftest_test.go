package httpidemtest_test

import (
	"testing"

	"github.com/repenguin22/idemlease"
	"github.com/repenguin22/idemlease/httpidem/httpidemtest"
	"github.com/repenguin22/idemlease/memstore"
)

// TestSuiteAgainstMemstore validates the suite itself and serves as the
// reference run every store implementation should match.
func TestSuiteAgainstMemstore(t *testing.T) {
	httpidemtest.RunHTTPTests(t, func(t *testing.T) idemlease.Store {
		return memstore.New()
	})
}
