package idemlease_test

import (
	"testing"

	"github.com/repenguin22/idemlease"
	"github.com/repenguin22/idemlease/idemleasetest"
)

// TestFakeStoreConformance keeps the in-package test double honest: the
// fake must satisfy the same Store semantics as real implementations,
// or the core tests written against it prove nothing.
func TestFakeStoreConformance(t *testing.T) {
	idemleasetest.RunStoreTests(t, func(t *testing.T) idemlease.Store {
		return newFakeStore()
	})
}
