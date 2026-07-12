package memstore_test

import (
	"testing"

	"github.com/repenguin22/idemlease"
	"github.com/repenguin22/idemlease/idemleasetest"
	"github.com/repenguin22/idemlease/memstore"
)

func TestConformance(t *testing.T) {
	idemleasetest.RunStoreTests(t, func(t *testing.T) idemlease.Store {
		return memstore.New()
	})
}

func TestStateMachine(t *testing.T) {
	idemleasetest.RunStateMachineTests(t, func(t *testing.T) idemlease.Store {
		return memstore.New()
	})
}
