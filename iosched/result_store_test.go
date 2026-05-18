package iosched

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResultStoreInitClearsReusedSlots(t *testing.T) {
	staleErr := errors.New("stale")
	var store resultStore
	store.init(5)
	for i := 0; i < 5; i++ {
		res := store.at(i)
		res.N = 128 << 10
		res.Err = staleErr
	}

	store.init(5)

	for i := 0; i < 5; i++ {
		require.Equal(t, Result{}, *store.at(i), "slot %d", i)
	}
}
