//go:build linux

package iosched

import (
	"testing"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/mempool"
	"github.com/stretchr/testify/require"
)

func TestURingDMARegistrationOwnership(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	s, err := NewURingScheduler(WithRingDepth(8))
	require.NoError(t, err)

	var pool *mempool.SlabPool
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, s.Close())
		}
		if pool != nil {
			pool.Close()
		}
	})

	require.ErrorContains(t, s.usePool(nil), "nil DMA slab")
	pool, err = mempool.NewSlabPool(align.HugepageSize, align.BlockSize)
	require.NoError(t, err)
	require.NoError(t, s.usePool(pool))
	require.Same(t, pool, s.registeredPool)
	require.ErrorContains(t, s.usePool(pool), "already registered")

	err = s.Close()
	closed = true
	require.NoError(t, err)
	require.Nil(t, s.registeredPool)
}
