//go:build linux

package iosched

import (
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/mempool"
	"github.com/stretchr/testify/require"
)

func TestURingDMARegistrationOwnership(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	_, err := NewURingScheduler(WithRingDepth(8), WithDMASlab(nil))
	require.ErrorContains(t, err, "nil DMA slab")
	pool, err := mempool.NewSlabPool(align.HugepageSize, align.BlockSize)
	require.NoError(t, err)
	s, err := NewURingScheduler(WithRingDepth(8), WithDMASlab(pool))
	require.NoError(t, err)

	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, s.Close())
		}
		pool.Close()
	})

	require.Same(t, pool, s.registeredPool)

	err = s.Close()
	closed = true
	require.NoError(t, err)
	require.Nil(t, s.registeredPool)
}
