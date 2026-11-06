//go:build !allocstats

package alloc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllocatorStatsDisabled(t *testing.T) {
	a := newAllocator(t, spanBytes)
	for _, size := range []int64{1, 12 << 10, 2 * spanBytes} {
		m, err := a.TryAcquire(size)
		require.NoError(t, err)
		require.Equal(t, Stats{}, a.Stats())
		m.Unpin()
	}
	a.Close()
	require.Equal(t, Stats{}, a.Stats())
}
