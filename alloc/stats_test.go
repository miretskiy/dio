//go:build allocstats

package alloc

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllocatorStats(t *testing.T) {
	a := newAllocator(t, spanBytes)
	require.Equal(t, Stats{}, a.Stats())
	first, err := a.TryAcquire(1)
	require.NoError(t, err)
	require.Equal(t, Stats{RequestedBytes: 1, AllocatedBytes: 4096}, a.Stats())
	second, err := a.TryAcquire(spanBytes)
	require.NoError(t, err)
	want := Stats{RequestedBytes: (1 + spanBytes), AllocatedBytes: spanBytes}
	require.Equal(t, want, a.Stats(), "count actual partial returns")
	_, err = a.TryAcquire(4096)
	require.ErrorIs(t, err, ErrExhausted)
	require.Panics(t, func() { a.TryAcquire(0) })
	require.Equal(t, want, a.Stats(), "failed attempts must not inflate totals")
	first.Unpin()
	second.Unpin()
	a.Close()
	require.Equal(t, want, a.Stats(), "release and Close do not reset cumulative totals")
}

func TestAllocatorStatsBeforeRoundingAndCapping(t *testing.T) {
	a := newAllocator(t, spanBytes)
	m, err := a.TryAcquire(12 << 10)
	require.NoError(t, err)
	require.Equal(t, Stats{RequestedBytes: (12 << 10), AllocatedBytes: (16 << 10)}, a.Stats())
	m.Unpin()
	m, err = a.TryAcquire(2 * spanBytes)
	require.NoError(t, err)
	require.Equal(t, Stats{
		RequestedBytes: (12<<10 + 2*spanBytes),
		AllocatedBytes: (16<<10 + spanBytes),
	}, a.Stats())
	m.Unpin()
}

func TestAllocatorStatsConcurrent(t *testing.T) {
	const workers, count = 16, 200
	a := newAllocator(t, 64*spanBytes, WithoutZeroing())
	totals := make(chan Stats, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			var total Stats
			for i := range count {
				requested := []int64{1, 12 << 10, 2 * MaxAllocationSize}[i%3]
				m, err := a.TryAcquire(requested)
				if err != nil {
					t.Error(err)
					return
				}
				total.RequestedBytes += uint64(requested)
				total.AllocatedBytes += uint64(len(m.Bytes()))
				_ = a.Stats() // Exercise reads alongside concurrent updates.
				m.Unpin()
			}
			totals <- total
		})
	}
	wg.Wait()
	close(totals)
	var want Stats
	for s := range totals {
		want.RequestedBytes += s.RequestedBytes
		want.AllocatedBytes += s.AllocatedBytes
	}
	require.Equal(t, want, a.Stats())
}

func TestBufferStatsCountAllocatorRequests(t *testing.T) {
	a := newAllocator(t, 3*spanBytes)
	b := NewAllocatingBuffer(a)
	defer b.Release()
	require.NoError(t, b.Grow(spanBytes+1))
	require.Equal(t, Stats{RequestedBytes: spanBytes + 1, AllocatedBytes: spanBytes + 4096}, a.Stats())
	before := a.Stats()
	require.ErrorIs(t, b.Grow(4*spanBytes), ErrExhausted)
	// Two successful 128 KiB requests return 124 KiB and 128 KiB. The final
	// exhausted request contributes nothing, just as a direct TryAcquire.
	want := Stats{RequestedBytes: before.RequestedBytes + 2*spanBytes, AllocatedBytes: 3 * spanBytes}
	require.Equal(t, want, a.Stats())
	require.ErrorIs(t, b.Grow(4*spanBytes), ErrExhausted)
	require.Equal(t, want, a.Stats(), "an attempt that obtains no memory contributes nothing")
}
