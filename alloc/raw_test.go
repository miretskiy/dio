package alloc

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufferGrowthNoopPreservesLease(t *testing.T) {
	a := newAllocator(t, spanBytes)
	b := NewAllocatingBuffer(a)
	defer b.Release()
	_, err := b.Write([]byte("prefix"))
	require.NoError(t, err)
	m := b.RawAllocations()[0]
	capacity := b.Cap()
	for _, n := range []int{0, -1, math.MinInt, capacity - b.Len()} {
		require.NoError(t, b.Grow(n))
		require.Equal(t, capacity, b.Cap())
		require.Equal(t, m, b.RawAllocations()[0])
	}
}

func TestRawClaimsDoNotPublishLeases(t *testing.T) {
	a := newAllocator(t, 2*spanBytes)
	a.arena.initCursors(1)
	stale, err := a.TryAcquire(spanBytes)
	require.NoError(t, err)
	stale.Unpin()
	a.arena.cursors[0].position.Store(0)
	generation := a.arena.leases.words[0].Load()
	first, err := a.acquireRaw(spanBytes)
	require.NoError(t, err)
	second, err := a.acquireRaw(spanBytes)
	require.NoError(t, err)
	require.Equal(t, generation, a.arena.leases.words[0].Load())
	require.Zero(t, a.arena.leases.words[spanPages].Load())
	require.False(t, stale.TryInc())
	require.True(t, first.join(second))
	m := first.lease(a.arena)
	require.False(t, stale.TryInc(), "publishing a raw run must not revive an earlier handle")
	require.Len(t, m.Bytes(), 2*spanBytes)
	require.Equal(t, len(m.Bytes()), cap(m.Bytes()))
	require.Zero(t, a.arena.leases.words[spanPages].Load(), "only the joined run's first page needs a lease")
	m.Unpin()
	a.requireEmpty(t)
}

func TestBufferPublishesOneLeasePerRawRun(t *testing.T) {
	a := newAllocator(t, 4<<20)
	a.arena.initCursors(1)
	b := NewAllocatingBuffer(a)
	require.NoError(t, b.Grow(4<<20))
	require.Len(t, b.mem, 1)
	for page := 1; page < len(a.arena.leases.words); page++ {
		require.Zero(t, a.arena.leases.words[page].Load(), "page %d has no intermediate handle to retire", page)
	}
	m := b.RawAllocations()[0]
	require.True(t, m.TryInc())
	b.Release()
	require.Zero(t, a.Available())
	m.Unpin()
	a.requireEmpty(t)
}

func TestRawJoinPrecedesLeaseAndPreservesData(t *testing.T) {
	a := newAllocator(t, spanBytes)
	first, err := a.acquireRaw(chunk)
	require.NoError(t, err)
	second, err := a.acquireRaw(chunk)
	require.NoError(t, err)
	first.bytes(a.arena)[0], second.bytes(a.arena)[0] = 1, 2
	// No stream order has been assigned: the resulting bytes use address order.
	require.True(t, second.join(first))
	m := second.lease(a.arena)
	require.Equal(t, byte(1), m.Bytes()[0])
	require.Equal(t, byte(2), m.Bytes()[chunk])
	m.Unpin()
	a.requireEmpty(t)
}
