package alloc

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/internal/buildutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestMemoryLastReferenceReleasesExactlyOnce(t *testing.T) {
	a := newAllocator(t, spanBytes)
	b, err := a.TryAcquire(4096)
	require.NoError(t, err)
	for range 31 {
		require.True(t, b.TryInc())
	}
	var wg sync.WaitGroup
	var last atomic.Int32
	for range 32 {
		wg.Go(func() {
			if b.Unpin() {
				last.Add(1)
				if a.Available() != a.Capacity() {
					t.Error("Unpin returned before releasing storage")
				}
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 1, last.Load())
	require.False(t, b.TryInc())
	require.Panics(t, func() { b.Unpin() })
	require.Equal(t, a.Capacity(), a.Available())
}

func TestMemoryPinRacesLastRelease(t *testing.T) {
	a := newAllocator(t, spanBytes)
	for range 100 {
		b, err := a.TryAcquire(4096)
		require.NoError(t, err)
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for range 100 {
					if !b.TryInc() {
						break
					}
					if a.Available() == a.Capacity() {
						t.Error("memory released with a reference still held")
					}
					b.Unpin()
				}
			})
		}
		b.Unpin()
		wg.Wait()
		require.Equal(t, a.Capacity(), a.Available())
	}
}

func TestMemoryRefcountOverflow(t *testing.T) {
	b := NewMemory(4096)
	b.word().Store(uint64(b.gen)<<32 | math.MaxUint32)
	require.False(t, b.TryInc())
	b.word().Store(uint64(b.gen)<<32 | 1)
	b.Unpin()
}

func TestNewMemory(t *testing.T) {
	b := NewMemory(4097)
	require.Equal(t, 8192, len(b.Bytes()))
	require.True(t, align.IsAligned(b.Bytes()))
	data := b.Bytes()
	b.Unpin()
	require.Error(t, unix.Munmap(data), "the pool must have unmapped the allocation")
	require.False(t, b.TryInc())
	for _, size := range []int64{-1, 0, math.MaxInt64} {
		require.Panics(t, func() { NewMemory(size) })
	}
}

// A handle kept past its lease cannot touch the next lease of the same memory.
func TestMemoryStaleHandle(t *testing.T) {
	a := newAllocator(t, spanBytes)
	first, err := a.TryAcquire(spanBytes)
	require.NoError(t, err)
	copied := first
	require.True(t, copied.TryInc(), "copies name the same lease")
	require.False(t, first.Unpin())
	require.True(t, copied.Unpin())
	next, err := a.TryAcquire(spanBytes)
	require.NoError(t, err)
	require.Equal(t, &first.Bytes()[0], &next.Bytes()[0], "the same span is lent again")
	require.False(t, first.TryInc())
	require.Panics(t, func() { first.Unpin() })
	require.True(t, next.TryInc(), "the new lease is unaffected")
	require.False(t, next.Unpin())
	require.True(t, next.Unpin())
	require.False(t, Memory{}.TryInc())
}

func TestMemoryExtendConsumesRawRange(t *testing.T) {
	a := newAllocator(t, 2*spanBytes)
	a.arena.cursors = []arenaCursor{{end: len(a.arena.words)}}
	left, err := a.TryAcquire(spanBytes / 2)
	require.NoError(t, err)
	right, err := a.acquireRaw(spanBytes / 2)
	require.NoError(t, err)
	next, err := a.acquireRaw(spanBytes)
	require.NoError(t, err)
	left.Bytes()[0], right.bytes(a.arena)[0], next.bytes(a.arena)[0] = 1, 2, 3
	oldLeft := left
	address := &left.Bytes()[0]
	require.True(t, left.extend(right))
	require.Equal(t, address, &left.Bytes()[0])
	require.Len(t, left.Bytes(), spanBytes)
	require.Equal(t, byte(1), left.Bytes()[0])
	require.Equal(t, byte(2), left.Bytes()[spanBytes/2])
	require.False(t, oldLeft.TryInc())
	require.Panics(t, func() { oldLeft.Unpin() })
	require.Zero(t, a.arena.leases.words[right.start].Load(), "the raw extension never needed a lease")
	oldLeft = left
	require.True(t, left.extend(next))
	require.False(t, oldLeft.TryInc())
	require.Len(t, left.Bytes(), 2*spanBytes)
	require.Equal(t, len(left.Bytes()), cap(left.Bytes()))
	require.Equal(t, byte(3), left.Bytes()[spanBytes])
	require.Zero(t, a.arena.leases.words[next.start].Load())
	pinned := left
	require.True(t, pinned.TryInc())
	require.False(t, left.Unpin())
	require.Zero(t, a.Available(), "one pin retains the complete extent")
	require.True(t, pinned.Unpin())
	a.requireEmpty(t)
	require.False(t, pinned.TryInc())
}

func TestMemoryExtendRejectsNonadjacentRange(t *testing.T) {
	a := newAllocator(t, spanBytes)
	left, err := a.TryAcquire(chunk)
	require.NoError(t, err)
	defer left.Unpin()
	middle, err := a.acquireRaw(chunk)
	require.NoError(t, err)
	defer a.arena.freePages(middle.start, middle.n)
	right, err := a.acquireRaw(chunk)
	require.NoError(t, err)
	defer a.arena.freePages(right.start, right.n)
	before := left
	require.False(t, left.extend(right))
	require.Equal(t, before, left)
}

func TestMemoryExtendPanicsOnSharedLease(t *testing.T) {
	t.Cleanup(buildutil.EnableTestMode(false))
	a := newAllocator(t, spanBytes)
	left, err := a.TryAcquire(chunk)
	require.NoError(t, err)
	defer left.Unpin()
	right, err := a.acquireRaw(chunk)
	require.NoError(t, err)
	defer a.arena.freePages(right.start, right.n)
	require.True(t, left.TryInc())
	before, refs := left, left.word().Load()
	require.PanicsWithValue(t, "alloc: extend requires exclusive ownership", func() { left.extend(right) })
	require.Equal(t, before, left)
	require.Equal(t, refs, left.word().Load())
	require.False(t, left.Unpin())
}

func TestMemoryExtendPanicsOnStaleLease(t *testing.T) {
	a := newAllocator(t, spanBytes)
	left, err := a.TryAcquire(chunk)
	require.NoError(t, err)
	right, err := a.acquireRaw(chunk)
	require.NoError(t, err)
	defer a.arena.freePages(right.start, right.n)
	left.Unpin()
	replacement, err := a.TryAcquire(chunk)
	require.NoError(t, err)
	require.Equal(t, left.unit, replacement.unit)
	before := left
	require.Panics(t, func() { left.extend(right) })
	require.Equal(t, before, left)
	require.True(t, replacement.Unpin(), "the replacement lease is untouched")
}
