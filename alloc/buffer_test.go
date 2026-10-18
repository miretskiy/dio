package alloc

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestBufferBounds(t *testing.T) {
	data := align.AllocAligned(8192)
	defer align.FreeAligned(data)
	b := newBuffer(data[:4096], func() {})
	defer b.Unpin()
	require.Equal(t, 4096, len(b.Chunks()[0]))
	require.Equal(t, 4096, cap(b.Chunks()[0]))
	require.Panics(t, func() { _ = b.Chunks()[0][:8192] })
}

func TestBufferLastReferenceReleasesExactlyOnce(t *testing.T) {
	data := align.AllocAligned(4096)
	defer align.FreeAligned(data)
	var releases atomic.Int32
	b := newBuffer(data, func() { releases.Add(1) })
	for range 31 {
		require.True(t, b.TryInc())
	}
	var wg sync.WaitGroup
	var last atomic.Int32
	for range 32 {
		wg.Go(func() {
			if b.Unpin() {
				last.Add(1)
				if releases.Load() != 1 {
					t.Error("Unpin returned before releasing storage")
				}
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 1, last.Load())
	require.EqualValues(t, 1, releases.Load())
	require.False(t, b.TryInc())
	require.Panics(t, func() { b.Unpin() })
	require.EqualValues(t, 1, releases.Load())
}

func TestBufferPinRacesLastRelease(t *testing.T) {
	data := align.AllocAligned(4096)
	defer align.FreeAligned(data)
	for range 100 {
		var releases atomic.Int32
		b := newBuffer(data, func() { releases.Add(1) })
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for range 100 {
					if !b.TryInc() {
						break
					}
					if releases.Load() != 0 {
						t.Error("memory released with a reference still held")
					}
					b.Unpin()
				}
			})
		}
		b.Unpin()
		wg.Wait()
		require.EqualValues(t, 1, releases.Load())
	}
}

func TestBufferRefcountOverflow(t *testing.T) {
	b := NewBuffer(4096)
	b.refs.Store(math.MaxInt32)
	require.False(t, b.TryInc())
	b.refs.Store(1)
	b.Unpin()
}

func TestNewBuffer(t *testing.T) {
	b := NewBuffer(4097)
	require.Equal(t, 8192, len(b.Chunks()[0]))
	require.True(t, align.IsAligned(b.Chunks()[0]))
	data := b.Chunks()[0]
	b.Unpin()
	require.Error(t, unix.Munmap(data), "the pool must have unmapped the allocation")
	require.False(t, b.TryInc())
	for _, size := range []int64{-1, 0, math.MaxInt64} {
		require.Panics(t, func() { NewBuffer(size) })
	}
}

// A cache can exclude new lookups while existing readers finish, without a
// retirement state in the buffer. The cache lock covers lookup AND pinning.
func TestCacheEvictionWithExistingReader(t *testing.T) {
	data := align.AllocAligned(4096)
	defer align.FreeAligned(data)
	var releases atomic.Int32
	var mu sync.Mutex
	entry := newBuffer(data, func() { releases.Add(1) })
	lookup := func() *Buffer {
		mu.Lock()
		defer mu.Unlock()
		if entry != nil && entry.TryInc() {
			return entry
		}
		return nil
	}
	reader := lookup()
	require.NotNil(t, reader)
	mu.Lock()
	evicted := entry
	entry = nil
	mu.Unlock()
	require.False(t, evicted.Unpin(), "the reader still pins storage")
	require.Nil(t, lookup())
	require.Zero(t, releases.Load())
	reader.Chunks()[0][0] = 42
	require.True(t, reader.Unpin(), "the final reference releases storage")
	require.EqualValues(t, 1, releases.Load())
}
