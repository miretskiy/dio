package alloc

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// Exercise the ownership and shutdown contract through the shared interface.
func TestAllocatorLifecycle(t *testing.T) {
	for _, test := range []struct {
		name string
		new  func() (Allocator, error)
	}{
		{"extent", func() (Allocator, error) {
			return NewExtentAllocator(4096, 2)
		}},
		{"slab", func() (Allocator, error) {
			return NewSlabAllocator(8192, WithChunkSize(4096))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, err := test.new()
			require.NoError(t, err)
			t.Cleanup(a.Close)
			first, err := a.Acquire(context.Background())
			require.NoError(t, err)
			second, err := a.TryAcquire()
			require.NoError(t, err)
			require.Equal(t, 4096, len(first.Bytes()))
			require.Equal(t, 4096, len(second.Bytes()))
			first.Bytes()[0], second.Bytes()[0] = 1, 2
			require.Equal(t, byte(1), first.Bytes()[0])
			require.True(t, first.TryInc())
			first.Unpin()
			_, err = a.TryAcquire()
			require.ErrorIs(t, err, ErrExhausted)
			first.Unpin()
			next, err := a.TryAcquire()
			require.NoError(t, err)
			require.NotEqual(t, first, next, "a new lease has a new generation")
			require.False(t, first.TryInc())
			require.Panics(t, func() { first.Unpin() })
			require.Equal(t, byte(2), second.Bytes()[0])
			next.Unpin()
			second.Unpin()
			a.Close()
			a.Close()
		})
	}
}

// Every acquisition is exactly one unit, rounded up to whole pages.
func TestAllocatorUnitSize(t *testing.T) {
	const unit = 3 * 4096 // Rounding must also work for non-powers of two.
	for _, kind := range []string{"extent", "slab"} {
		t.Run(kind, func(t *testing.T) {
			var a Allocator
			var err error
			if kind == "extent" {
				a, err = NewExtentAllocator(unit-1, 3)
			} else {
				a, err = NewSlabAllocator(3*unit, WithChunkSize(unit-1))
			}
			require.NoError(t, err)
			defer a.Close()
			held := make([]Memory, 3)
			addresses := make(map[*byte]bool)
			for i := range held {
				held[i], err = a.Acquire(context.Background())
				require.NoError(t, err)
				data := held[i].Bytes()
				require.Len(t, data, unit)
				require.Equal(t, unit, cap(data))
				require.True(t, align.IsAligned(data))
				require.False(t, addresses[&data[0]])
				addresses[&data[0]] = true
			}
			_, err = a.TryAcquire()
			require.ErrorIs(t, err, ErrExhausted)
			for _, m := range held {
				m.Unpin()
			}
		})
	}
}

func TestAllocatorConcurrent(t *testing.T) {
	for _, kind := range []string{"extent", "slab"} {
		for _, chunks := range []int{1, 3, 65, 129} {
			t.Run(fmt.Sprintf("%s/chunks%d", kind, chunks), func(t *testing.T) {
				var p Allocator
				var err error
				if kind == "extent" {
					p, err = NewExtentAllocator(4096, chunks)
				} else {
					p, err = NewSlabAllocator(int64(chunks*4096), WithRetryInterval(time.Millisecond))
				}
				require.NoError(t, err)
				defer p.Close()
				var live sync.Map
				var wg sync.WaitGroup
				for worker := range 32 {
					wg.Go(func() {
						for i := range 200 {
							var m Memory
							var err error
							if i%3 == 0 {
								m, err = p.TryAcquire()
								if err == ErrExhausted {
									continue
								}
							} else {
								m, err = p.Acquire(context.Background())
							}
							if err != nil {
								t.Error(err)
								return
							}
							data := m.Bytes()
							if _, exists := live.LoadOrStore(&data[0], true); exists {
								t.Error("overlapping live memory")
							}
							data[0] = byte(worker)
							runtime.Gosched()
							if data[0] != byte(worker) {
								t.Error("memory overwritten by another borrower")
							}
							live.Delete(&data[0])
							m.Unpin()
						}
					})
				}
				wg.Wait()
			})
		}
	}
}

func TestAllocatorZeroedAcquisitions(t *testing.T) {
	const chunk = 4096
	for _, kind := range []string{"extent", "slab", "supplied-backing"} {
		for _, chunks := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/chunks%d", kind, chunks), func(t *testing.T) {
				var a Allocator
				var err error
				switch kind {
				case "extent":
					a, err = NewExtentAllocator(chunk, chunks)
				case "slab":
					a, err = NewSlabAllocator(int64(chunks * chunk))
				case "supplied-backing":
					backing := NewMemory(int64(chunks * chunk))
					defer backing.Unpin()
					for i := range backing.Bytes() {
						backing.Bytes()[i] = 0xa5
					}
					a, err = NewSlabAllocator(0, WithBacking(backing.Bytes()))
				}
				require.NoError(t, err)
				defer a.Close()
				acquire := func() (Memory, error) { return a.Acquire(context.Background()) }
				for range 3 {
					held := make([]Memory, chunks)
					for i := range held {
						held[i], err = acquire()
						require.NoError(t, err)
						data := held[i].Bytes()
						require.Equal(t, make([]byte, chunk), data)
						for j := range data {
							data[j] = 0xa5
						}
					}
					for _, m := range held {
						m.Unpin()
					}
					acquire = a.TryAcquire
				}
			})
		}
	}
}

func TestMemoryBounds(t *testing.T) {
	data := align.AllocAligned(8192)
	defer align.FreeAligned(data)
	b := newMemory(data[:4096], func() {})
	defer b.Unpin()
	require.Equal(t, 4096, len(b.Bytes()))
	require.Equal(t, 4096, cap(b.Bytes()))
	require.Panics(t, func() { _ = b.Bytes()[:8192] })
}

func TestMemoryLastReferenceReleasesExactlyOnce(t *testing.T) {
	data := align.AllocAligned(4096)
	defer align.FreeAligned(data)
	var releases atomic.Int32
	b := newMemory(data, func() { releases.Add(1) })
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

func TestMemoryPinRacesLastRelease(t *testing.T) {
	data := align.AllocAligned(4096)
	defer align.FreeAligned(data)
	for range 100 {
		var releases atomic.Int32
		b := newMemory(data, func() { releases.Add(1) })
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

// A cache can exclude new lookups while existing readers finish, without a
// retirement state in the memory. The cache lock covers lookup AND pinning.
func TestCacheEvictionWithExistingReader(t *testing.T) {
	data := align.AllocAligned(4096)
	defer align.FreeAligned(data)
	var releases atomic.Int32
	var mu sync.Mutex
	m := newMemory(data, func() { releases.Add(1) })
	entry := &m
	lookup := func() *Memory {
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
	reader.Bytes()[0] = 42
	require.True(t, reader.Unpin(), "the final reference releases storage")
	require.EqualValues(t, 1, releases.Load())
}

// Each allocator has one unit, so reacquiring returns the same bytes.
func TestAllocatorWithoutZeroing(t *testing.T) {
	for _, noZero := range []bool{false, true} {
		var opts []Option
		if noZero {
			opts = append(opts, WithoutZeroing())
		}
		slab, err := NewSlabAllocator(4096, opts...)
		require.NoError(t, err)
		extent, err := NewExtentAllocator(4096, 1, opts...)
		require.NoError(t, err)
		span, err := NewSpanAllocator(spanBytes, opts...)
		require.NoError(t, err)
		for name, acquire := range map[string]func() (Memory, error){
			"slab":   slab.TryAcquire,
			"extent": extent.TryAcquire,
			"span":   func() (Memory, error) { return span.TryAcquire(spanBytes) },
		} {
			m, err := acquire()
			require.NoError(t, err)
			m.Bytes()[0] = 0xa5
			m.Unpin()
			m, err = acquire()
			require.NoError(t, err)
			want := byte(0)
			if noZero {
				want = 0xa5
			}
			require.Equal(t, want, m.Bytes()[0], "%s noZero=%v", name, noZero)
			m.Unpin()
		}
		slab.Close()
		extent.Close()
		span.Close()
	}
}

// A handle kept past its lease cannot touch the next lease of the same memory.
func TestMemoryStaleHandle(t *testing.T) {
	a, err := NewSlabAllocator(4096)
	require.NoError(t, err)
	defer a.Close()
	first, err := a.TryAcquire()
	require.NoError(t, err)
	copied := first
	require.True(t, copied.TryInc(), "copies name the same lease")
	require.False(t, first.Unpin())
	require.True(t, copied.Unpin())
	next, err := a.TryAcquire()
	require.NoError(t, err)
	require.Equal(t, &first.Bytes()[0], &next.Bytes()[0], "the same chunk is lent again")
	require.False(t, first.TryInc())
	require.Panics(t, func() { first.Unpin() })
	require.True(t, next.TryInc(), "the new lease is unaffected")
	require.False(t, next.Unpin())
	require.True(t, next.Unpin())
	require.False(t, Memory{}.TryInc())
}

// The hook runs once per lease, on the final Unpin, before the memory is
// returned: each allocator here has a single unit, so it is still taken.
func TestAllocatorReleaseHook(t *testing.T) {
	var released [][]byte
	var reacquire func() error
	hook := WithReleaseHook(func(data []byte) {
		released = append(released, data)
		require.ErrorIs(t, reacquire(), ErrExhausted, "the hook runs before the memory is returned")
	})
	slab, err := NewSlabAllocator(4096, hook)
	require.NoError(t, err)
	defer slab.Close()
	extent, err := NewExtentAllocator(4096, 1, hook)
	require.NoError(t, err)
	defer extent.Close()
	span, err := NewSpanAllocator(spanBytes, hook)
	require.NoError(t, err)
	defer span.Close()
	for name, acquire := range map[string]func() (Memory, error){
		"slab":   slab.TryAcquire,
		"extent": extent.TryAcquire,
		"span":   func() (Memory, error) { return span.TryAcquire(spanBytes) },
	} {
		reacquire = func() error { _, err := acquire(); return err }
		released = nil
		m, err := acquire()
		require.NoError(t, err)
		require.True(t, m.TryInc())
		require.False(t, m.Unpin())
		require.Empty(t, released, "%s: only the final Unpin runs the hook", name)
		require.True(t, m.Unpin())
		require.Len(t, released, 1, name)
		require.Equal(t, &m.Bytes()[0], &released[0][0], name)
		m, err = acquire()
		require.NoError(t, err, "%s: the memory is returned after the hook", name)
		m.Unpin()
	}
}

func TestAllocatorReleaseHookConcurrentUnpin(t *testing.T) {
	var hooks atomic.Int32
	a, err := NewSlabAllocator(4096, WithReleaseHook(func([]byte) { hooks.Add(1) }))
	require.NoError(t, err)
	defer a.Close()
	m, err := a.TryAcquire()
	require.NoError(t, err)
	for range 31 {
		require.True(t, m.TryInc())
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if m.Unpin() && hooks.Load() != 1 {
				t.Error("final Unpin returned before the hook completed")
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 1, hooks.Load())
}
