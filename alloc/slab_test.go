package alloc

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
)

func TestSlabAllocatorLayoutAndReuse(t *testing.T) {
	for _, chunks := range []int{1, 3, 63, 64, 65, 129} {
		t.Run(fmt.Sprintf("chunks%d", chunks), func(t *testing.T) {
			p, err := NewSlabAllocator(int64(chunks*8192+4096), WithChunkSize(4097))
			require.NoError(t, err)
			defer p.Close()
			held := make([]Memory, chunks)
			addresses := make(map[*byte]bool)
			for i := range held {
				held[i], err = p.TryAcquire()
				require.NoError(t, err)
				b := held[i]
				require.Equal(t, 8192, len(b.Bytes()))
				require.Equal(t, 8192, cap(b.Bytes()))
				require.True(t, align.IsAligned(b.Bytes()))
				require.False(t, addresses[&b.Bytes()[0]])
				addresses[&b.Bytes()[0]] = true
				b.Bytes()[0] = byte(i)
			}
			_, err = p.TryAcquire()
			require.ErrorIs(t, err, ErrExhausted)
			for i := len(held) - 1; i >= 0; i-- {
				b := held[i]
				require.Equal(t, byte(i), b.Bytes()[0])
				address := &b.Bytes()[0]
				b.Unpin()
				held[i], err = p.TryAcquire()
				require.NoError(t, err)
				require.Same(t, address, &held[i].Bytes()[0])
			}
			for _, b := range held {
				b.Unpin()
			}
		})
	}
}

// The slab borrows the caller's region without owning it: closing the slab
// leaves the region with the caller.
func TestSlabAllocatorBacking(t *testing.T) {
	extents, err := NewExtentAllocator(8192, 1)
	require.NoError(t, err)
	defer extents.Close()
	backing, err := extents.TryAcquire()
	require.NoError(t, err)
	p, err := NewSlabAllocator(0, WithBacking(backing.Bytes()), WithWarmup())
	require.NoError(t, err)
	require.EqualValues(t, 8192, p.Capacity())
	b, err := p.TryAcquire()
	require.NoError(t, err)
	b.Bytes()[0] = 42
	require.Equal(t, byte(42), backing.Bytes()[0], "chunks come from the supplied region")
	b.Unpin()
	p.Close()
	_, err = extents.TryAcquire()
	require.ErrorIs(t, err, ErrExhausted, "the caller still owns the region")
	backing.Unpin()
	reused, err := extents.TryAcquire()
	require.NoError(t, err)
	reused.Unpin()

	region := NewMemory(8192)
	defer region.Unpin()
	_, err = NewSlabAllocator(0, WithBacking(region.Bytes()[1:]))
	require.ErrorContains(t, err, "page-aligned")
	_, err = NewSlabAllocator(0, WithBacking(region.Bytes()), WithChunkSize(3*4096))
	require.ErrorContains(t, err, "at least one chunk")
}

func TestSlabAllocatorAcquireRetry(t *testing.T) {
	for _, n := range []int{1, 65} {
		for _, interval := range []time.Duration{100 * time.Millisecond, time.Second} {
			t.Run(fmt.Sprintf("n%d/%s", n, interval), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var opts []Option
					if interval != 100*time.Millisecond {
						opts = append(opts, WithRetryInterval(interval))
					}
					p, err := NewSlabAllocator(int64(n*4096), opts...)
					require.NoError(t, err)
					defer p.Close()
					start := time.Now()
					held := make([]Memory, n)
					for i := range held {
						held[i], err = p.Acquire(context.Background())
						require.NoError(t, err)
					}
					require.Equal(t, start, time.Now(), "first attempt must be immediate")
					type result struct {
						memory Memory
						err    error
					}
					results := make(chan result, 1)
					go func() {
						m, err := p.Acquire(context.Background())
						results <- result{m, err}
					}()
					synctest.Wait()
					time.Sleep(interval) // First retry still finds every chunk occupied.
					synctest.Wait()
					require.Empty(t, results)
					time.Sleep(interval / 2)
					released := &held[n-1].Bytes()[0]
					held[n-1].Unpin()
					synctest.Wait()
					require.Empty(t, results, "release does not wake the retry timer")
					time.Sleep(interval / 2)
					synctest.Wait()
					require.Len(t, results, 1, "second retry must acquire the released chunk")
					acquired := <-results
					require.NoError(t, acquired.err)
					require.Same(t, released, &acquired.memory.Bytes()[0])
					acquired.memory.Unpin()
					for _, m := range held[:n-1] {
						m.Unpin()
					}
				})
			})
		}
	}
}

func TestSlabAllocatorAcquireCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, err := NewSlabAllocator(8192, WithRetryInterval(time.Second))
		require.NoError(t, err)
		defer p.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for range 2 {
			held, err := p.Acquire(ctx) // Available capacity wins without checking ctx.
			require.NoError(t, err)
			defer held.Unpin()
		}
		_, err = p.Acquire(ctx)
		require.ErrorIs(t, err, context.Canceled)

		start := time.Now()
		ctx, cancel = context.WithCancel(context.Background())
		var waitErr error
		go func() { _, waitErr = p.Acquire(ctx) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		require.ErrorIs(t, waitErr, context.Canceled)
		require.Equal(t, start, time.Now(), "cancellation must not wait for the retry timer")

		ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err = p.Acquire(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 100*time.Millisecond, time.Since(start))
	})
}

func TestSlabAllocatorChunkSize(t *testing.T) {
	for _, whole := range []bool{false, true} {
		name, count, size := "default", 2, 4096
		var opts []Option
		if whole {
			name, count, size = "whole", 1, 8192
			opts = append(opts, WithChunkSize(4097))
		}
		t.Run(name, func(t *testing.T) {
			p, err := NewSlabAllocator(4097, opts...)
			require.NoError(t, err)
			defer p.Close()
			for range count {
				b, err := p.TryAcquire()
				require.NoError(t, err)
				defer b.Unpin()
				require.Equal(t, size, len(b.Bytes()))
			}
			_, err = p.TryAcquire()
			require.ErrorIs(t, err, ErrExhausted)
		})
	}
}
