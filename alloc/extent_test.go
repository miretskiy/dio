package alloc

import (
	"context"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func newExtentAllocator(t *testing.T, size int64, capacity int, opts ...Option) *ExtentAllocator {
	t.Helper()
	p, err := NewExtentAllocator(size, capacity, opts...)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	return p
}

func TestExtentAllocatorCapacityAndReuse(t *testing.T) {
	p := newExtentAllocator(t, 4097, 2, WithWarmup())
	first, err := p.Acquire(context.Background(), 4096)
	require.NoError(t, err)
	require.Equal(t, 8192, len(first.Chunks()[0]))
	require.True(t, align.IsAligned(first.Chunks()[0]))
	second, err := p.TryAcquire(4096)
	require.NoError(t, err)
	_, err = p.TryAcquire(4096)
	require.ErrorIs(t, err, ErrExhausted)
	require.True(t, first.TryInc())
	first.Unpin()
	_, err = p.TryAcquire(4096)
	require.ErrorIs(t, err, ErrExhausted, "one reference still holds the first slab")
	address := &first.Chunks()[0][0]
	first.Unpin()
	next, err := p.TryAcquire(4096)
	require.NoError(t, err)
	require.Equal(t, address, &next.Chunks()[0][0])
	require.NotSame(t, first, next)
	require.False(t, first.TryInc())
	require.Panics(t, func() { first.Unpin() })
	_, err = p.TryAcquire(4096)
	require.ErrorIs(t, err, ErrExhausted, "stale buffer must not release the new lease")
	next.Unpin()
	second.Unpin()
	require.Zero(t, p.slots.Len())
}

func TestExtentAllocatorAcquireWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newExtentAllocator(t, 4096, 3)
		one, err := p.TryAcquire(4096)
		require.NoError(t, err)
		two, err := p.TryAcquire(8192)
		require.NoError(t, err)
		var small, large *Buffer
		var smallErr, largeErr error
		go func() { large, largeErr = p.Acquire(context.Background(), 8192) }()
		synctest.Wait()
		go func() { small, smallErr = p.Acquire(context.Background(), 1) }()
		synctest.Wait()
		require.Nil(t, small)
		require.Nil(t, large)
		one.Unpin()
		synctest.Wait()
		require.NoError(t, smallErr)
		require.NotNil(t, small, "release must wake a waiter whose request fits")
		require.Nil(t, large, "larger request must wait for enough capacity")
		two.Unpin()
		synctest.Wait()
		require.NoError(t, largeErr)
		require.NotNil(t, large)
		require.Len(t, large.Chunks(), 2)
		small.Unpin()
		large.Unpin()
	})
}

func TestExtentAllocatorConfigurationAndClose(t *testing.T) {
	for _, n := range []int{-1, 0, 1} {
		p, err := NewExtentAllocator(4096, n)
		require.NoError(t, err)
		require.Equal(t, 1, p.slots.Cap())
		p.Close()
		_, err = p.Acquire(context.Background(), 4096)
		require.ErrorIs(t, err, ErrClosed)
		_, err = p.TryAcquire(4096)
		require.ErrorIs(t, err, ErrClosed)
	}
}

func TestExtentAllocatorMappingFailurePanics(t *testing.T) {
	if math.MaxInt != math.MaxInt64 {
		t.Skip("requires a size beyond the process address space")
	}
	const size = math.MaxInt64 &^ align.BlockMask
	require.Panics(t, func() { NewExtentAllocator(size, 1) })
}

func TestExtentAllocatorCloseReleasesReturnedAndUnusedSlabs(t *testing.T) {
	p, err := NewExtentAllocator(4096, 2)
	require.NoError(t, err)
	var mappings [][]byte
	for _, data := range p.slots.AllSlots() {
		mappings = append(mappings, (*data).Chunks()[0])
	}
	b, err := p.Acquire(context.Background(), 4096)
	require.NoError(t, err)
	b.Unpin()
	p.Close()
	for _, data := range mappings {
		require.Error(t, unix.Munmap(data), "Close must unmap even free slots")
	}
}

func TestExtentAllocatorAcquireAlreadyCancelled(t *testing.T) {
	p := newExtentAllocator(t, 4096, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b, err := p.Acquire(ctx, 4096)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, b)
	require.Zero(t, p.slots.Len())
	b, err = p.TryAcquire(4096)
	require.NoError(t, err)
	b.Unpin()
}

func TestExtentAllocatorAcquireCancelledWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newExtentAllocator(t, 4096, 1)
		held, err := p.TryAcquire(4096)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		liveCtx, cancelLive := context.WithCancel(context.Background())
		defer cancelLive()
		var cancelled, live *Buffer
		var cancelledErr, liveErr error
		var liveDone bool
		go func() { cancelled, cancelledErr = p.Acquire(ctx, 4096) }()
		go func() {
			live, liveErr = p.Acquire(liveCtx, 4096)
			liveDone = true
		}()
		synctest.Wait() // Both calls are waiting for the held slab.
		cancel()
		synctest.Wait()
		require.ErrorIs(t, cancelledErr, context.Canceled)
		require.Nil(t, cancelled)
		require.False(t, liveDone, "cancellation must not create capacity")
		held.Unpin()
		synctest.Wait()
		require.True(t, liveDone)
		require.NoError(t, liveErr)
		require.NotNil(t, live)
		live.Unpin()
		require.Zero(t, p.slots.Len())
	})
}

func TestExtentAllocatorAcquireDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newExtentAllocator(t, 4096, 1)
		held, err := p.TryAcquire(4096)
		require.NoError(t, err)
		defer held.Unpin()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		b, err := p.Acquire(ctx, 4096)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, b)
		_, err = p.TryAcquire(4096)
		require.ErrorIs(t, err, ErrExhausted)
	})
}

func TestExtentAllocatorAcquireCancellationRacesRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newExtentAllocator(t, 4096, 1)
		for range 100 {
			held, err := p.TryAcquire(4096)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			var firstErr, secondErr error
			var firstDone, secondDone bool
			go func() {
				b, err := p.Acquire(ctx, 4096)
				if b != nil {
					b.Unpin()
				}
				firstErr, firstDone = err, true
			}()
			go func() {
				b, err := p.Acquire(context.Background(), 4096)
				if b != nil {
					b.Unpin()
				}
				secondErr, secondDone = err, true
			}()
			synctest.Wait()
			go cancel()
			held.Unpin()
			synctest.Wait()
			require.True(t, firstDone)
			require.True(t, secondDone, "cancelled waiter must not strand another waiter")
			if firstErr != nil {
				require.ErrorIs(t, firstErr, context.Canceled)
			}
			require.NoError(t, secondErr)
			require.Zero(t, p.slots.Len())
		}
	})
}
