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
	first, err := p.Acquire(context.Background())
	require.NoError(t, err)
	require.Equal(t, 8192, len(first.Bytes()))
	require.True(t, align.IsAligned(first.Bytes()))
	second, err := p.TryAcquire()
	require.NoError(t, err)
	_, err = p.TryAcquire()
	require.ErrorIs(t, err, ErrExhausted)
	require.True(t, first.TryInc())
	first.Unpin()
	_, err = p.TryAcquire()
	require.ErrorIs(t, err, ErrExhausted, "one reference still holds the first slab")
	address := &first.Bytes()[0]
	first.Unpin()
	next, err := p.TryAcquire()
	require.NoError(t, err)
	require.Equal(t, address, &next.Bytes()[0])
	require.NotEqual(t, first, next, "a new lease has a new generation")
	require.False(t, first.TryInc())
	require.Panics(t, func() { first.Unpin() })
	_, err = p.TryAcquire()
	require.ErrorIs(t, err, ErrExhausted, "stale memory must not release the new lease")
	next.Unpin()
	second.Unpin()
	require.Zero(t, p.slots.Len())
}

func TestExtentAllocatorAcquireWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newExtentAllocator(t, 4096, 2)
		one, err := p.TryAcquire()
		require.NoError(t, err)
		two, err := p.TryAcquire()
		require.NoError(t, err)
		var acquired [2]Memory
		var errs [2]error
		for i := range acquired {
			go func() { acquired[i], errs[i] = p.Acquire(context.Background()) }()
		}
		synctest.Wait()
		require.Equal(t, [2]Memory{}, acquired)
		one.Unpin()
		synctest.Wait()
		require.Equal(t, [2]error{}, errs)
		require.True(t, (acquired[0].leases == nil) != (acquired[1].leases == nil), "release must wake exactly one waiter")
		two.Unpin()
		synctest.Wait()
		require.NotZero(t, acquired[0])
		require.NotZero(t, acquired[1])
		acquired[0].Unpin()
		acquired[1].Unpin()
	})
}

func TestExtentAllocatorConfigurationAndClose(t *testing.T) {
	for _, n := range []int{-1, 0, 1} {
		p, err := NewExtentAllocator(4096, n)
		require.NoError(t, err)
		require.Equal(t, 1, p.slots.Cap())
		p.Close()
		_, err = p.Acquire(context.Background())
		require.ErrorIs(t, err, ErrClosed)
		_, err = p.TryAcquire()
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
	for _, slot := range p.slots.AllSlots() {
		mappings = append(mappings, slot.data)
	}
	b, err := p.Acquire(context.Background())
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
	b, err := p.Acquire(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, b)
	require.Zero(t, p.slots.Len())
	b, err = p.TryAcquire()
	require.NoError(t, err)
	b.Unpin()
}

func TestExtentAllocatorAcquireCancelledWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newExtentAllocator(t, 4096, 1)
		held, err := p.TryAcquire()
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		liveCtx, cancelLive := context.WithCancel(context.Background())
		defer cancelLive()
		var cancelled, live Memory
		var cancelledErr, liveErr error
		var liveDone bool
		go func() { cancelled, cancelledErr = p.Acquire(ctx) }()
		go func() {
			live, liveErr = p.Acquire(liveCtx)
			liveDone = true
		}()
		synctest.Wait() // Both calls are waiting for the held slab.
		cancel()
		synctest.Wait()
		require.ErrorIs(t, cancelledErr, context.Canceled)
		require.Zero(t, cancelled)
		require.False(t, liveDone, "cancellation must not create capacity")
		held.Unpin()
		synctest.Wait()
		require.True(t, liveDone)
		require.NoError(t, liveErr)
		require.NotZero(t, live)
		live.Unpin()
		require.Zero(t, p.slots.Len())
	})
}

func TestExtentAllocatorAcquireDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newExtentAllocator(t, 4096, 1)
		held, err := p.TryAcquire()
		require.NoError(t, err)
		defer held.Unpin()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		b, err := p.Acquire(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Zero(t, b)
		_, err = p.TryAcquire()
		require.ErrorIs(t, err, ErrExhausted)
	})
}

func TestExtentAllocatorAcquireCancellationRacesRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newExtentAllocator(t, 4096, 1)
		for range 100 {
			held, err := p.TryAcquire()
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			var firstErr, secondErr error
			var firstDone, secondDone bool
			go func() {
				b, err := p.Acquire(ctx)
				if err == nil {
					b.Unpin()
				}
				firstErr, firstDone = err, true
			}()
			go func() {
				b, err := p.Acquire(context.Background())
				if err == nil {
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
