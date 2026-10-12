package alloc

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func newTestPool(t *testing.T, size int64, capacity int, opts ...Option) *MmapPool {
	t.Helper()
	p, err := NewMmapPool(size, capacity, opts...)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	return p
}

func TestPoolCapacityAndReuse(t *testing.T) {
	p := newTestPool(t, 4097, 2)
	first, err := p.Acquire(context.Background())
	require.NoError(t, err)
	require.Equal(t, 8192, first.Cap())
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
	require.NotSame(t, first, next)
	require.False(t, first.TryInc())
	require.Panics(t, first.Unpin)
	_, err = p.TryAcquire()
	require.ErrorIs(t, err, ErrExhausted, "stale buffer must not release the new lease")
	next.Unpin()
	second.Unpin()
	require.Zero(t, p.slots.Len())
}

func TestPoolAcquireWaits(t *testing.T) {
	p := newTestPool(t, 4096, 1)
	held, err := p.Acquire(context.Background())
	require.NoError(t, err)
	acquired := make(chan *Buffer, 1)
	go func() {
		b, err := p.Acquire(context.Background())
		if err != nil {
			t.Error(err)
		}
		acquired <- b
	}()
	select {
	case b := <-acquired:
		if b != nil {
			b.Unpin()
		}
		t.Fatal("Acquire did not wait for capacity")
	case <-time.After(20 * time.Millisecond):
	}
	held.Unpin()
	select {
	case b := <-acquired:
		require.NotNil(t, b)
		b.Unpin()
	case <-time.After(time.Second):
		t.Fatal("Acquire did not resume after release")
	}
}

func TestPoolLazyReuse(t *testing.T) {
	mapped := func(p *MmapPool) int {
		count := 0
		for _, data := range p.slots.AllSlots() {
			if *data != nil {
				count++
			}
		}
		return count
	}
	for _, lazy := range []int{0, 2, 4, 10} {
		t.Run(fmt.Sprint(lazy), func(t *testing.T) {
			p := newTestPool(t, 4096, 4, WithLazyInitSlabs(lazy))
			initial := max(0, 4-lazy)
			require.Equal(t, initial, mapped(p))
			for range 10 {
				b, err := p.Acquire(context.Background())
				require.NoError(t, err)
				b.Unpin()
			}
			require.Equal(t, max(1, initial), mapped(p), "serial use must reuse a mapping")
			var held []*Buffer
			for range p.Capacity() {
				b, err := p.TryAcquire()
				require.NoError(t, err)
				held = append(held, b)
			}
			_, err := p.TryAcquire()
			require.ErrorIs(t, err, ErrExhausted)
			for _, b := range held {
				b.Unpin()
			}
			require.Equal(t, p.Capacity(), mapped(p))
			require.Zero(t, p.slots.Len())
		})
	}
}

func TestPoolConcurrent(t *testing.T) {
	for _, lazy := range []int{0, 4, 8} {
		t.Run(fmt.Sprint(lazy), func(t *testing.T) {
			p := newTestPool(t, 4096, 8, WithLazyInitSlabs(lazy))
			var wg sync.WaitGroup
			for range 32 {
				wg.Go(func() {
					for i := range 200 {
						var b *Buffer
						var err error
						if i%3 == 0 {
							b, err = p.TryAcquire()
							if err == ErrExhausted {
								continue
							}
						} else {
							b, err = p.Acquire(context.Background())
						}
						if err != nil {
							t.Error(err)
							return
						}
						b.Bytes()[0] = byte(i)
						b.Unpin()
					}
				})
			}
			wg.Wait()
			require.Zero(t, p.slots.Len())
		})
	}
}

func TestPoolConfigurationAndClose(t *testing.T) {
	for _, size := range []int64{-1, 0, math.MaxInt64} {
		p, err := NewMmapPool(size, 1, WithLazyInitSlabs(1))
		require.Error(t, err)
		require.Nil(t, p)
	}
	_, err := NewMmapPool(4096, 1, WithLazyInitSlabs(-1))
	require.Error(t, err)
	for _, n := range []int{-1, 0, 1} {
		p, err := NewMmapPool(4096, n)
		require.NoError(t, err)
		require.Equal(t, 1, p.Capacity())
		p.Close()
		_, err = p.Acquire(context.Background())
		require.ErrorIs(t, err, ErrClosed)
		_, err = p.TryAcquire()
		require.ErrorIs(t, err, ErrClosed)
	}
}

func TestPoolMappingFailurePanics(t *testing.T) {
	if math.MaxInt != math.MaxInt64 {
		t.Skip("requires a size beyond the process address space")
	}
	const size = math.MaxInt64 &^ align.BlockMask
	require.Panics(t, func() { NewMmapPool(size, 1) })
	p := newTestPool(t, size, 1, WithLazyInitSlabs(1))
	require.Panics(t, func() { p.Acquire(context.Background()) })
	p = newTestPool(t, size, 1, WithLazyInitSlabs(1))
	require.Panics(t, func() { p.TryAcquire() })
}

func TestPoolCloseReleasesReturnedAndUnusedSlabs(t *testing.T) {
	p, err := NewMmapPool(4096, 2)
	require.NoError(t, err)
	var mappings [][]byte
	for _, data := range p.slots.AllSlots() {
		mappings = append(mappings, *data)
	}
	b, err := p.Acquire(context.Background())
	require.NoError(t, err)
	b.Unpin()
	p.Close()
	for _, data := range mappings {
		require.Error(t, unix.Munmap(data), "Close must unmap even free slots")
	}
}

func TestPoolAcquireAlreadyCancelled(t *testing.T) {
	p := newTestPool(t, 4096, 1, WithLazyInitSlabs(1))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b, err := p.Acquire(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, b)
	require.Zero(t, p.slots.Len())
	for _, data := range p.slots.AllSlots() {
		require.Nil(t, *data, "cancelled acquisition must not map memory")
	}
	b, err = p.TryAcquire()
	require.NoError(t, err)
	b.Unpin()
}

func TestPoolAcquireCancelledWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newTestPool(t, 4096, 1)
		held, err := p.TryAcquire()
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		liveCtx, cancelLive := context.WithCancel(context.Background())
		defer cancelLive()
		var cancelled, live *Buffer
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

func TestPoolAcquireDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newTestPool(t, 4096, 1)
		held, err := p.TryAcquire()
		require.NoError(t, err)
		defer held.Unpin()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		b, err := p.Acquire(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, b)
		_, err = p.TryAcquire()
		require.ErrorIs(t, err, ErrExhausted)
	})
}

func TestPoolAcquireCancellationRacesRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newTestPool(t, 4096, 1)
		for range 100 {
			held, err := p.TryAcquire()
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			var firstErr, secondErr error
			var firstDone, secondDone bool
			go func() {
				b, err := p.Acquire(ctx)
				if b != nil {
					b.Unpin()
				}
				firstErr, firstDone = err, true
			}()
			go func() {
				b, err := p.Acquire(context.Background())
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
