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
			held := make([]*Buffer, chunks)
			addresses := make(map[*byte]bool)
			for i := range held {
				held[i], err = p.TryAcquire(4096)
				require.NoError(t, err)
				b := held[i]
				require.Equal(t, 8192, len(b.Chunks()[0]))
				require.Equal(t, 8192, cap(b.Chunks()[0]))
				require.True(t, align.IsAligned(b.Chunks()[0]))
				require.False(t, addresses[&b.Chunks()[0][0]])
				addresses[&b.Chunks()[0][0]] = true
				b.Chunks()[0][0] = byte(i)
			}
			_, err = p.TryAcquire(4096)
			require.ErrorIs(t, err, ErrExhausted)
			for i := len(held) - 1; i >= 0; i-- {
				b := held[i]
				require.Equal(t, byte(i), b.Chunks()[0][0])
				address := &b.Chunks()[0][0]
				b.Unpin()
				held[i], err = p.TryAcquire(4096)
				require.NoError(t, err)
				require.Same(t, address, &held[i].Chunks()[0][0])
			}
			for _, b := range held {
				b.Unpin()
			}
		})
	}
}

func TestSlabAllocatorBacking(t *testing.T) {
	for _, first := range []string{"caller", "slab"} {
		t.Run(first+"_releases_first", func(t *testing.T) {
			extents, err := NewExtentAllocator(8192, 1)
			require.NoError(t, err)
			defer extents.Close()
			backing, err := extents.TryAcquire(4096)
			require.NoError(t, err)
			p, err := NewSlabAllocator(8192, WithBackingBuffer(backing), WithWarmup())
			require.NoError(t, err)
			defer p.Close()
			if first == "caller" {
				backing.Unpin()
			}
			b, err := p.TryAcquire(4096)
			require.NoError(t, err)
			b.Chunks()[0][0] = 42
			_, err = extents.TryAcquire(4096)
			require.ErrorIs(t, err, ErrExhausted)
			b.Unpin()
			p.Close()
			if first == "slab" {
				backing.Chunks()[0][0] = 43
				_, err = extents.TryAcquire(4096)
				require.ErrorIs(t, err, ErrExhausted)
				backing.Unpin()
			}
			reused, err := extents.TryAcquire(4096)
			require.NoError(t, err)
			reused.Unpin()
		})
	}
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
					held, err := p.Acquire(context.Background(), 4096)
					require.NoError(t, err)
					require.Equal(t, start, time.Now(), "first attempt must be immediate")
					type result struct {
						buffer *Buffer
						err    error
					}
					results := make(chan result, 1)
					go func() {
						b, err := p.Acquire(context.Background(), int64(n*4096))
						results <- result{b, err}
					}()
					synctest.Wait()
					time.Sleep(interval) // First retry still finds an occupied chunk.
					synctest.Wait()
					require.Empty(t, results)
					time.Sleep(interval / 2)
					held.Unpin()
					synctest.Wait()
					require.Empty(t, results, "release does not wake the retry timer")
					time.Sleep(interval / 2)
					synctest.Wait()
					require.Len(t, results, 1, "second retry must acquire the released chunk")
					acquired := <-results
					require.NoError(t, acquired.err)
					require.Len(t, acquired.buffer.Chunks(), n)
					acquired.buffer.Unpin()
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
		held, err := p.Acquire(ctx, 4096) // Available capacity wins without checking ctx.
		require.NoError(t, err)
		defer held.Unpin()
		_, err = p.Acquire(ctx, 8192)
		require.ErrorIs(t, err, context.Canceled)

		start := time.Now()
		ctx, cancel = context.WithCancel(context.Background())
		var waitErr error
		go func() { _, waitErr = p.Acquire(ctx, 8192) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		require.ErrorIs(t, waitErr, context.Canceled)
		require.Equal(t, start, time.Now(), "cancellation must not wait for the retry timer")

		ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err = p.Acquire(ctx, 8192)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 100*time.Millisecond, time.Since(start))
		remaining, err := p.TryAcquire(4096)
		require.NoError(t, err)
		remaining.Unpin()
	})
}

func TestSlabAllocatorFragmentedChunks(t *testing.T) {
	for _, layout := range []struct{ n, chunk int }{{3, 4096}, {64, 4096}, {65, 4096}, {65, 3 * 4096}} {
		n, chunk := layout.n, layout.chunk
		t.Run(fmt.Sprintf("n%d/chunk%d", n, chunk), func(t *testing.T) {
			capacity := 2*n + 1
			p, err := NewSlabAllocator(int64(capacity*chunk), WithChunkSize(int64(chunk)))
			require.NoError(t, err)
			defer p.Close()
			held := make([]*Buffer, capacity)
			for i := range held {
				held[i], err = p.TryAcquire(int64(chunk))
				require.NoError(t, err)
				held[i].Chunks()[0][0] = byte(i)
			}
			free := make(map[*byte]bool)
			for i := 1; i < capacity; i += 2 {
				free[&held[i].Chunks()[0][0]] = true
				held[i].Unpin()
				held[i] = nil
			}
			b, err := p.TryAcquire(int64((n + 1) * chunk))
			require.ErrorIs(t, err, ErrExhausted)
			require.Nil(t, b)
			// Start at the last shard to exercise release across a wrapped scan.
			b = p.slab.tryAcquireN(len(p.slab.shards)-1, n)
			require.NotNil(t, b, "failed acquisition must return all partial claims")
			require.Len(t, b.Chunks(), n)
			for _, data := range b.Chunks() {
				require.True(t, free[&data[0]], "must use only released chunks")
				delete(free, &data[0])
				require.Len(t, data, chunk)
				require.Equal(t, chunk, cap(data))
				require.True(t, align.IsAligned(data))
				data[0] = 255
			}
			require.Empty(t, free)
			require.True(t, b.TryInc())
			b.Unpin()
			_, err = p.TryAcquire(4096)
			require.ErrorIs(t, err, ErrExhausted, "one reference still owns every claim")
			b.Unpin()
			for i, other := range held {
				if other != nil {
					require.Equal(t, byte(i), other.Chunks()[0][0])
					other.Unpin()
				}
			}
			b, err = p.TryAcquire(int64(capacity * chunk))
			require.NoError(t, err)
			require.Len(t, b.Chunks(), capacity)
			_, err = p.TryAcquire(4096)
			require.ErrorIs(t, err, ErrExhausted, "padding bits must stay occupied")
			b.Unpin()
		})
	}
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
				b, err := p.TryAcquire(4096)
				require.NoError(t, err)
				defer b.Unpin()
				require.Equal(t, size, len(b.Chunks()[0]))
			}
			_, err = p.TryAcquire(4096)
			require.ErrorIs(t, err, ErrExhausted)
		})
	}
}
