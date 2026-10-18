package alloc

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
			first, err := a.Acquire(context.Background(), 4096)
			require.NoError(t, err)
			second, err := a.TryAcquire(4096)
			require.NoError(t, err)
			require.Equal(t, 4096, len(first.Chunks()[0]))
			require.Equal(t, 4096, len(second.Chunks()[0]))
			first.Chunks()[0][0], second.Chunks()[0][0] = 1, 2
			require.Equal(t, byte(1), first.Chunks()[0][0])
			require.True(t, first.TryInc())
			first.Unpin()
			_, err = a.TryAcquire(4096)
			require.ErrorIs(t, err, ErrExhausted)
			first.Unpin()
			next, err := a.TryAcquire(4096)
			require.NoError(t, err)
			require.NotSame(t, first, next)
			require.False(t, first.TryInc())
			require.Panics(t, first.Unpin)
			require.Equal(t, byte(2), second.Chunks()[0][0])
			next.Unpin()
			second.Unpin()
			a.Close()
			a.Close()
		})
	}
}

func TestAllocatorSizes(t *testing.T) {
	const chunk = 3 * 4096 // Chunk rounding must also work for non-powers of two.
	for _, kind := range []string{"extent", "slab"} {
		for _, size := range []int64{1, chunk - 1, chunk, chunk + 1, 2 * chunk, 2*chunk + 1} {
			t.Run(fmt.Sprintf("%s/bytes%d", kind, size), func(t *testing.T) {
				var a Allocator
				var err error
				if kind == "extent" {
					a, err = NewExtentAllocator(chunk-1, 3)
				} else {
					a, err = NewSlabAllocator(3*chunk, WithChunkSize(chunk-1))
				}
				require.NoError(t, err)
				defer a.Close()
				b, err := a.Acquire(context.Background(), size)
				require.NoError(t, err)
				n := int((size-1)/chunk + 1)
				require.Len(t, b.Chunks(), n)
				addresses := make(map[*byte]bool)
				for _, data := range b.Chunks() {
					require.Len(t, data, chunk)
					require.Equal(t, chunk, cap(data))
					require.False(t, addresses[&data[0]])
					addresses[&data[0]] = true
				}
				_, err = a.TryAcquire(int64((3-n)*chunk + 1))
				require.ErrorIs(t, err, ErrExhausted)
				if n < 3 {
					rest, err := a.TryAcquire(int64((3 - n) * chunk))
					require.NoError(t, err, "failed acquisition must return partial claims")
					for _, data := range rest.Chunks() {
						require.False(t, addresses[&data[0]])
					}
					rest.Unpin()
				}
				require.True(t, b.TryInc())
				b.Unpin()
				_, err = a.TryAcquire(3 * chunk)
				require.ErrorIs(t, err, ErrExhausted)
				b.Unpin()
				all, err := a.TryAcquire(3 * chunk)
				require.NoError(t, err)
				require.Len(t, all.Chunks(), 3)
				all.Unpin()
			})
		}
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
							n := 1 + i%min(chunks, 17)
							var b *Buffer
							var err error
							if i%3 == 0 {
								b, err = p.TryAcquire(int64(n * 4096))
								if err == ErrExhausted {
									continue
								}
							} else {
								b, err = p.Acquire(context.Background(), int64(n*4096))
							}
							if err != nil {
								t.Error(err)
								return
							}
							for _, data := range b.Chunks() {
								if _, exists := live.LoadOrStore(&data[0], true); exists {
									t.Error("overlapping live buffers")
								}
								data[0] = byte(worker)
							}
							runtime.Gosched()
							for _, data := range b.Chunks() {
								if data[0] != byte(worker) {
									t.Error("buffer overwritten by another borrower")
								}
								live.Delete(&data[0])
							}
							b.Unpin()
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
					backing := NewBuffer(int64(chunks * chunk))
					defer backing.Unpin()
					for i := range backing.Chunks()[0] {
						backing.Chunks()[0][i] = 0xa5
					}
					a, err = NewSlabAllocator(int64(chunks*chunk), WithBackingBuffer(backing))
				}
				require.NoError(t, err)
				defer a.Close()
				for _, acquire := range []func(int64) (*Buffer, error){
					a.TryAcquire,
					func(size int64) (*Buffer, error) { return a.Acquire(context.Background(), size) },
					a.TryAcquire,
				} {
					b, err := acquire(int64((chunks-1)*chunk + 1))
					require.NoError(t, err)
					require.Len(t, b.Chunks(), chunks)
					for _, data := range b.Chunks() {
						require.Equal(t, make([]byte, chunk), data)
						for i := range data {
							data[i] = 0xa5
						}
					}
					b.Unpin()
				}
			})
		}
	}
}
