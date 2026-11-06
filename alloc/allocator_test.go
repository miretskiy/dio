package alloc

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"unsafe"

	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
)

func newAllocator(t testing.TB, size int64, opts ...Option) *Allocator {
	t.Helper()
	a := NewAllocator(size, opts...)
	t.Cleanup(a.Close)
	return a
}

// pageOf returns the first page of m within the arena.
func (a *Allocator) pageOf(m Memory) int {
	return int(uintptr(unsafe.Pointer(&m.Bytes()[0]))-uintptr(unsafe.Pointer(&a.arena.region[0]))) / align.BlockSize
}

func (a *Allocator) requireEmpty(t *testing.T) {
	t.Helper()
	require.Equal(t, a.Capacity(), a.Available())
	for i := range a.arena.words {
		require.Equal(t, emptySpan, spanWord(a.arena.words[i].Load()), "span %d", i)
	}
}

func TestAllocatorSizes(t *testing.T) {
	a := newAllocator(t, 64*spanBytes)
	for _, test := range []struct {
		size  int64
		pages int
	}{
		{1, 1}, {4096, 1}, {4097, 2}, {8192, 2}, {12 << 10, 4}, {14 << 10, 4}, {16 << 10, 4},
		{5 * 4096, 8}, {7 * 4096, 8}, {32 << 10, 8}, {32<<10 + 1, 16}, {16 * 4096, 16}, {16*4096 + 1, 32},
		{30 * 4096, 32}, {31*4096 + 1, 32}, {MaxAllocationSize, 32},
		{MaxAllocationSize + 1, 32}, {320 << 10, 32}, {1<<63 - 1, 32},
	} {
		t.Run(fmt.Sprint(test.size), func(t *testing.T) {
			m, err := a.TryAcquire(test.size)
			require.NoError(t, err)
			data := m.Bytes()
			page := a.pageOf(m)
			require.LessOrEqual(t, page%spanPages+test.pages, spanPages)
			require.Len(t, data, test.pages*align.BlockSize)
			require.Zero(t, page%test.pages, "buddy blocks are aligned to their size")
			require.Equal(t, a.Capacity()-int64(len(data)), a.Available())
			require.Equal(t, len(data), cap(data))
			require.True(t, align.IsAligned(data))
			require.Equal(t, make([]byte, len(data)), data)
			for i := range data {
				data[i] = 0xa5
			}
			require.True(t, m.Unpin())
		})
	}
	require.EqualValues(t, MaxAllocationSize, a.MaxSize())
	a.requireEmpty(t)
}

func TestAllocatorInvalidSize(t *testing.T) {
	a := newAllocator(t, spanBytes)
	for _, occupied := range []bool{false, true} {
		t.Run(fmt.Sprintf("occupied%t", occupied), func(t *testing.T) {
			if occupied {
				m, err := a.TryAcquire(spanBytes)
				require.NoError(t, err)
				defer m.Unpin()
			}
			available := a.Available()
			for _, size := range []int64{-1 << 63, -1, 0} {
				require.Panics(t, func() { a.TryAcquire(size) }, "size %d", size)
				require.Equal(t, available, a.Available(), "invalid requests must not change occupancy")
			}
		})
	}
	a.requireEmpty(t)
}

// The requested limit must never become the slice length or release extent
// when a span supplies a smaller block. Acquire shares TryAcquire's contract.
func TestAllocatorPartialBlock(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry%t", retry), func(t *testing.T) {
			a := newAllocator(t, spanBytes)
			held, err := a.TryAcquire(64 << 10)
			require.NoError(t, err)
			for i := range held.Bytes() {
				held.Bytes()[i] = 0xa5
			}
			var m Memory
			if retry {
				m, err = a.Acquire(context.Background(), spanBytes)
			} else {
				m, err = a.TryAcquire(spanBytes)
			}
			require.NoError(t, err)
			require.Len(t, m.Bytes(), 64<<10)
			require.Equal(t, len(m.Bytes()), cap(m.Bytes()))
			require.Equal(t, halfPages, a.pageOf(m))
			require.Zero(t, a.Available())
			for i := range m.Bytes() {
				m.Bytes()[i] = 0x5a
			}
			m.Unpin()
			require.EqualValues(t, 64<<10, a.Available())
			for _, value := range held.Bytes() {
				require.Equal(t, byte(0xa5), value)
			}
			held.Unpin()
			a.requireEmpty(t)
		})
	}
}

// Large allocations must report and release their actual extent, including
// unaligned starts and non-power-of-two lengths. Keep a live page beside them
// to detect overlap, accidental zeroing, or premature reuse while pinned.
func TestAllocatorLargeBlock(t *testing.T) {
	for page := range spanPages {
		t.Run(fmt.Sprint(page), func(t *testing.T) {
			a := newAllocator(t, spanBytes)
			var held [spanPages]Memory
			for i := range held {
				m, err := a.TryAcquire(align.BlockSize)
				require.NoError(t, err)
				held[i] = m
			}
			for i, m := range held {
				if i != page {
					m.Unpin()
				}
			}
			neighbor := held[page]
			for i := range neighbor.Bytes() {
				neighbor.Bytes()[i] = 0xa5
			}
			wantPages, wantStart := spanPages-page-1, page+1
			if page >= halfPages {
				wantPages, wantStart = page, 0
			}
			for range 2 { // Reacquire the released extent and check zeroing again.
				m, err := a.Acquire(context.Background(), MaxAllocationSize)
				require.NoError(t, err)
				require.Equal(t, wantStart, a.pageOf(m))
				require.Len(t, m.Bytes(), wantPages*align.BlockSize)
				require.Equal(t, len(m.Bytes()), cap(m.Bytes()))
				require.True(t, align.IsAligned(m.Bytes()))
				require.Equal(t, make([]byte, len(m.Bytes())), m.Bytes())
				for i := range m.Bytes() {
					m.Bytes()[i] = 0x5a
				}
				require.True(t, m.TryInc())
				require.False(t, m.Unpin())
				require.EqualValues(t, (spanPages-1-wantPages)*align.BlockSize, a.Available())
				require.True(t, m.Unpin())
				require.False(t, m.TryInc())
				require.EqualValues(t, (spanPages-1)*align.BlockSize, a.Available())
				for _, value := range neighbor.Bytes() {
					require.Equal(t, byte(0xa5), value)
				}
			}
			neighbor.Unpin()
			a.requireEmpty(t)
		})
	}
}

func TestAllocatorExhaustionAndReuse(t *testing.T) {
	for _, pages := range []int{1, 3, 16, 17, 32} {
		t.Run(fmt.Sprintf("pages%d", pages), func(t *testing.T) {
			a := newAllocator(t, 8*spanBytes)
			fit := 8 * (spanPages / (1 << levelOf(pages)))
			var held []Memory
			for {
				m, err := a.TryAcquire(int64(pages) * align.BlockSize)
				if err != nil {
					require.ErrorIs(t, err, ErrExhausted)
					break
				}
				held = append(held, m)
			}
			require.Len(t, held, fit)
			for _, m := range held {
				m.Unpin()
			}
			a.requireEmpty(t)
		})
	}
}

// A 14 KiB request reserves a 16 KiB subtree. Freeing it must clear that
// subtree without clearing ancestors still shared with a live 4 KiB buddy.
func TestAllocatorBuddyTree(t *testing.T) {
	a := newAllocator(t, spanBytes)
	large, err := a.TryAcquire(14 << 10)
	require.NoError(t, err)
	require.Len(t, large.Bytes(), 16<<10)
	require.Zero(t, a.pageOf(large))
	// Four leaves, two 8 KiB nodes, one 16 KiB node, and both ancestors.
	require.Equal(t, uint64(0x800000000007818b), a.arena.words[0].Load())
	small, err := a.TryAcquire(4096)
	require.NoError(t, err)
	require.Equal(t, 4, a.pageOf(small))
	small.Bytes()[0] = 0xa5
	large.Unpin()
	// Only the small allocation's leaf and ancestors remain.
	require.Equal(t, uint64(0x8000000000080213), a.arena.words[0].Load())
	again, err := a.TryAcquire(14 << 10)
	require.NoError(t, err, "a set ancestor must not hide a free sibling subtree")
	require.Zero(t, a.pageOf(again))
	require.Equal(t, byte(0xa5), small.Bytes()[0])
	again.Unpin()
	small.Unpin()
	a.requireEmpty(t)
}

// Random sizes from many goroutines, each holding some memory to fragment the
// arena. Every page has an owner; a claim of an owned page fails the test.
func TestAllocatorConcurrent(t *testing.T) {
	sizes := []int{1, 1, 1, 2, 3, 4, 5, 8, 12, 16, 17, 24, 31, 32}
	for _, spans := range []int{1, 65, 257} {
		t.Run(fmt.Sprintf("spans%d", spans), func(t *testing.T) {
			a := newAllocator(t, int64(spans)*spanBytes, WithoutZeroing())
			owners := make([]atomic.Int32, a.Capacity()/align.BlockSize)
			var wg sync.WaitGroup
			for worker := range 32 {
				wg.Go(func() {
					r := rand.New(rand.NewPCG(uint64(worker), 7))
					var held []Memory
					release := func(i int) {
						m := held[i]
						for p := a.pageOf(m); p < a.pageOf(m)+len(m.Bytes())/align.BlockSize; p++ {
							owners[p].Store(0)
						}
						m.Unpin()
						held[i] = held[len(held)-1]
						held = held[:len(held)-1]
					}
					for range 2000 {
						if len(held) == 8 || len(held) > 0 && r.IntN(2) == 0 {
							release(r.IntN(len(held)))
							continue
						}
						m, err := a.TryAcquire(int64(sizes[r.IntN(len(sizes))]) * align.BlockSize)
						if err == ErrExhausted {
							continue
						}
						if err != nil {
							t.Error(err)
							return
						}
						for p := a.pageOf(m); p < a.pageOf(m)+len(m.Bytes())/align.BlockSize; p++ {
							if !owners[p].CompareAndSwap(0, int32(worker+1)) {
								t.Errorf("page %d claimed twice", p)
								return
							}
						}
						held = append(held, m)
					}
					for len(held) > 0 {
						release(0)
					}
				})
			}
			wg.Wait()
			a.requireEmpty(t)
		})
	}
}

func TestAllocatorLifecycle(t *testing.T) {
	a := newAllocator(t, spanBytes)
	first, err := a.Acquire(context.Background(), 64<<10)
	require.NoError(t, err)
	second, err := a.TryAcquire(64 << 10)
	require.NoError(t, err)
	require.Zero(t, a.arena.words[0].Load()>>62, "both halves are occupied")
	first.Bytes()[0], second.Bytes()[0] = 1, 2
	require.True(t, first.TryInc())
	first.Unpin()
	require.Zero(t, a.arena.words[0].Load()>>62, "a remaining reference keeps A occupied")
	_, err = a.TryAcquire(4096)
	require.ErrorIs(t, err, ErrExhausted)
	first.Unpin()
	require.Equal(t, uint64(1), a.arena.words[0].Load()>>62, "only A became empty")
	next, err := a.TryAcquire(64 << 10)
	require.NoError(t, err)
	require.Zero(t, a.arena.words[0].Load()>>62, "reacquiring A clears its empty flag")
	require.NotEqual(t, first, next, "a new lease has a new generation")
	require.False(t, first.TryInc())
	require.Panics(t, func() { first.Unpin() })
	require.Equal(t, byte(2), second.Bytes()[0])
	next.Unpin()
	require.Equal(t, uint64(1), a.arena.words[0].Load()>>62)
	second.Unpin()
	a.requireEmpty(t)
	a.Close()
	a.Close()
	require.Zero(t, a.Capacity())
	require.Zero(t, a.Available())
}

// Separated free pages yield smaller blocks. Once all children are released,
// the entire span is immediately reusable.
func TestAllocatorCoalescesAfterRemoteRelease(t *testing.T) {
	a := newAllocator(t, spanBytes)
	memories := make([]Memory, spanPages)
	for i := range memories {
		m, err := a.TryAcquire(align.BlockSize)
		require.NoError(t, err)
		memories[i] = m
	}
	_, err := a.TryAcquire(align.BlockSize)
	require.ErrorIs(t, err, ErrExhausted)
	require.EqualValues(t, spanBytes, a.Capacity(), "exhaustion cannot grow the arena")
	for i := 0; i < len(memories); i += 2 {
		memories[i].Unpin()
	}
	partial, err := a.TryAcquire(2 * align.BlockSize)
	require.NoError(t, err)
	require.Len(t, partial.Bytes(), align.BlockSize)
	require.EqualValues(t, spanBytes/2-align.BlockSize, a.Available())
	partial.Unpin()
	require.EqualValues(t, spanBytes/2, a.Available())
	var wg sync.WaitGroup
	for i := 1; i < len(memories); i += 2 {
		wg.Go(func() { memories[i].Unpin() })
	}
	wg.Wait()
	m, err := a.TryAcquire(MaxAllocationSize)
	require.NoError(t, err)
	require.Zero(t, a.Available())
	for _, stale := range memories {
		require.False(t, stale.TryInc())
	}
	m.Unpin()
	a.requireEmpty(t)
}

// The arena is one span, so reacquiring it returns the same bytes.
func TestAllocatorWithoutZeroing(t *testing.T) {
	for _, noZero := range []bool{false, true} {
		var opts []Option
		if noZero {
			opts = append(opts, WithoutZeroing())
		}
		a := newAllocator(t, spanBytes, opts...)
		m, err := a.TryAcquire(spanBytes)
		require.NoError(t, err)
		m.Bytes()[0] = 0xa5
		m.Unpin()
		m, err = a.TryAcquire(spanBytes)
		require.NoError(t, err)
		want := byte(0)
		if noZero {
			want = 0xa5
		}
		require.Equal(t, want, m.Bytes()[0], "noZero=%v", noZero)
		m.Unpin()
	}
}

func TestAllocatorAvailability(t *testing.T) {
	a := newAllocator(t, 4*spanBytes)
	capacity := a.Capacity()
	require.Equal(t, capacity, a.Available())
	m, err := a.TryAcquire(5 * 4096)
	require.NoError(t, err)
	require.Equal(t, capacity-8*4096, a.Available(), "the entire 32 KiB buddy block is reserved")
	require.True(t, m.TryInc())
	require.False(t, m.Unpin())
	require.Equal(t, capacity-8*4096, a.Available())
	require.True(t, m.Unpin())
	require.Equal(t, capacity, a.Available())
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				m, err := a.TryAcquire(4096)
				if err != nil {
					t.Error(err)
					return
				}
				if available := a.Available(); available < 0 || available >= capacity || available%4096 != 0 {
					t.Errorf("invalid availability snapshot: %d", available)
				}
				m.Unpin()
			}
		})
	}
	wg.Wait()
	require.Equal(t, capacity, a.Available())
}

func TestAllocatorAcquireRetry(t *testing.T) {
	for _, size := range []int64{4096, spanBytes} {
		for _, interval := range []time.Duration{100 * time.Millisecond, time.Second} {
			t.Run(fmt.Sprintf("size%d/%s", size, interval), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var opts []Option
					if interval != 100*time.Millisecond {
						opts = append(opts, WithRetryInterval(interval))
					}
					a := newAllocator(t, spanBytes, opts...)
					start := time.Now()
					held := make([]Memory, spanBytes/size)
					var err error
					for i := range held {
						held[i], err = a.Acquire(context.Background(), size)
						require.NoError(t, err)
					}
					require.Equal(t, start, time.Now(), "first attempt must be immediate")
					type result struct {
						memory Memory
						err    error
					}
					results := make(chan result, 1)
					go func() {
						m, err := a.Acquire(context.Background(), size)
						results <- result{m, err}
					}()
					synctest.Wait()
					time.Sleep(interval) // The first retry still finds the arena full.
					synctest.Wait()
					require.Empty(t, results)
					time.Sleep(interval / 2)
					released := &held[len(held)-1].Bytes()[0]
					held[len(held)-1].Unpin()
					synctest.Wait()
					require.Empty(t, results, "release does not wake the retry timer")
					time.Sleep(interval / 2)
					synctest.Wait()
					require.Len(t, results, 1, "the second retry acquires the released memory")
					acquired := <-results
					require.NoError(t, acquired.err)
					require.Same(t, released, &acquired.memory.Bytes()[0])
					acquired.memory.Unpin()
					for _, m := range held[:len(held)-1] {
						m.Unpin()
					}
				})
			})
		}
	}
}

func TestAllocatorAcquireCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := newAllocator(t, spanBytes, WithRetryInterval(time.Second))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for range 2 {
			held, err := a.Acquire(ctx, 64<<10) // Free capacity wins without checking ctx.
			require.NoError(t, err)
			defer held.Unpin()
		}
		_, err := a.Acquire(ctx, 4096)
		require.ErrorIs(t, err, context.Canceled)

		start := time.Now()
		ctx, cancel = context.WithCancel(context.Background())
		var waitErr error
		go func() { _, waitErr = a.Acquire(ctx, 4096) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		require.ErrorIs(t, waitErr, context.Canceled)
		require.Equal(t, start, time.Now(), "cancellation must not wait for the retry timer")

		ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err = a.Acquire(ctx, 4096)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 100*time.Millisecond, time.Since(start))
	})
}
