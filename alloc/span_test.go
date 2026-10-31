package alloc

import (
	"context"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
)

func newSpanAllocator(t testing.TB, spans int, opts ...Option) *SpanAllocator {
	t.Helper()
	a, err := NewSpanAllocator(int64(spans)*spanBytes, opts...)
	require.NoError(t, err)
	t.Cleanup(a.Close)
	return a
}

// pageOf returns the page index of m within a's backing memory.
func (a *SpanAllocator) pageOf(m Memory) int {
	return int(uintptr(unsafe.Pointer(&m.Bytes()[0]))-uintptr(unsafe.Pointer(&a.region[0]))) / align.BlockSize
}

func TestSpanClassLayout(t *testing.T) {
	for c, pages := range spanClassPages {
		cl := newSpanAllocator(t, 1).classes[c]
		slots := min(tagShift, spanPages/pages)
		require.Equal(t, uint64(c), cl.empty>>tagShift)
		require.Equal(t, slots, 64-4-bits.OnesCount64(cl.empty&slotMask), "class %d", c)
		require.LessOrEqual(t, slots*pages, spanPages)
	}
	for p := 1; p <= maxSmallPages; p++ {
		c := pagesToClass[p]
		require.GreaterOrEqual(t, spanClassPages[c], p)
		if c > 0 {
			require.Less(t, spanClassPages[c-1], p)
		}
	}
}

func TestSpanAllocatorSizes(t *testing.T) {
	a := newSpanAllocator(t, 128)
	for _, test := range []struct{ size, pages int }{
		{1, 1}, {4096, 1}, {4097, 2}, {3 * 4096, 3}, {5 * 4096, 5}, {7 * 4096, 7},
		{11 * 4096, 12}, {13 * 4096, 16}, {17 * 4096, 21}, {22 * 4096, 32},
		{32 * 4096, 32}, {33 * 4096, 64}, {64 * 4096, 64},
		{65 * 4096, 128}, {64 * 64 * 4096, 64 * 64},
	} {
		t.Run(fmt.Sprint(test.size), func(t *testing.T) {
			m, err := a.TryAcquire(int64(test.size))
			require.NoError(t, err)
			data := m.Bytes()
			require.Len(t, data, test.pages*align.BlockSize)
			require.True(t, align.IsAligned(data))
			require.Equal(t, make([]byte, len(data)), data)
			for i := range data {
				data[i] = 0xa5
			}
			m.Unpin()
		})
	}
	require.EqualValues(t, 64*spanBytes, a.MaxSize())
	_, err := a.TryAcquire(64*spanBytes + 1)
	require.ErrorIs(t, err, ErrTooLarge)
	require.Equal(t, a.Capacity(), a.Available())
}

// A request larger than the allocator fails at once instead of waiting.
func TestSpanAllocatorMaxSize(t *testing.T) {
	a := newSpanAllocator(t, 3)
	require.EqualValues(t, 3*spanBytes, a.MaxSize())
	m, err := a.TryAcquire(a.MaxSize())
	require.NoError(t, err)
	m.Unpin()
	_, err = a.TryAcquire(a.MaxSize() + 1)
	require.ErrorIs(t, err, ErrTooLarge)
	_, err = a.Acquire(context.Background(), a.MaxSize()+1)
	require.ErrorIs(t, err, ErrTooLarge)
}

// Runs cross inUse words: free spans 32..95 fill neither word alone.
func TestSpanAllocatorRunAcrossWords(t *testing.T) {
	a := newSpanAllocator(t, 128)
	for i := range a.largeHints {
		a.largeHints[i].word.Store(1)
	}
	quarters := make([]Memory, 4) // spans 96, 64, 32, 0 in claim order
	for i := range quarters {
		m, err := a.TryAcquire(32 * spanBytes)
		require.NoError(t, err)
		require.Equal(t, (3-i)*32, a.pageOf(m)/spanPages)
		quarters[i] = m
	}
	quarters[1].Unpin()
	quarters[2].Unpin()
	run, err := a.TryAcquire(64 * spanBytes)
	require.NoError(t, err)
	require.Equal(t, 32, a.pageOf(run)/spanPages)
	_, err = a.TryAcquire(1)
	require.ErrorIs(t, err, ErrExhausted)
	run.Unpin()

	// A shorter run takes as much as fits from the upper word.
	run, err = a.TryAcquire(48 * spanBytes)
	require.NoError(t, err)
	require.Equal(t, 48, a.pageOf(run)/spanPages)
	run.Unpin()
	quarters[0].Unpin()
	quarters[3].Unpin()
	require.Equal(t, a.Capacity(), a.Available())
	for range 2 {
		m, err := a.TryAcquire(64 * spanBytes)
		require.NoError(t, err)
		defer m.Unpin()
	}
}

// Concurrent large runs over a region whose words they must share and cross,
// including the partial last word. Span ownership is tracked independently.
func TestSpanAllocatorConcurrentRuns(t *testing.T) {
	const spans = 200
	a := newSpanAllocator(t, spans)
	owner := make([]atomic.Int32, spans)
	var wg sync.WaitGroup
	for worker := 1; worker <= 16; worker++ {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(worker), 11))
			for range 2000 {
				k := 2 + r.IntN(maxLargeSpans-1)
				m, err := a.TryAcquire(int64(k) * spanBytes)
				if err == ErrExhausted {
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				s := a.pageOf(m) / spanPages
				for j := s; j < s+k; j++ {
					if !owner[j].CompareAndSwap(0, int32(worker)) {
						t.Errorf("span %d already owned by worker %d", j, owner[j].Load())
					}
				}
				runtime.Gosched()
				for j := s; j < s+k; j++ {
					owner[j].Store(0)
				}
				m.Unpin()
			}
		})
	}
	wg.Wait()
	require.Equal(t, a.Capacity(), a.Available())
	for range spans / 64 {
		m, err := a.TryAcquire(64 * spanBytes)
		require.NoError(t, err)
		defer m.Unpin()
	}
}

// Filling the region with one class, then freeing it, must leave every span
// reusable by a large run: empty spans retire or are reclaimed from lanes.
func TestSpanAllocatorExhaustAndReclaim(t *testing.T) {
	const spans = 4
	a := newSpanAllocator(t, spans)
	var held []Memory
	for {
		m, err := a.TryAcquire(4096)
		if err != nil {
			require.ErrorIs(t, err, ErrExhausted)
			break
		}
		held = append(held, m)
	}
	require.Len(t, held, spans*tagShift)
	_, err := a.TryAcquire(spanBytes)
	require.ErrorIs(t, err, ErrExhausted)
	for _, m := range held {
		m.Unpin()
	}
	require.Equal(t, a.Capacity(), a.Available())
	all, err := a.TryAcquire(spans * spanBytes)
	require.NoError(t, err, "idle class spans must be reclaimed for a large run")
	require.Zero(t, a.Available())
	_, err = a.TryAcquire(1)
	require.ErrorIs(t, err, ErrExhausted)
	all.Unpin()
	require.Equal(t, a.Capacity(), a.Available())
}

func TestSpanAllocatorPlacement(t *testing.T) {
	a := newSpanAllocator(t, 8)
	small, err := a.TryAcquire(4096)
	require.NoError(t, err)
	defer small.Unpin()
	medium, err := a.TryAcquire(8 * 4096)
	require.NoError(t, err)
	defer medium.Unpin()
	large, err := a.TryAcquire(spanBytes + 1)
	require.NoError(t, err)
	defer large.Unpin()
	require.Equal(t, 0, a.pageOf(small)/spanPages, "class spans pack from the bottom")
	require.Equal(t, 1, a.pageOf(medium)/spanPages)
	require.Equal(t, 6, a.pageOf(large)/spanPages, "large runs pack from the top")
	again, err := a.TryAcquire(4096)
	require.NoError(t, err)
	defer again.Unpin()
	require.Equal(t, 0, a.pageOf(again)/spanPages, "a partial span is reused")
}

func TestSpanAllocatorAcquireCancel(t *testing.T) {
	a := newSpanAllocator(t, 1)
	m, err := a.TryAcquire(spanBytes)
	require.NoError(t, err)
	defer m.Unpin()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = a.Acquire(ctx, 1)
	require.ErrorIs(t, err, context.Canceled)
}

var spanTestSizes = []int{1, 2, 3, 4, 5, 6, 8, 12, 16, 24, 32, 48, 64, 100, 128}

// Concurrent mixed sizes. Page ownership is tracked independently to detect
// overlapping allocations, and contents are checked after yielding.
func TestSpanAllocatorConcurrent(t *testing.T) {
	const spans = 64
	a := newSpanAllocator(t, spans)
	owner := make([]atomic.Int32, spans*spanPages)
	var wg sync.WaitGroup
	for worker := 1; worker <= 32; worker++ {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(worker), 7))
			var held []Memory
			for i := range 3000 {
				if len(held) > 0 && (len(held) > 8 || r.IntN(2) == 0) {
					j := r.IntN(len(held))
					m := held[j]
					held[j] = held[len(held)-1]
					held = held[:len(held)-1]
					data := m.Bytes()
					if data[0] != byte(worker) || data[len(data)-1] != byte(worker) {
						t.Error("memory overwritten by another borrower")
					}
					p := a.pageOf(m)
					for k := range len(data) / align.BlockSize {
						owner[p+k].Store(0)
					}
					m.Unpin()
					continue
				}
				m, err := a.TryAcquire(int64(spanTestSizes[r.IntN(len(spanTestSizes))]) * align.BlockSize)
				if err == ErrExhausted {
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				data := m.Bytes()
				p := a.pageOf(m)
				for k := range len(data) / align.BlockSize {
					if !owner[p+k].CompareAndSwap(0, int32(worker)) {
						t.Errorf("page %d already owned by worker %d", p+k, owner[p+k].Load())
					}
				}
				data[0], data[len(data)-1] = byte(worker), byte(worker)
				held = append(held, m)
				if i%16 == 0 {
					runtime.Gosched()
				}
			}
			for _, m := range held {
				p := a.pageOf(m)
				for k := range len(m.Bytes()) / align.BlockSize {
					owner[p+k].Store(0)
				}
				m.Unpin()
			}
		})
	}
	wg.Wait()
	require.Equal(t, a.Capacity(), a.Available())
	all, err := a.TryAcquire(spans * spanBytes)
	require.NoError(t, err, "every span must be reusable once all memory is returned")
	all.Unpin()
}

func TestSpanAllocatorBacking(t *testing.T) {
	backing := NewMemory(3 * spanBytes)
	defer backing.Unpin()
	a, err := NewSpanAllocator(0, WithBacking(backing.Bytes()))
	require.NoError(t, err)
	require.EqualValues(t, 3*spanBytes, a.Capacity())
	m, err := a.TryAcquire(spanBytes)
	require.NoError(t, err)
	m.Bytes()[0] = 1
	m.Unpin()
	a.Close()

	_, err = NewSpanAllocator(0, WithBacking(backing.Bytes()[:spanBytes-4096]))
	require.ErrorContains(t, err, "at least 256 KiB")
	_, err = NewSpanAllocator(0, WithBacking(backing.Bytes()[1:]))
	require.ErrorContains(t, err, "page-aligned")
}
