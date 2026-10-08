package mempool_test

import (
	"sync"
	"testing"
	"unsafe"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/mempool"
	"github.com/stretchr/testify/require"
)

func TestLazyMmapPoolCapacityReuseAndTrim(t *testing.T) {
	p := mempool.NewLazyMmapPool("lazy", 4096, 2)
	defer p.Close()
	require.Zero(t, p.Allocated())
	a, ok := p.TryAcquire()
	require.True(t, ok)
	require.Equal(t, 1, p.Allocated())
	a.Unpin()
	a = p.Acquire()
	require.Equal(t, 2, p.Allocated(), "nil capacity ahead of returned buffers grows the pool")
	b := p.Acquire()
	require.Equal(t, 2, p.Allocated())
	_, ok = p.TryAcquire()
	require.False(t, ok)
	ptr := unsafe.SliceData(a.Bytes())
	a.Unpin()
	replacement, ok := p.TryAcquire()
	require.True(t, ok)
	require.Equal(t, ptr, unsafe.SliceData(replacement.Bytes()))
	require.NotSame(t, a, replacement)
	require.False(t, a.TryInc())
	replacement.Unpin()
	p.Trim()
	require.Equal(t, 1, p.Allocated(), "checked-out buffers survive trimming")
	require.True(t, b.TryInc())
	b.Unpin()
	b.Unpin()
	p.Trim()
	require.Zero(t, p.Allocated())
	c := p.Acquire()
	require.Equal(t, 1, p.Allocated(), "trimmed capacity can grow again")
	c.Unpin()
}

func TestLazyMmapPoolConcurrentGrowthAndTrim(t *testing.T) {
	const capacity = 4
	p := mempool.NewLazyMmapPool("concurrent", 4096, capacity)
	defer p.Close()
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			for range 100 {
				b := p.Acquire()
				b.Bytes()[0] = 1
				b.Unpin()
			}
		})
	}
	wg.Go(func() {
		for range 100 {
			p.Trim()
		}
	})
	wg.Wait()
	require.LessOrEqual(t, p.Allocated(), capacity)
	require.Zero(t, p.Outstanding())
	var held []*mempool.MmapBuffer
	for range capacity {
		held = append(held, p.Acquire())
	}
	_, ok := p.TryAcquire()
	require.False(t, ok)
	for _, b := range held {
		b.Unpin()
	}
}

func TestSlabPoolBorrowedMemory(t *testing.T) {
	memory := align.AllocAligned(3 * 4096)
	defer align.FreeAligned(memory)
	p, err := mempool.NewSlabPoolFrom(memory, 4096)
	require.NoError(t, err)
	require.Equal(t, 3, p.NumSlots())
	var slots []mempool.Slot
	for range 3 {
		slot, err := p.Acquire()
		require.NoError(t, err)
		require.Equal(t, 4096, cap(slot.Data), "a slice cannot grow into its neighbor")
		slot.Data[0] = 7
		slots = append(slots, slot)
	}
	_, err = p.Acquire()
	require.ErrorIs(t, err, mempool.ErrSlabExhausted)
	for _, slot := range slots {
		slot.Release()
	}
	p.Close()
	// Closing the borrowed view must not unmap its owner's memory.
	require.Equal(t, byte(7), memory[0])
	memory[0] = 9
	_, err = mempool.NewSlabPoolFrom(memory[1:], 4096)
	require.Error(t, err)
	_, err = mempool.NewSlabPoolFrom(memory, 16<<10)
	require.Error(t, err)
}
