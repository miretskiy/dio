package alloc_test

import (
	"math"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
)

func TestNewSlabPool_Basic(t *testing.T) {
	// 2 MiB total, 128 KiB slots → 16 slots.
	p, err := alloc.NewSlabPool(2<<20, 128<<10)
	require.NoError(t, err)
	defer p.Close()
	require.Equal(t, 128<<10, p.SlotSize())
	require.Equal(t, 16, p.NumSlots())
}

func TestNewSlabPool_SlotSizeRounding(t *testing.T) {
	// Slot size 1 should be rounded up to a full page (align.BlockSize).
	p, err := alloc.NewSlabPool(2<<20, 1)
	require.NoError(t, err)
	defer p.Close()
	require.Equal(t, align.BlockSize, p.SlotSize())
	// All slots fit: 2 MiB / 4 KiB = 512.
	require.Equal(t, (2<<20)/align.BlockSize, p.NumSlots())
}

func TestNewSlabPool_SlotSizeAlreadyAligned(t *testing.T) {
	// A slot size that is already page-aligned must not be changed.
	p, err := alloc.NewSlabPool(2<<20, 4096)
	require.NoError(t, err)
	defer p.Close()
	require.Equal(t, 4096, p.SlotSize())
}

func TestNewSlabPool_InvalidSlotSize(t *testing.T) {
	_, err := alloc.NewSlabPool(2<<20, 0)
	require.Error(t, err)

	_, err = alloc.NewSlabPool(2<<20, -1)
	require.Error(t, err)
}

func TestSlabPoolRejectsInvalidDimensionsAndAllocations(t *testing.T) {
	for _, size := range []int{-1, 0, math.MaxInt} {
		_, err := alloc.NewSlabPool(size, 4096)
		require.Error(t, err)
		_, err = alloc.NewSlabPool(4096, size)
		require.Error(t, err)
	}
	data := alloc.NewBuffer(4096)
	defer data.Unpin()
	_, err := alloc.NewSlabPoolFrom(data, math.MaxInt)
	require.Error(t, err)
	p, err := alloc.NewSlabPoolFrom(data, 4096)
	require.NoError(t, err)
	defer p.Close()
	mem, err := p.TryAcquire()
	require.NoError(t, err)
	_, err = p.TryAcquire()
	require.ErrorIs(t, err, alloc.ErrExhausted)
	mem.Unpin()
}

func TestNewSlabPool_TotalRoundedToHugepage(t *testing.T) {
	// Even a 1-byte total is rounded up to HugepageSize.
	p, err := alloc.NewSlabPool(1, align.HugepageSize)
	require.NoError(t, err)
	defer p.Close()
	// Rounded total = HugepageSize; one slot of HugepageSize → 1 slot.
	require.Equal(t, 1, p.NumSlots())
}

func TestSlabPool_AcquireRelease(t *testing.T) {
	p, err := alloc.NewSlabPool(2<<20, 128<<10)
	require.NoError(t, err)
	defer p.Close()

	s, err := p.TryAcquire()
	require.NoError(t, err)
	require.Len(t, s.Bytes(), 128<<10)
	require.True(t, align.IsAligned(s.Bytes()), "slot must be page-aligned")

	// Write something and verify it survives the round-trip.
	s.Bytes()[0] = 0xAB
	s.Bytes()[len(s.Bytes())-1] = 0xCD
	s.Unpin()

	// Slot can be re-acquired after release.
	s2, err := p.TryAcquire()
	require.NoError(t, err)
	s2.Unpin()
}

func TestSlabPool_Exhaust(t *testing.T) {
	// 4 slots of 512 KiB each in one 2 MiB hugepage.
	const slotSize = align.HugepageSize / 4
	p, err := alloc.NewSlabPool(align.HugepageSize, slotSize)
	require.NoError(t, err)
	defer p.Close()
	require.Equal(t, 4, p.NumSlots())

	slots := make([]*alloc.Buffer, p.NumSlots())
	for i := range slots {
		slots[i], err = p.TryAcquire()
		require.NoError(t, err, "slot %d", i)
	}

	// Pool exhausted.
	_, err = p.TryAcquire()
	require.ErrorIs(t, err, alloc.ErrExhausted)

	// Release one → can acquire again.
	slots[0].Unpin()
	s, err := p.TryAcquire()
	require.NoError(t, err)
	s.Unpin()

	for i := 1; i < len(slots); i++ {
		slots[i].Unpin()
	}
}

func TestSlabPool_NoOverlap(t *testing.T) {
	p, err := alloc.NewSlabPool(2<<20, 4096)
	require.NoError(t, err)
	defer p.Close()

	slots := make([]*alloc.Buffer, p.NumSlots())
	addrs := make(map[uintptr]bool, p.NumSlots())
	for i := range slots {
		slots[i], err = p.TryAcquire()
		require.NoError(t, err)
		addr := uintptr(unsafe.Pointer(&slots[i].Bytes()[0]))
		require.False(t, addrs[addr], "duplicate slot address at index %d", i)
		addrs[addr] = true
	}
	for _, s := range slots {
		s.Unpin()
	}
}

func TestSlabPool_SlotsWithinSlab(t *testing.T) {
	p, err := alloc.NewSlabPool(2<<20, 128<<10)
	require.NoError(t, err)
	defer p.Close()

	raw := p.RawData()
	slabStart := uintptr(unsafe.Pointer(&raw[0]))
	slabEnd := slabStart + uintptr(len(raw))

	for range p.NumSlots() {
		s, err := p.TryAcquire()
		require.NoError(t, err)
		base := uintptr(unsafe.Pointer(&s.Bytes()[0]))
		require.GreaterOrEqual(t, base, slabStart)
		require.Less(t, base+uintptr(len(s.Bytes())), slabEnd+1)
		s.Unpin()
	}
}

func TestSlabPool_Concurrent(t *testing.T) {
	// 32 slots; 16 goroutines each do 200 iterations of Acquire/write/Release.
	p, err := alloc.NewSlabPool(2<<20, (2<<20)/32)
	require.NoError(t, err)
	defer p.Close()

	const goroutines = 16
	const iters = 200

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iters {
				s, err := p.TryAcquire()
				if err != nil {
					// Pool transiently exhausted; back off and retry.
					for err == alloc.ErrExhausted {
						s, err = p.TryAcquire()
					}
					if err != nil {
						errs <- err
						return
					}
				}
				s.Bytes()[0] = 0xFF
				s.Unpin()
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestSlabPool_ConcurrentNoDoubleAlloc(t *testing.T) {
	// Acquire all slots simultaneously from multiple goroutines; verify each
	// address is handed out exactly once.
	const slotSize = align.HugepageSize / 8
	p, err := alloc.NewSlabPool(align.HugepageSize, slotSize)
	require.NoError(t, err)
	defer p.Close()

	n := p.NumSlots()
	slots := make([]*alloc.Buffer, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	idx := 0

	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := p.TryAcquire()
			require.NoError(t, err)
			mu.Lock()
			slots[idx] = s
			idx++
			mu.Unlock()
		}()
	}
	wg.Wait()

	// Verify all addresses are distinct.
	addrs := make(map[uintptr]bool, n)
	for _, s := range slots {
		addr := uintptr(unsafe.Pointer(&s.Bytes()[0]))
		require.False(t, addrs[addr], "duplicate address %x", addr)
		addrs[addr] = true
		s.Unpin()
	}
}

func TestSlabPoolSharedBacking(t *testing.T) {
	memory := alloc.NewBuffer(3 * 4096)
	defer memory.Unpin()
	p, err := alloc.NewSlabPoolFrom(memory, 4096)
	require.NoError(t, err)
	require.Equal(t, 3, p.NumSlots())
	var slots []*alloc.Buffer
	for range 3 {
		slot, err := p.TryAcquire()
		require.NoError(t, err)
		require.Equal(t, 4096, cap(slot.Bytes()), "a slice cannot grow into its neighbor")
		slot.Bytes()[0] = 7
		slots = append(slots, slot)
	}
	_, err = p.TryAcquire()
	require.ErrorIs(t, err, alloc.ErrExhausted)
	for _, slot := range slots {
		slot.Unpin()
	}
	p.Close()
	// Closing the pool leaves the caller's backing reference intact.
	require.Equal(t, byte(7), memory.Bytes()[0])
	memory.Bytes()[0] = 9
	_, err = alloc.NewSlabPoolFrom(nil, 4096)
	require.Error(t, err)
	_, err = alloc.NewSlabPoolFrom(memory, 16<<10)
	require.Error(t, err)
}

func TestSlabPoolLastReferenceAndStaleBuffer(t *testing.T) {
	data := alloc.NewBuffer(4096)
	defer data.Unpin()
	p, err := alloc.NewSlabPoolFrom(data, 4096)
	require.NoError(t, err)
	b, err := p.TryAcquire()
	require.NoError(t, err)
	require.True(t, b.TryInc())
	b.Unpin()
	_, err = p.TryAcquire()
	require.ErrorIs(t, err, alloc.ErrExhausted)
	b.Unpin()
	next, err := p.TryAcquire()
	require.NoError(t, err)
	require.NotSame(t, b, next)
	require.False(t, b.TryInc())
	require.Panics(t, b.Unpin)
	_, err = p.TryAcquire()
	require.ErrorIs(t, err, alloc.ErrExhausted)
	next.Unpin()
	p.Close()
	_, err = p.TryAcquire()
	require.ErrorIs(t, err, alloc.ErrClosed)
}

func TestSlabPoolRetainsPooledBacking(t *testing.T) {
	pool, err := alloc.NewMmapPool(8192, 1)
	require.NoError(t, err)
	defer pool.Close()
	backing, err := pool.TryAcquire()
	require.NoError(t, err)
	slab, err := alloc.NewSlabPoolFrom(backing, 4096)
	require.NoError(t, err)
	backing.Unpin()
	_, err = pool.TryAcquire()
	require.ErrorIs(t, err, alloc.ErrExhausted)
	slot, err := slab.TryAcquire()
	require.NoError(t, err)
	slot.Bytes()[0] = 42
	slot.Unpin()
	slab.Close()
	require.False(t, backing.TryInc(), "closing the slab releases its backing reference")
	next, err := pool.TryAcquire()
	require.NoError(t, err)
	require.Equal(t, byte(42), next.Bytes()[0])
	next.Unpin()
}

func TestSlabPoolFromRejectsWithoutRetaining(t *testing.T) {
	backing := alloc.NewBuffer(4096)
	_, err := alloc.NewSlabPoolFrom(backing, 8192)
	require.Error(t, err)
	backing.Unpin()
	require.False(t, backing.TryInc(), "failed construction must not retain a reference")
	_, err = alloc.NewSlabPoolFrom(backing, 4096)
	require.Error(t, err, "released backing cannot be revived")
}
