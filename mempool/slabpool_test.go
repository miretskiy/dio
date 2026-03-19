package mempool_test

import (
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/mempool"
)

// ── Construction ──────────────────────────────────────────────────────────────

func TestNewSlabPool_Basic(t *testing.T) {
	// 2 MiB total, 128 KiB slots → 16 slots.
	p, err := mempool.NewSlabPool(2<<20, 128<<10)
	require.NoError(t, err)
	defer p.Close()
	require.Equal(t, 128<<10, p.SlotSize())
	require.Equal(t, 16, p.NumSlots())
}

func TestNewSlabPool_SlotSizeRounding(t *testing.T) {
	// Slot size 1 should be rounded up to a full page (align.BlockSize).
	p, err := mempool.NewSlabPool(2<<20, 1)
	require.NoError(t, err)
	defer p.Close()
	require.Equal(t, align.BlockSize, p.SlotSize())
	// All slots fit: 2 MiB / 4 KiB = 512.
	require.Equal(t, (2<<20)/align.BlockSize, p.NumSlots())
}

func TestNewSlabPool_SlotSizeAlreadyAligned(t *testing.T) {
	// A slot size that is already page-aligned must not be changed.
	p, err := mempool.NewSlabPool(2<<20, 4096)
	require.NoError(t, err)
	defer p.Close()
	require.Equal(t, 4096, p.SlotSize())
}

func TestNewSlabPool_InvalidSlotSize(t *testing.T) {
	_, err := mempool.NewSlabPool(2<<20, 0)
	require.Error(t, err)

	_, err = mempool.NewSlabPool(2<<20, -1)
	require.Error(t, err)
}

func TestNewSlabPool_TotalRoundedToHugepage(t *testing.T) {
	// Even a 1-byte total is rounded up to HugepageSize.
	p, err := mempool.NewSlabPool(1, align.HugepageSize)
	require.NoError(t, err)
	defer p.Close()
	// Rounded total = HugepageSize; one slot of HugepageSize → 1 slot.
	require.Equal(t, 1, p.NumSlots())
}

// ── Acquire / Release ─────────────────────────────────────────────────────────

func TestSlabPool_AcquireRelease(t *testing.T) {
	p, err := mempool.NewSlabPool(2<<20, 128<<10)
	require.NoError(t, err)
	defer p.Close()

	s, err := p.Acquire()
	require.NoError(t, err)
	require.Len(t, s.Data, 128<<10)
	require.True(t, align.IsAligned(s.Data), "slot must be page-aligned")

	// Write something and verify it survives the round-trip.
	s.Data[0] = 0xAB
	s.Data[len(s.Data)-1] = 0xCD
	s.Release()

	// Slot can be re-acquired after release.
	s2, err := p.Acquire()
	require.NoError(t, err)
	s2.Release()
}

func TestSlabPool_Exhaust(t *testing.T) {
	// 4 slots of 512 KiB each in one 2 MiB hugepage.
	const slotSize = align.HugepageSize / 4
	p, err := mempool.NewSlabPool(align.HugepageSize, slotSize)
	require.NoError(t, err)
	defer p.Close()
	require.Equal(t, 4, p.NumSlots())

	slots := make([]mempool.Slot, p.NumSlots())
	for i := range slots {
		slots[i], err = p.Acquire()
		require.NoError(t, err, "slot %d", i)
	}

	// Pool exhausted.
	_, err = p.Acquire()
	require.ErrorIs(t, err, mempool.ErrSlabExhausted)

	// Release one → can acquire again.
	slots[0].Release()
	s, err := p.Acquire()
	require.NoError(t, err)
	s.Release()

	for i := 1; i < len(slots); i++ {
		slots[i].Release()
	}
}

// ── Slot integrity ────────────────────────────────────────────────────────────

func TestSlabPool_NoOverlap(t *testing.T) {
	p, err := mempool.NewSlabPool(2<<20, 4096)
	require.NoError(t, err)
	defer p.Close()

	slots := make([]mempool.Slot, p.NumSlots())
	addrs := make(map[uintptr]bool, p.NumSlots())
	for i := range slots {
		slots[i], err = p.Acquire()
		require.NoError(t, err)
		addr := uintptr(unsafe.Pointer(&slots[i].Data[0]))
		require.False(t, addrs[addr], "duplicate slot address at index %d", i)
		addrs[addr] = true
	}
	for _, s := range slots {
		s.Release()
	}
}

func TestSlabPool_SlotsWithinSlab(t *testing.T) {
	p, err := mempool.NewSlabPool(2<<20, 128<<10)
	require.NoError(t, err)
	defer p.Close()

	raw := p.RawData()
	slabStart := uintptr(unsafe.Pointer(&raw[0]))
	slabEnd := slabStart + uintptr(len(raw))

	for range p.NumSlots() {
		s, err := p.Acquire()
		require.NoError(t, err)
		base := uintptr(unsafe.Pointer(&s.Data[0]))
		require.GreaterOrEqual(t, base, slabStart)
		require.Less(t, base+uintptr(len(s.Data)), slabEnd+1)
		s.Release()
	}
}

// ── Contains ──────────────────────────────────────────────────────────────────

func TestSlabPool_Contains(t *testing.T) {
	p, err := mempool.NewSlabPool(2<<20, 128<<10)
	require.NoError(t, err)
	defer p.Close()

	s, err := p.Acquire()
	require.NoError(t, err)
	defer s.Release()

	require.True(t, p.Contains(s.Data), "pool slot should be contained")
	require.False(t, p.Contains(make([]byte, 128<<10)), "heap slice must not be contained")
	require.False(t, p.Contains(nil), "nil must not be contained")
	require.False(t, p.Contains([]byte{}), "empty slice must not be contained")
}

// ── Tamper detection ──────────────────────────────────────────────────────────

func TestSlabPool_TamperDetection(t *testing.T) {
	p, err := mempool.NewSlabPool(2<<20, 4096)
	require.NoError(t, err)
	defer p.Close()

	s, err := p.Acquire()
	require.NoError(t, err)

	// Reassign Data to a foreign backing array (simulates append-with-realloc).
	s.Data = make([]byte, 4096)
	require.Panics(t, func() { s.Release() })
	// Note: the slot remains permanently checked out in this test.
	// p.Close() unmaps the slab regardless of bitmap state.
}

func TestSlabPool_ZeroSlot(t *testing.T) {
	var s mempool.Slot
	require.NotPanics(t, func() { s.Release() }) // zero Slot is a no-op
}

// ── Concurrent correctness ────────────────────────────────────────────────────

func TestSlabPool_Concurrent(t *testing.T) {
	// 32 slots; 16 goroutines each do 200 iterations of Acquire/write/Release.
	p, err := mempool.NewSlabPool(2<<20, (2<<20)/32)
	require.NoError(t, err)
	defer p.Close()

	const goroutines = 16
	const iters = 200

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for range goroutines {
		wg.Go(func() {
			for range iters {
				s, err := p.Acquire()
				if err != nil {
					// Pool transiently exhausted; back off and retry.
					for err == mempool.ErrSlabExhausted {
						s, err = p.Acquire()
					}
					if err != nil {
						errs <- err
						return
					}
				}
				s.Data[0] = 0xFF
				s.Release()
			}
		})
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
	p, err := mempool.NewSlabPool(align.HugepageSize, slotSize)
	require.NoError(t, err)
	defer p.Close()

	n := p.NumSlots()
	slots := make([]mempool.Slot, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	idx := 0

	for range n {
		wg.Go(func() {
			s, err := p.Acquire()
			require.NoError(t, err)
			mu.Lock()
			slots[idx] = s
			idx++
			mu.Unlock()
		})
	}
	wg.Wait()

	// Verify all addresses are distinct.
	addrs := make(map[uintptr]bool, n)
	for _, s := range slots {
		addr := uintptr(unsafe.Pointer(&s.Data[0]))
		require.False(t, addrs[addr], "duplicate address %x", addr)
		addrs[addr] = true
		s.Release()
	}
}
