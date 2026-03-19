package mempool

import (
	"errors"
	"fmt"
	"math/bits"
	"sync/atomic"
	"unsafe"

	"github.com/miretskiy/dio/align"
)

// Slab geometry constants.
const (
	// SlabBitsPerShard is the number of slots tracked per shard (one uint64 mask).
	SlabBitsPerShard = 64
	// SlabMaxShards caps the pool at 65 536 slots.
	SlabMaxShards = 1024
	// SlabMaxSlots is the maximum total slot count (SlabMaxShards × SlabBitsPerShard).
	SlabMaxSlots = SlabMaxShards * SlabBitsPerShard
)

// ErrSlabExhausted is returned by Acquire when all slots are in use.
var ErrSlabExhausted = errors.New("mempool: slab exhausted")

// slabShard holds a 64-bit occupancy mask for 64 consecutive slots, padded
// to a cache line to prevent false sharing between adjacent shards.
//
// Bit convention: 0 = free, 1 = allocated.
type slabShard struct {
	mask atomic.Uint64
	_    [56]byte // pad to 64-byte cache line (atomic.Uint64 is 8 bytes)
}

// Slot is a fixed-size view into a [SlabPool] slab.
//
// Data is the usable byte slice for the slot. The caller may read and write
// Data freely, but must not reassign it (e.g. via append that reallocates the
// backing array). [Slot.Release] validates the base pointer and panics if
// Data has been tampered with.
type Slot struct {
	Data   []byte    // usable slice; do not reassign the backing array
	rawPtr uintptr   // original &Data[0]; tamper-detection anchor (unexported)
	pool   *SlabPool // owning pool (unexported)
}

// Release returns this slot to its pool.
// Safe to call on a zero Slot (no-op).
// Panics if Slot.Data's base pointer has changed since Acquire (e.g. via
// append-with-reallocation or manual reassignment).
func (s Slot) Release() {
	if s.pool == nil {
		return // zero Slot
	}
	s.pool.Release(s)
}

// SlabPool is a contiguous, page-aligned slab allocator.
//
// A single anonymous mmap region is divided into fixed-size slots tracked by
// per-shard 64-bit atomic bitmasks. Acquire and Release are wait-free under
// low contention and spin briefly on the same shard under high contention.
//
// On Linux, if the rounded total size is a multiple of [align.HugepageSize]
// (2 MiB), [align.AllocAligned] will attempt MAP_HUGETLB transparently and
// fall back to standard 4 KiB pages if huge pages are unavailable.
//
// Create with [NewSlabPool]; close with [Close] after all Slots have been
// returned and any io_uring ring that registered this pool has been closed.
type SlabPool struct {
	rawData      []byte
	basePtr      uintptr
	slotSize     uint32
	numSlots     uint32
	activeShards uint32 // number of shards in use; precomputed at init
	shards       [SlabMaxShards]slabShard
}

// NewSlabPool creates a SlabPool covering at least totalSize bytes, divided
// into slots of at least slotSize bytes.
//
// slotSize is rounded up to the nearest [align.BlockSize] (4 KiB) page
// boundary automatically. totalSize is rounded up to the nearest
// [align.HugepageSize] (2 MiB) boundary.
//
// Returns an error if slotSize ≤ 0, if the pool would require more than
// [SlabMaxSlots] slots, or if the total size is too small for even one slot.
func NewSlabPool(totalSize, slotSize int) (*SlabPool, error) {
	if slotSize <= 0 {
		return nil, errors.New("mempool: slot size must be positive")
	}
	// Round slot size up to page boundary.
	alignedSlot := int(align.PageAlign(int64(slotSize)))

	// Round total size up to hugepage boundary for hugepage eligibility.
	roundedTotal := (totalSize + align.HugepageSize - 1) &^ (align.HugepageSize - 1)

	slots := roundedTotal / alignedSlot
	if slots == 0 {
		return nil, fmt.Errorf("mempool: total size %d too small for slot size %d", totalSize, slotSize)
	}
	if slots > SlabMaxSlots {
		return nil, fmt.Errorf("mempool: too many slots: %d requested, max %d", slots, SlabMaxSlots)
	}

	data := align.AllocAligned(roundedTotal)
	p := &SlabPool{
		rawData:      data,
		basePtr:      uintptr(unsafe.Pointer(&data[0])),
		slotSize:     uint32(alignedSlot),
		numSlots:     uint32(slots),
		activeShards: uint32((slots + SlabBitsPerShard - 1) / SlabBitsPerShard),
	}
	p.initMask(slots)
	return p, nil
}

func (p *SlabPool) initMask(slots int) {
	// Start everything busy.
	for i := range p.shards {
		p.shards[i].mask.Store(^uint64(0))
	}
	fullShards := slots / SlabBitsPerShard
	remaining := slots % SlabBitsPerShard
	for i := range fullShards {
		p.shards[i].mask.Store(0) // all 64 slots free
	}
	if remaining > 0 {
		// Bits [0, remaining) free; bits [remaining, 63] busy.
		p.shards[fullShards].mask.Store(^uint64(0) << uint(remaining))
	}
}

// Acquire returns a free [Slot]. Returns [ErrSlabExhausted] if all slots are
// in use. Non-blocking; the caller is expected to retry or back off.
func (p *SlabPool) Acquire() (Slot, error) {
	for i := range p.activeShards {
		for {
			mask := p.shards[i].mask.Load()
			if mask == ^uint64(0) {
				break // shard full, try next
			}
			freePos := bits.TrailingZeros64(^mask) // index of first 0-bit
			if p.shards[i].mask.CompareAndSwap(mask, mask|(uint64(1)<<freePos)) {
				slot := int(i)*SlabBitsPerShard + freePos
				start := slot * int(p.slotSize)
				data := p.rawData[start : start+int(p.slotSize)]
				return Slot{
					Data:   data,
					rawPtr: uintptr(unsafe.Pointer(&data[0])),
					pool:   p,
				}, nil
			}
			// CAS lost to a concurrent Acquire; reload and retry this shard.
		}
	}
	return Slot{}, ErrSlabExhausted
}

// Release returns s to the pool.
// Panics if s.Data's base pointer has changed since Acquire.
// Safe to call on a zero Slot (no-op).
func (p *SlabPool) Release(s Slot) {
	if s.rawPtr == 0 {
		return // zero Slot
	}
	if len(s.Data) == 0 || uintptr(unsafe.Pointer(&s.Data[0])) != s.rawPtr {
		panic("mempool: SlabPool.Release: Slot.Data base pointer has been tampered")
	}
	offset := s.rawPtr - p.basePtr
	slot := int(offset / uintptr(p.slotSize))
	shardIdx := slot / SlabBitsPerShard
	bit := uint(slot % SlabBitsPerShard)
	p.shards[shardIdx].mask.And(^(uint64(1) << bit)) // atomically clear bit
}

// Contains reports whether buf's base pointer lies within this slab.
// Useful for validating that a buffer was obtained from this pool.
func (p *SlabPool) Contains(buf []byte) bool {
	if len(buf) == 0 {
		return false
	}
	base := uintptr(unsafe.Pointer(&buf[0]))
	return base >= p.basePtr && base < p.basePtr+uintptr(len(p.rawData))
}

// SlotSize returns the (page-aligned) size of each slot in bytes.
func (p *SlabPool) SlotSize() int { return int(p.slotSize) }

// NumSlots returns the total number of slots.
func (p *SlabPool) NumSlots() int { return int(p.numSlots) }

// RawData returns the underlying contiguous slab.
// The caller must not modify the returned slice.
// Intended for registering the buffer with io_uring (io_uring_register_buffers).
func (p *SlabPool) RawData() []byte { return p.rawData }

// Close unmaps the slab. Must be called only after all Slots have been
// released and after any io_uring ring that registered this pool has exited.
func (p *SlabPool) Close() {
	align.FreeAligned(p.rawData)
}
