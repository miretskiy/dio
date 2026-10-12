package alloc

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"sync/atomic"

	"github.com/miretskiy/dio/v2/align"
)

const (
	slabBitsPerShard = 64
	slabMaxSlots     = 65536
)

// slabShard holds a 64-bit occupancy mask for 64 consecutive slots, padded
// to a cache line to prevent false sharing between adjacent shards.
//
// Bit convention: 0 = free, 1 = allocated.
type slabShard struct {
	mask atomic.Uint64
	_    [56]byte // pad to 64-byte cache line (atomic.Uint64 is 8 bytes)
}

// SlabPool is a contiguous, page-aligned slab allocator.
//
// A single anonymous mmap region is divided into fixed-size slots tracked by
// per-shard 64-bit atomic bitmasks. Acquisition claims a free bit; the returned
// buffer clears it on its last Unpin. Each acquisition uses a fresh buffer,
// so stale references cannot release or pin a later borrower of the same slot.
//
// On Linux, if the rounded total size is a multiple of [align.HugepageSize]
// (2 MiB), [align.AllocAligned] will attempt MAP_HUGETLB transparently and
// fall back to standard 4 KiB pages if huge pages are unavailable.
//
// Create with [NewSlabPool]; close with [SlabPool.Close] after all buffers have been
// returned and any io_uring ring or scheduler that registered this pool has
// been closed. Registration retains the pool but does not transfer ownership.
type SlabPool struct {
	backing  *Buffer
	slotSize uint32
	numSlots uint32
	shards   []slabShard
}

// NewSlabPool creates a SlabPool covering at least totalSize bytes, divided
// into slots of at least slotSize bytes.
//
// slotSize is rounded up to the nearest [align.BlockSize] (4 KiB) page
// boundary automatically. totalSize is rounded up to the nearest
// [align.HugepageSize] (2 MiB) boundary.
//
// Invalid dimensions return errors. Mapping failures panic.
func NewSlabPool(totalSize, slotSize int) (*SlabPool, error) {
	if totalSize <= 0 || totalSize > math.MaxInt-(align.HugepageSize-1) {
		return nil, errors.New("alloc: invalid slab size")
	}
	totalSize = (totalSize + align.HugepageSize - 1) &^ (align.HugepageSize - 1)
	alignedSlot, slots, err := slabLayout(totalSize, slotSize)
	if err != nil {
		return nil, err
	}
	return newSlabPool(NewBuffer(int64(totalSize)), alignedSlot, slots), nil
}

// NewSlabPoolFrom divides backing into fixed-size slots. On success it retains
// its own reference to backing until Close; the caller keeps its reference.
// Slot size is rounded up to a page; any trailing partial slot is unused.
func NewSlabPoolFrom(backing *Buffer, slotSize int) (*SlabPool, error) {
	if backing == nil {
		return nil, errors.New("alloc: nil backing buffer")
	}
	alignedSlot, slots, err := slabLayout(backing.Cap(), slotSize)
	if err != nil {
		return nil, err
	}
	if !backing.TryInc() {
		return nil, errors.New("alloc: backing buffer cannot be pinned")
	}
	return newSlabPool(backing, alignedSlot, slots), nil
}

func slabLayout(totalSize, slotSize int) (alignedSlot, slots int, err error) {
	if slotSize <= 0 || int64(slotSize) > min(int64(math.MaxInt), int64(math.MaxUint32))-align.BlockMask {
		return 0, 0, errors.New("alloc: invalid slot size")
	}
	alignedSlot = int(align.PageAlign(int64(slotSize)))
	slots = totalSize / alignedSlot
	if slots == 0 || slots > slabMaxSlots {
		return 0, 0, fmt.Errorf("alloc: slab must contain 1..%d slots, got %d", slabMaxSlots, slots)
	}
	return alignedSlot, slots, nil
}

// newSlabPool takes ownership of one backing reference.
func newSlabPool(backing *Buffer, slotSize, slots int) *SlabPool {
	p := &SlabPool{
		backing:  backing,
		slotSize: uint32(slotSize),
		numSlots: uint32(slots),
		shards:   make([]slabShard, (slots+slabBitsPerShard-1)/slabBitsPerShard),
	}
	p.initMask(slots)
	return p
}

func (p *SlabPool) initMask(slots int) {
	// Start everything busy.
	for i := range p.shards {
		p.shards[i].mask.Store(^uint64(0))
	}
	fullShards := slots / slabBitsPerShard
	remaining := slots % slabBitsPerShard
	for i := range fullShards {
		p.shards[i].mask.Store(0) // all 64 slots free
	}
	if remaining > 0 {
		// Bits [0, remaining) free; bits [remaining, 63] busy.
		p.shards[fullShards].mask.Store(^uint64(0) << uint(remaining))
	}
}

// TryAcquire returns one slot as a buffer with one reference. It never waits
// for borrowers: it returns ErrExhausted if all slots are held and ErrClosed
// after Close. Return the buffer with Unpin.
//
// Starts at a random shard to distribute load across shards under concurrent
// use, then probes linearly (with wrap-around) until a free slot is found.
func (p *SlabPool) TryAcquire() (*Buffer, error) {
	if p.backing == nil {
		return nil, ErrClosed
	}
	n := len(p.shards)
	start := rand.IntN(n)
	for j := range n {
		i := (start + j) % n
		for {
			mask := p.shards[i].mask.Load()
			if mask == ^uint64(0) {
				break // shard full, try next
			}
			freePos := bits.TrailingZeros64(^mask) // index of first 0-bit
			if p.shards[i].mask.CompareAndSwap(mask, mask|(uint64(1)<<freePos)) {
				slot := i*slabBitsPerShard + freePos
				offset := slot * int(p.slotSize)
				data := p.backing.Bytes()[offset : offset+int(p.slotSize) : offset+int(p.slotSize)]
				return newBuffer(data, func() {
					p.shards[i].mask.And(^(uint64(1) << freePos))
				}), nil
			}
			// CAS lost to a concurrent TryAcquire; reload and retry this shard.
		}
	}
	return nil, ErrExhausted
}

// SlotSize returns the (page-aligned) size of each slot in bytes.
func (p *SlabPool) SlotSize() int { return int(p.slotSize) }

// NumSlots returns the total number of slots.
func (p *SlabPool) NumSlots() int { return int(p.numSlots) }

// RawData returns a view of the complete slab for io_uring buffer registration.
// It does not transfer ownership. Do not free it or access slots through this
// view; use the Buffers returned by TryAcquire. The view expires at Close.
func (p *SlabPool) RawData() []byte {
	if p.backing == nil {
		return nil
	}
	return p.backing.Bytes()
}

// Close releases the slab's backing reference. Call after all acquired buffers
// have been unpinned and any io_uring registration has ended. Close must not race
// acquisition or Unpin.
func (p *SlabPool) Close() {
	if p.backing != nil {
		p.backing.Unpin()
		p.backing = nil
	}
}
