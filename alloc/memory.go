package alloc

import (
	"math"
	"sync/atomic"

	"github.com/miretskiy/dio/v2/align"
)

// Memory is a handle to one lease of contiguous, page-aligned memory. It is a
// small value; copies name the same lease. Acquisition returns one reference,
// each successful TryInc adds one, and every reference is released with Unpin.
// The last Unpin returns the storage to its owner.
//
// Each lease has a generation, stored with its reference count in a word owned
// by the allocator. Every operation compares the handle's generation with the
// word's, and the last Unpin advances the generation, so a handle kept past its
// lease fails TryInc and panics on Unpin even after the memory is lent again.
type Memory struct {
	data   []byte // len(data) is the lease's extent
	leases *leases
	unit   uint32 // the lease's first page in its allocator
	gen    uint32
}

// leases holds the lease state for every unit an owner lends: the first page
// of each allocation of an arena, or the one unit of standalone memory.
type leases struct {
	words []atomic.Uint64 // per unit: generation<<32 | reference count
	owner *arena          // nil for standalone memory, unmapped on its last Unpin
}

func newLeases(units int, owner *arena) *leases {
	return &leases{words: make([]atomic.Uint64, units), owner: owner}
}

// lease starts a lease on unit with one reference. The caller owns the unit.
func (l *leases) lease(unit int, data []byte) Memory {
	gen := uint32(l.words[unit].Load() >> 32)
	l.words[unit].Store(uint64(gen)<<32 | 1)
	return Memory{data: data[:len(data):len(data)], leases: l, unit: uint32(unit), gen: gen}
}

// NewMemory maps at least size zeroed bytes, rounded up to a page, without warming it.
// The returned Memory owns one reference and unmaps its storage on the last
// Unpin. Invalid sizes and mapping failures panic.
func NewMemory(size int64) Memory {
	return newLeases(1, nil).lease(0, align.AllocAligned(size))
}

// Bytes returns the allocation, including any allocator rounding. Hold a
// reference while using it, including pending I/O. Do not reslice beyond it.
func (m Memory) Bytes() []byte { return m.data }

// extend consumes an adjacent raw range from the same arena. The receiver must
// be exclusively owned, with no concurrent TryInc/Unpin. The new extent retires
// old copies of the receiver; the raw range has no handles to retire.
func (m *Memory) extend(other rawMemory) bool {
	word := m.word()
	if word.Load() != uint64(m.gen)<<32|1 {
		panic("alloc: extend requires exclusive ownership")
	}
	pages := len(m.data) / align.BlockSize
	if int(m.unit)+pages != other.start {
		return false
	}
	m.data = (rawMemory{start: int(m.unit), n: pages + other.n}).bytes(m.leases.owner)
	m.gen++
	word.Store(uint64(m.gen)<<32 | 1)
	return true
}

// trim returns an exclusively owned lease's unused tail to the arena. Like
// extend, changing the extent retires copies of the old handle.
func (m *Memory) trim(size int) {
	word := m.word()
	if word.Load() != uint64(m.gen)<<32|1 {
		panic("alloc: trim requires exclusive ownership")
	}
	tail := len(m.data) - size
	m.data = m.data[:size:size]
	m.gen++
	word.Store(uint64(m.gen)<<32 | 1)
	m.leases.owner.freePages(int(m.unit)+size/align.BlockSize, tail/align.BlockSize)
}

// word returns the lease word, or nil for the zero Memory.
func (m Memory) word() *atomic.Uint64 {
	if m.leases == nil {
		return nil
	}
	return &m.leases.words[m.unit]
}

// TryInc acquires another reference unless this lease has ended or the
// reference count would overflow. Safe for concurrent use. This checks
// memory lifetime only; a cache must synchronize lookup-and-pin with eviction.
func (m Memory) TryInc() bool {
	w := m.word()
	if w == nil {
		return false
	}
	for {
		v := w.Load()
		if uint32(v>>32) != m.gen || uint32(v) == 0 || uint32(v) == math.MaxUint32 {
			return false
		}
		if w.CompareAndSwap(v, v+1) {
			return true
		}
	}
}

// Unpin releases one reference and reports whether it was the last reference.
// When true is returned, the storage has been returned to its owner: freed in
// its arena, where another acquisition may already have claimed it, or
// unmapped. Calling Unpin after the lease has ended is a programming error.
func (m Memory) Unpin() bool {
	w := m.word()
	for {
		v := w.Load()
		if uint32(v>>32) != m.gen || uint32(v) == 0 {
			panic("alloc: Unpin called on released memory")
		}
		if uint32(v) > 1 {
			if w.CompareAndSwap(v, v-1) {
				return false
			}
			continue
		}
		// The last reference: end the lease before the unit can be lent again.
		if w.CompareAndSwap(v, uint64(m.gen+1)<<32) {
			if owner := m.leases.owner; owner != nil {
				page, pages := int(m.unit), len(m.data)/align.BlockSize
				// Keep the common single-span release direct. Merged leases
				// crossing a span boundary use the range loop.
				if start := page % spanPages; pages <= spanPages-start {
					owner.freeSpan(page/spanPages, start, pages)
				} else {
					owner.freePages(page, pages)
				}
			} else {
				align.FreeAligned(m.data)
			}
			return true
		}
	}
}
