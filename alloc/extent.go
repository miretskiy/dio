package alloc

import (
	"context"
	"errors"
	"sync"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/collections"
)

// ExtentAllocator owns a fixed number of equally sized extents, mapped
// at construction. Each acquisition lends one whole extent. Acquire waits for a
// free extent; TryAcquire returns ErrExhausted instead. The last Unpin returns
// the extent for reuse.
type ExtentAllocator struct {
	// Occupied slots are borrowed; free slots retain their mappings for reuse.
	mu         sync.Mutex
	available  sync.Cond
	slots      collections.FixedList[extentSlot]
	handles    []collections.Handle // per unit: the slot handle of its current lease
	leases     *leases              // one unit per extent
	closed     bool
	noZero     bool
	extentSize int64
}

// NewExtentAllocator creates maxExtents extents of extentSize bytes rounded up to 4 KiB.
// maxExtents is clamped to at least one. All extents are mapped during
// construction; WithWarmup also commits their pages. Mapping failures panic.
func NewExtentAllocator(extentSize int64, maxExtents int, opts ...Option) (*ExtentAllocator, error) {
	maxExtents = max(1, maxExtents)
	var cfg allocatorConfig
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.backing != nil {
		return nil, errors.New("alloc: WithBacking requires NewSlabAllocator or NewSpanAllocator")
	}
	if cfg.chunkSize != 0 {
		return nil, errors.New("alloc: WithChunkSize requires NewSlabAllocator")
	}
	extentSize = align.PageAlign(extentSize)
	p := &ExtentAllocator{
		slots:      collections.MakeFixedList[extentSlot](maxExtents),
		handles:    make([]collections.Handle, maxExtents),
		noZero:     cfg.noZero,
		extentSize: extentSize,
	}
	p.available.L = &p.mu
	p.leases = newLeases(maxExtents, func(unit int, _ []byte) { p.release(unit) }, cfg.releaseHook)
	for i, slot := range p.slots.AllSlots() {
		*slot = extentSlot{data: align.AllocAligned(extentSize), unit: i}
		if cfg.warmup {
			warmup(slot.data, align.BlockSize)
		}
	}
	return p, nil
}

// Acquire waits for a free extent or ctx cancellation and returns it with one
// reference. Cancellation observed before reservation returns ctx.Err(); after
// reservation it does not revoke the memory. Closed allocators return ErrClosed.
// Release with Unpin.
func (p *ExtentAllocator) Acquire(ctx context.Context) (Memory, error) {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return Memory{}, ErrClosed
	}

	if p.slots.Len() == p.slots.Cap() {
		// AfterFunc runs the callback in a separate goroutine, even if ctx is
		// already cancelled. It can safely wait for mu, which we hold here:
		// Wait releases it while sleeping; other paths unlock before returning.
		stop := context.AfterFunc(ctx, func() {
			// Lock before broadcasting so cancellation cannot miss a waiter
			// between its context check and Wait. Wake all to reach this caller.
			p.mu.Lock()
			defer p.mu.Unlock()
			p.available.Broadcast()
		})
		defer stop()
		for p.slots.Len() == p.slots.Cap() && ctx.Err() == nil {
			p.available.Wait()
		}
	}

	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		return Memory{}, err
	}

	m := p.acquire()
	p.mu.Unlock()
	if !p.noZero {
		clear(m.data)
	}
	return m, nil
}

// TryAcquire returns a free extent, ErrExhausted if none is free, or ErrClosed
// after Close. It may wait for the allocator lock but never waits for a borrower.
func (p *ExtentAllocator) TryAcquire() (Memory, error) {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return Memory{}, ErrClosed
	}
	if p.slots.Len() == p.slots.Cap() {
		p.mu.Unlock()
		return Memory{}, ErrExhausted
	}
	m := p.acquire()
	p.mu.Unlock()
	if !p.noZero {
		clear(m.data)
	}
	return m, nil
}

// extentSlot is one extent's mapping and its lease unit.
type extentSlot struct {
	data []byte
	unit int
}

// The caller holds mu and has checked that an extent is free.
func (p *ExtentAllocator) acquire() Memory {
	handle := p.slots.PushBack()
	slot := p.slots.Value(handle)
	p.handles[slot.unit] = handle
	return p.leases.lease(slot.unit, slot.data)
}

func (p *ExtentAllocator) release(unit int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.slots.Recycle(p.handles[unit])
	p.available.Broadcast()
}

// Capacity reports the pool's total usable bytes, or zero after Close.
func (p *ExtentAllocator) Capacity() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0
	}
	return int64(p.slots.Cap()) * p.extentSize
}

// Available reports a snapshot of free extent bytes. An extent stays occupied
// until its last Unpin. The result does not reserve those bytes.
func (p *ExtentAllocator) Available() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0
	}
	return int64(p.slots.Cap()-p.slots.Len()) * p.extentSize
}

// Close releases all mappings. It must not race acquisitions or Unpin calls.
// The caller must finish using and releasing borrowed memory first.
// Repeated closes are harmless.
func (p *ExtentAllocator) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	for _, slot := range p.slots.AllSlots() {
		align.FreeAligned(slot.data)
		*slot = extentSlot{}
	}
	p.closed = true
}
