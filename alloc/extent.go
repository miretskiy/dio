package alloc

import (
	"context"
	"errors"
	"sync"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/collections"
)

// ExtentAllocator owns a fixed number of equally sized backing extents, mapped
// at construction. Acquire waits for capacity; TryAcquire returns ErrExhausted
// instead. The last Unpin returns all acquired extents for reuse.
type ExtentAllocator struct {
	// Occupied slots are borrowed; free slots retain their mappings for reuse.
	mu         sync.Mutex
	available  sync.Cond
	slots      collections.FixedList[*Buffer]
	closed     bool
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
		return nil, errors.New("alloc: WithBackingBuffer requires NewSlabAllocator")
	}
	if cfg.chunkSize != 0 {
		return nil, errors.New("alloc: WithChunkSize requires NewSlabAllocator")
	}
	extentSize = align.PageAlign(extentSize)
	p := &ExtentAllocator{
		slots:      collections.MakeFixedList[*Buffer](maxExtents),
		extentSize: extentSize,
	}
	p.available.L = &p.mu
	for _, data := range p.slots.AllSlots() {
		*data = NewBuffer(extentSize)
		if cfg.warmup {
			warmup((*data).Chunks()[0], align.BlockSize)
		}
	}
	return p, nil
}

// Acquire waits for capacity or ctx cancellation and returns a buffer with one
// reference on success. Cancellation observed before reservation returns ctx.Err();
// after reservation it does not revoke the buffer. Closed allocators return ErrClosed.
// Size must be positive and is rounded up to whole extents. Release with Unpin.
func (p *ExtentAllocator) Acquire(ctx context.Context, size int64) (*Buffer, error) {
	n := int((size-1)/p.extentSize + 1)
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return nil, ErrClosed
	}

	if p.slots.Cap()-p.slots.Len() < n {
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
		for p.slots.Cap()-p.slots.Len() < n && ctx.Err() == nil {
			p.available.Wait()
		}
	}

	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		return nil, err
	}

	b := p.acquireN(n)
	p.mu.Unlock()
	b.clear()
	return b, nil
}

// TryAcquire rounds positive size up to whole extents. It returns ErrExhausted
// if too few extents are free, or ErrClosed after Close.
// It may wait for the allocator lock but never waits for a borrower.
func (p *ExtentAllocator) TryAcquire(size int64) (*Buffer, error) {
	n := int((size-1)/p.extentSize + 1)
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return nil, ErrClosed
	}
	if p.slots.Cap()-p.slots.Len() < n {
		p.mu.Unlock()
		return nil, ErrExhausted
	}
	b := p.acquireN(n)
	p.mu.Unlock()
	b.clear()
	return b, nil
}

// The caller holds mu and has checked that n extents are available.
func (p *ExtentAllocator) acquireN(n int) *Buffer {
	if n == 1 {
		handle := p.slots.PushBack()
		return newBuffer((*p.slots.Value(handle)).Chunks()[0], func() { p.release(handle) })
	}
	chunks := make([][]byte, n)
	handles := make([]collections.Handle, n)
	for i := range chunks {
		handle := p.slots.PushBack()
		handles[i] = handle
		chunks[i] = (*p.slots.Value(handle)).Chunks()[0]
	}
	return newChunkBuffer(chunks, func() { p.release(handles...) })
}

func (p *ExtentAllocator) release(handles ...collections.Handle) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, handle := range handles {
		p.slots.Recycle(handle)
	}
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
// until the buffer's last Unpin. The result does not reserve those bytes.
func (p *ExtentAllocator) Available() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0
	}
	return int64(p.slots.Cap()-p.slots.Len()) * p.extentSize
}

// Close releases all mappings. It must not race acquisitions or Unpin calls.
// The caller must finish using and releasing borrowed buffers first.
// Repeated closes are harmless.
func (p *ExtentAllocator) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	for _, data := range p.slots.AllSlots() {
		(*data).Unpin()
		*data = nil
	}
	p.closed = true
}
