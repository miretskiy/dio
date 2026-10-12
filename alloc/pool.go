// Package alloc provides page-aligned storage for I/O. MmapPool lends whole
// mmap slabs; SlabPool divides one contiguous region into fixed-size slots.
// Both return reference-counted Buffers. NewBuffer maps standalone storage.
package alloc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/internal/intrusive"
)

// ErrExhausted means all of a pool's capacity is held by borrowers.
var ErrExhausted = errors.New("alloc: pool exhausted")

// ErrClosed means acquisition was attempted after the pool was closed.
var ErrClosed = errors.New("alloc: pool closed")

// MmapPool owns a bounded number of equally sized mmap slabs. Acquire waits
// for capacity; TryAcquire returns ErrExhausted instead. Returned mappings are
// reused before new ones are allocated. The last Unpin returns a slab.
type MmapPool struct {
	// Occupied slots are borrowed; free slots retain their mappings for reuse.
	mu        sync.Mutex
	available sync.Cond
	slots     intrusive.FixedList[[]byte]
	closed    bool
	slabSize  int64
}

type poolConfig struct {
	lazyInitSlabs int
}

// Option configures a MmapPool at construction time.
type Option func(*poolConfig)

// WithLazyInitSlabs defers mapping up to n slabs. Returned slabs are reused
// before mapping more. n must be non-negative; n >= capacity makes all slabs lazy.
func WithLazyInitSlabs(n int) Option {
	return func(pc *poolConfig) { pc.lazyInitSlabs = n }
}

// NewMmapPool creates numSlabs slabs of slabSize bytes rounded up to 4 KiB.
// numSlabs is clamped to at least one. Slabs are mapped and prefaulted unless
// deferred by WithLazyInitSlabs. Invalid configuration returns an error;
// mapping failures panic.
func NewMmapPool(slabSize int64, numSlabs int, opts ...Option) (*MmapPool, error) {
	numSlabs = max(1, numSlabs)
	if slabSize <= 0 || slabSize > int64(math.MaxInt)-align.BlockMask {
		return nil, fmt.Errorf("alloc: invalid slab size %d", slabSize)
	}
	var cfg poolConfig
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.lazyInitSlabs < 0 {
		return nil, errors.New("alloc: lazy slab count must be non-negative")
	}
	p := &MmapPool{
		slots:    intrusive.MakeFixedList[[]byte](numSlabs),
		slabSize: align.PageAlign(slabSize),
	}
	p.available.L = &p.mu
	for i, data := range p.slots.AllSlots() {
		if i >= numSlabs-cfg.lazyInitSlabs {
			break
		}
		*data = align.AllocAligned(p.slabSize)
	}
	return p, nil
}

// Acquire waits for capacity or ctx cancellation and returns a buffer with one
// reference on success. Cancellation observed before reservation returns ctx.Err();
// after reservation it does not interrupt mapping or revoke the buffer. Closed pools
// return ErrClosed; mapping failures panic. Release the buffer with Unpin.
func (p *MmapPool) Acquire(ctx context.Context) (*Buffer, error) {
	handle, data, err := p.acquireSlot(ctx)
	if err != nil {
		return nil, err
	}
	return p.acquireBuffer(handle, data), nil
}

func (p *MmapPool) acquireSlot(ctx context.Context) (intrusive.Handle, *[]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, nil, ErrClosed
	}

	if p.slots.Len() == p.slots.Cap() {
		// AfterFunc runs the callback in a separate goroutine, even if ctx is
		// already cancelled. It can safely wait for mu, which we hold here:
		// Wait releases it while sleeping, and returning releases it via defer.
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
		// Pass on any release signal consumed by this cancelled waiter.
		p.available.Signal()
		return 0, nil, err
	}

	handle := p.slots.PushBack()
	return handle, p.slots.Value(handle), nil
}

// TryAcquire returns ErrExhausted if every slab is held or ErrClosed after Close.
// It may map and prefault memory but never waits for a borrower. Mapping failures panic.
func (p *MmapPool) TryAcquire() (*Buffer, error) {
	handle, data, err := p.tryAcquireSlot()
	if err != nil {
		return nil, err
	}
	return p.acquireBuffer(handle, data), nil
}

func (p *MmapPool) tryAcquireSlot() (intrusive.Handle, *[]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, nil, ErrClosed
	}
	handle, ok := p.slots.TryPushBack()
	if !ok {
		return 0, nil, ErrExhausted
	}
	return handle, p.slots.Value(handle), nil
}

// The reserved slot belongs exclusively to this acquisition. Its storage is
// stable, so mapping and prefaulting can run without holding the pool mutex.
func (p *MmapPool) acquireBuffer(handle intrusive.Handle, data *[]byte) *Buffer {
	if *data == nil {
		*data = align.AllocAligned(p.slabSize)
	}
	return newBuffer(*data, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.slots.Remove(handle)
		p.available.Signal()
	})
}

// Capacity returns the maximum number of slabs held at once.
func (p *MmapPool) Capacity() int { return p.slots.Cap() }

// Close frees all mappings. Call exactly once after all acquisitions and Unpin
// calls have finished. It must not race pool use.
func (p *MmapPool) Close() {
	for _, data := range p.slots.AllSlots() {
		align.FreeAligned(*data)
		*data = nil
	}
	p.closed = true
}
