package alloc

import (
	"context"
	"errors"
	"math/bits"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/miretskiy/dio/v2/align"
)

// SlabAllocator subdivides one region into fixed-size
// objects. Acquisition and release use atomic bitmaps. Acquire retries on a
// timer when exhausted; returning a chunk does not notify waiting borrowers.
type SlabAllocator struct {
	slab          slabState
	mapped        []byte // the region, when the allocator mapped it
	retryInterval time.Duration
	noZero        bool
}

// NewSlabAllocator creates an allocator for one page-rounded slab, with 4 KiB
// chunks unless WithChunkSize is supplied. The slab is mapped during
// construction unless WithBacking supplies it, in which case slabSize is
// ignored. WithWarmup commits all pages before returning. Mapping failures panic.
func NewSlabAllocator(slabSize int64, opts ...Option) (*SlabAllocator, error) {
	cfg := allocatorConfig{chunkSize: align.BlockSize, retryInterval: 100 * time.Millisecond}
	for _, opt := range opts {
		opt(&cfg)
	}
	chunkSize := int(align.PageAlign(cfg.chunkSize))
	p := &SlabAllocator{retryInterval: cfg.retryInterval, noZero: cfg.noZero}
	region := cfg.backing
	if region == nil {
		if align.PageAlign(slabSize) < int64(chunkSize) {
			return nil, errors.New("alloc: slab smaller than one chunk")
		}
		p.mapped = align.AllocAligned(slabSize)
		region = p.mapped
	} else if len(region) < chunkSize || !align.IsAligned(region) {
		return nil, errors.New("alloc: backing must be page-aligned and hold at least one chunk")
	}
	if cfg.warmup {
		warmup(region, align.BlockSize)
	}
	p.slab = newSlabState(region, chunkSize)
	p.slab.leases = newLeases(len(p.slab.shards)*chunksPerShard, func(slot int, _ []byte) {
		p.slab.release(slot)
	}, cfg.releaseHook)
	return p, nil
}

// TryAcquire claims one chunk with atomic CAS, or returns ErrExhausted.
// The allocator must be open.
func (p *SlabAllocator) TryAcquire() (Memory, error) {
	slot, ok := p.slab.tryAcquire(rand.IntN(len(p.slab.shards)))
	if !ok {
		return Memory{}, ErrExhausted
	}
	offset := slot * p.slab.chunkSize
	m := p.slab.leases.lease(slot, p.slab.region[offset:offset+p.slab.chunkSize])
	if !p.noZero {
		clear(m.data)
	}
	return m, nil
}

// Acquire tries immediately, then retries every 100 ms (or WithRetryInterval)
// when exhausted. Context cancellation interrupts the wait. A returned chunk
// becomes visible to waiting callers on their next retry. Available capacity
// may be acquired even if ctx is already cancelled.
func (p *SlabAllocator) Acquire(ctx context.Context) (Memory, error) {
	if m, err := p.TryAcquire(); err != ErrExhausted {
		return m, err
	}

	timer := time.NewTimer(p.retryInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return Memory{}, ctx.Err()
		case <-timer.C:
			if m, err := p.TryAcquire(); err != ErrExhausted {
				return m, err
			}
			timer.Reset(p.retryInterval)
		}
	}
}

// Capacity reports the usable bytes in whole chunks, excluding a partial tail.
// It must not race Close.
func (p *SlabAllocator) Capacity() int64 { return int64(len(p.slab.region)) }

// Available reports a snapshot of unclaimed bytes in whole chunks. Chunks stay
// claimed until their last Unpin, even after a cache drops its own reference.
// It scans the allocation bitmaps without adding a shared counter to the hot
// path. Concurrent acquisitions and releases can change the result immediately.
// This is not a reservation.
// It must not race Close.
func (p *SlabAllocator) Available() int64 {
	var chunks int64
	for i := range p.slab.shards {
		chunks += int64(bits.OnesCount64(^p.slab.shards[i].mask.Load()))
	}
	return chunks * int64(p.slab.chunkSize)
}

// Close unmaps the slab if the allocator mapped it. It must not race allocator
// use. Do not acquire after Close. The caller must finish using and releasing
// borrowed memory first. Repeated closes are harmless.
func (p *SlabAllocator) Close() {
	if p.mapped != nil {
		align.FreeAligned(p.mapped)
	}
	p.slab, p.mapped = slabState{}, nil
}

const chunksPerShard = 64

// Keep independently contended masks on separate 64-byte cache lines.
type slabShard struct {
	mask atomic.Uint64
	_    [56]byte
}

// slabState subdivides a region into fixed-size chunks. Geometry is immutable;
// occupancy is atomic.
// A set bit means occupied; padding in the last shard is permanently occupied.
type slabState struct {
	region    []byte  // whole chunks
	leases    *leases // one unit per chunk
	shards    []slabShard
	chunkSize int
}

func newSlabState(region []byte, chunkSize int) slabState {
	chunks := len(region) / chunkSize
	s := slabState{
		region: region[: chunks*chunkSize : chunks*chunkSize], chunkSize: chunkSize,
		shards: make([]slabShard, (chunks-1)/chunksPerShard+1),
	}
	if tail := chunks % chunksPerShard; tail != 0 {
		s.shards[len(s.shards)-1].mask.Store(^uint64(0) << tail)
	}
	return s
}

// tryAcquire claims a free chunk, scanning shards from start.
func (s *slabState) tryAcquire(start int) (slot int, ok bool) {
	for j := range s.shards {
		i := (start + j) % len(s.shards)
		for {
			mask := s.shards[i].mask.Load()
			if mask == ^uint64(0) {
				break
			}
			bit := bits.TrailingZeros64(^mask)
			if s.shards[i].mask.CompareAndSwap(mask, mask|uint64(1)<<bit) {
				return i*chunksPerShard + bit, true
			}
		}
	}
	return 0, false
}

func (s *slabState) release(slot int) {
	shard := &s.shards[slot/chunksPerShard]
	bit := uint64(1) << (slot % chunksPerShard)
	shard.mask.And(^bit)
}
