package alloc

import (
	"context"
	"math/bits"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/miretskiy/dio/v2/align"
)

// SlabAllocator subdivides one backing Buffer into fixed-size
// objects. Acquisition and release use atomic bitmaps. Acquire retries on a
// timer when exhausted; returning a buffer does not notify waiting borrowers.
type SlabAllocator struct {
	slab          slabState
	retryInterval time.Duration
}

// NewSlabAllocator creates an allocator for one page-rounded slab, with 4 KiB
// chunks unless WithChunkSize is supplied. It holds its own reference to supplied
// backing. Otherwise backing is mapped during construction. WithWarmup commits
// all backing pages before returning.
// Mapping failures panic.
func NewSlabAllocator(slabSize int64, opts ...Option) (*SlabAllocator, error) {
	cfg := allocatorConfig{chunkSize: align.BlockSize, retryInterval: 100 * time.Millisecond}
	for _, opt := range opts {
		opt(&cfg)
	}
	backing := cfg.backing
	if backing == nil {
		backing = NewBuffer(align.PageAlign(slabSize))
		defer backing.Unpin() // Release the initial reference after taking the slab's.
	}
	backing.TryInc()
	if cfg.warmup {
		warmup(backing.Chunks()[0], align.BlockSize)
	}
	return &SlabAllocator{
		slab:          newSlabState(backing, int(align.PageAlign(cfg.chunkSize))),
		retryInterval: cfg.retryInterval,
	}, nil
}

// TryAcquire rounds size up to whole chunks and claims them with atomic CAS.
// Chunks need not be contiguous. Insufficient space returns ErrExhausted after
// returning any partial claims. Size must be positive; the allocator must be open.
func (p *SlabAllocator) TryAcquire(size int64) (*Buffer, error) {
	n := int((size-1)/int64(p.slab.chunkSize) + 1)
	start := rand.IntN(len(p.slab.shards))
	var b *Buffer
	if n == 1 {
		b = p.slab.tryAcquire(start)
	} else {
		b = p.slab.tryAcquireN(start, n)
	}
	if b != nil {
		b.clear()
		return b, nil
	}
	return nil, ErrExhausted
}

// Acquire tries immediately, then retries every 100 ms (or WithRetryInterval)
// when exhausted. Context cancellation interrupts the wait. A returned chunk
// becomes visible to waiting callers on their next retry. Available capacity
// may be acquired even if ctx is already cancelled.
func (p *SlabAllocator) Acquire(ctx context.Context, size int64) (*Buffer, error) {
	if b, err := p.TryAcquire(size); err != ErrExhausted {
		return b, err
	}

	timer := time.NewTimer(p.retryInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			if b, err := p.TryAcquire(size); err != ErrExhausted {
				return b, err
			}
			timer.Reset(p.retryInterval)
		}
	}
}

// Close releases the allocator's backing reference.
// It must not race allocator use. Do not acquire buffers after Close.
// The caller must finish using and releasing borrowed buffers first.
// Repeated closes are harmless.
func (p *SlabAllocator) Close() {
	if p.slab.backing != nil {
		p.slab.backing.Unpin()
		p.slab = slabState{}
	}
}

const chunksPerShard = 64

// Keep independently contended masks on separate 64-byte cache lines.
type slabShard struct {
	mask atomic.Uint64
	_    [56]byte
}

// slabState subdivides backing into fixed-size chunks. Geometry is immutable;
// occupancy is atomic.
// A set bit means occupied; padding in the last shard is permanently occupied.
type slabState struct {
	backing   *Buffer
	shards    []slabShard
	chunkSize int
}

type slabClaim struct {
	shardIdx int
	mask     uint64
}

func newSlabState(backing *Buffer, chunkSize int) slabState {
	chunks := len(backing.Chunks()[0]) / chunkSize
	s := slabState{
		backing: backing, chunkSize: chunkSize,
		shards: make([]slabShard, (chunks-1)/chunksPerShard+1),
	}
	if tail := chunks % chunksPerShard; tail != 0 {
		s.shards[len(s.shards)-1].mask.Store(^uint64(0) << tail)
	}
	return s
}

func (s *slabState) tryAcquire(start int) *Buffer {
	for j := range s.shards {
		i := (start + j) % len(s.shards)
		for {
			mask := s.shards[i].mask.Load()
			if mask == ^uint64(0) {
				break
			}
			bit := bits.TrailingZeros64(^mask)
			if s.shards[i].mask.CompareAndSwap(mask, mask|uint64(1)<<bit) {
				slot := i*chunksPerShard + bit
				offset := slot * s.chunkSize
				data := s.backing.Chunks()[0][offset : offset+s.chunkSize : offset+s.chunkSize]
				return newBuffer(data, func() { s.release(slot) })
			}
		}
	}
	return nil
}

func (s *slabState) release(slot int) {
	shard := &s.shards[slot/chunksPerShard]
	bit := uint64(1) << (slot % chunksPerShard)
	shard.mask.And(^bit)
}

func (s *slabState) tryAcquireN(start, n int) *Buffer {
	if n <= 0 {
		return nil
	}
	var claims []slabClaim
	remaining := n
	for j := 0; j < len(s.shards) && remaining > 0; j++ {
		i := (start + j) % len(s.shards)
		for {
			mask := s.shards[i].mask.Load()
			free := ^mask
			if free == 0 {
				break
			}
			var claimMask uint64
			claimed := 0
			for claimed < remaining && free != 0 {
				claimMask |= uint64(1) << bits.TrailingZeros64(free)
				free &= free - 1
				claimed++
			}
			if s.shards[i].mask.CompareAndSwap(mask, mask|claimMask) {
				claims = append(claims, slabClaim{i, claimMask})
				remaining -= claimed
				break
			}
		}
	}
	if remaining > 0 {
		s.releaseClaims(claims)
		return nil
	}

	// Assemble views only after the entire allocation succeeds.
	data := make([][]byte, n)
	next := 0
	for _, c := range claims {
		for mask := c.mask; mask != 0; mask &= mask - 1 {
			slot := c.shardIdx*chunksPerShard + bits.TrailingZeros64(mask)
			offset := slot * s.chunkSize
			data[next] = s.backing.Chunks()[0][offset : offset+s.chunkSize : offset+s.chunkSize]
			next++
		}
	}
	return newChunkBuffer(data, func() { s.releaseClaims(claims) })
}

func (s *slabState) releaseClaims(claims []slabClaim) {
	for _, c := range claims {
		s.shards[c.shardIdx].mask.And(^c.mask)
	}
}
