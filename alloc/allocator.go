// Package alloc provides page-aligned, reference-counted Memory. An Allocator
// lends Memory of up to 128 KiB from a fixed arena. NewMemory maps standalone
// storage. Buffer combines adjacent allocations for larger byte streams.
package alloc

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/internal/buildutil"
)

// ErrExhausted means there are no free pages available for an allocation.
var ErrExhausted = errors.New("alloc: allocator exhausted")

// Allocator lends page-aligned Memory of up to MaxAllocationSize bytes from
// one fixed arena, zeroed unless built WithoutZeroing. TryAcquire and release
// are safe for concurrent use; Acquire retries on exhaustion.
//
// The arena is divided into 128 KiB spans. Each span stores two 64 KiB buddy
// trees in one atomic word. Requests round up to a power of two, with a 4 KiB
// minimum, to set an upper limit. The first nonfull span supplies its largest
// eligible contiguous block within that limit. Above 64 KiB, this is the free
// run crossing the half boundary; smaller blocks use buddy sizes and alignment.
// A claim marks the allocation's buddy subtrees and ancestors with one CAS;
// release coalesces free buddies in the same word.
//
// Allocation uses persistent atomic cursors spread across GOMAXPROCS preferred
// shards. Cursors retain positions both inside and outside their home shard;
// a failed search can inspect every span. These are hints, not ownership.
// Buffer growth batches raw claims and leases each contiguous run, which may
// exceed MaxAllocationSize. An existing unshared tail can be extended.
type Allocator struct {
	arena *arena
	cfg   allocatorConfig
	stats []allocationCounters
}

// MaxAllocationSize is the largest block returned by TryAcquire or Acquire.
// Buffer can merge adjacent claims into larger contiguous Memory handles.
const MaxAllocationSize = spanBytes

// NewAllocator maps one fixed arena of size bytes rounded up to whole
// 128 KiB spans. Mapping failures panic. The arena stays mapped until Close.
func NewAllocator(size int64, opts ...Option) *Allocator {
	cfg := allocatorConfig{retryInterval: 100 * time.Millisecond}
	for _, opt := range opts {
		opt(&cfg)
	}
	spans := int(max(1, (size+spanBytes-1)/spanBytes))
	region := align.AllocAligned(int64(spans) * spanBytes)
	if cfg.warmup {
		warmup(region, align.BlockSize)
	}
	ar := newArena(spans)
	ar.region = region
	return &Allocator{arena: ar, cfg: cfg, stats: make([]allocationCounters, spans)}
}

// TryAcquire returns one contiguous block, zeroed unless built WithoutZeroing.
// Size is capped at MaxSize and rounded up to a power of two (at least 4 KiB)
// as an upper limit; the returned block may be smaller. Use len(m.Bytes()) as
// the acquired size. It returns ErrExhausted when no pages are free.
// Size must be positive. Buffer combines blocks to satisfy the full request.
func (a *Allocator) TryAcquire(size int64) (Memory, error) {
	if buildutil.TestMode {
		buildutil.Assert(size > 0)
	}
	claimed, ok := a.arena.claim(allocationPages(size))
	if !ok {
		return Memory{}, ErrExhausted
	}
	m := rawMemory(claimed)
	a.prepareRaw(m, size)
	return m.lease(a.arena), nil
}

// allocationPages converts a positive request to a span's size-class limit.
func allocationPages(size int64) int {
	size = min(size, MaxAllocationSize)
	return 1 << levelOf(int((size+align.BlockSize-1)/align.BlockSize))
}

// Acquire returns one block under the same size limit as TryAcquire; it may
// be smaller than size. It tries immediately, then retries every 100 ms (or
// WithRetryInterval) while no pages are free. Context cancellation interrupts
// the wait. Free capacity is acquired even if ctx is already cancelled.
func (a *Allocator) Acquire(ctx context.Context, size int64) (Memory, error) {
	if m, err := a.TryAcquire(size); err != ErrExhausted {
		return m, err
	}
	timer := time.NewTimer(a.cfg.retryInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return Memory{}, ctx.Err()
		case <-timer.C:
			if m, err := a.TryAcquire(size); err != ErrExhausted {
				return m, err
			}
			timer.Reset(a.cfg.retryInterval)
		}
	}
}

// Capacity reports the arena's size in bytes, or zero after Close.
// It must not race Close.
func (a *Allocator) Capacity() int64 { return int64(len(a.arena.region)) }

// MaxSize reports the largest TryAcquire or Acquire result: 128 KiB.
func (a *Allocator) MaxSize() int64 { return MaxAllocationSize }

// Available reports a snapshot of free bytes. Memory stays in use until
// its last Unpin. It scans every span and must not race Close.
func (a *Allocator) Available() int64 {
	if a.arena.region == nil {
		return 0
	}
	return int64(a.arena.available()) * align.BlockSize
}

// Close unmaps the arena. It must not race other use; the caller must release
// borrowed memory first. Repeated closes are harmless. Do not acquire after Close.
func (a *Allocator) Close() {
	if a.arena.region != nil {
		align.FreeAligned(a.arena.region)
		a.arena.region = nil
	}
}

type allocatorConfig struct {
	retryInterval time.Duration
	warmup        bool
	noZero        bool
}

// Option configures an allocator at construction time.
type Option func(*allocatorConfig)

// WithRetryInterval sets the delay between Acquire retries when exhausted. The
// default is 100 ms. Cancellation interrupts the wait.
func WithRetryInterval(interval time.Duration) Option {
	return func(c *allocatorConfig) { c.retryInterval = interval }
}

// WithWarmup commits the arena's memory at construction. By default, pages
// are faulted in when first used.
func WithWarmup() Option {
	return func(c *allocatorConfig) { c.warmup = true }
}

// WithoutZeroing skips zeroing memory on acquisition, so it holds whatever its
// previous borrower left. Use it when every byte is written before it is read,
// such as a direct-I/O read buffer.
func WithoutZeroing() Option {
	return func(c *allocatorConfig) { c.noZero = true }
}

// Use at most eight workers, with at least 64 MiB per worker. Partition
// boundaries follow stride so each page is touched by exactly one worker.
func warmup(data []byte, stride int) {
	workers := min(8, runtime.GOMAXPROCS(0), max(1, len(data)/(64<<20)))
	if workers == 1 {
		align.Warmup(data, stride)
		return
	}
	partitionSize := (len(data) + workers - 1) / workers
	partitionSize = (partitionSize + stride - 1) / stride * stride
	var wg sync.WaitGroup
	for start := 0; start < len(data); start += partitionSize {
		wg.Go(func() { align.Warmup(data[start:min(start+partitionSize, len(data))], stride) })
	}
	wg.Wait()
}
