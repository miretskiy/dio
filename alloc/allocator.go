// Package alloc provides allocators of page-aligned, reference-counted Memory.
// ExtentAllocator lends whole extents. SlabAllocator and SpanAllocator subdivide
// one region. NewMemory maps standalone storage. Buffer provides byte I/O.
package alloc

import (
	"context"
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miretskiy/dio/v2/align"
)

// ErrExhausted means there is insufficient free capacity for an allocation.
var ErrExhausted = errors.New("alloc: allocator exhausted")

// ErrClosed means acquisition was attempted after the allocator was closed.
var ErrClosed = errors.New("alloc: allocator closed")

// Allocator lends zeroed, fixed-size Memory with one reference, unless built
// WithoutZeroing. Every Memory from one allocator has the same size: one slab
// chunk or one extent. Release
// each acquired reference with Memory.Unpin. Acquisition and release are safe
// for concurrent use.
type Allocator interface {
	// Acquire waits for capacity or context cancellation.
	Acquire(ctx context.Context) (Memory, error)
	// TryAcquire returns ErrExhausted instead of waiting for a borrower.
	TryAcquire() (Memory, error)
	// Close releases retained storage. It must not race acquisitions or Unpin.
	// The caller must finish using and releasing borrowed memory first.
	// Repeated closes are harmless. Do not acquire after Close.
	Close()
}

var (
	_ Allocator = (*ExtentAllocator)(nil)
	_ Allocator = (*SlabAllocator)(nil)
)

type allocatorConfig struct {
	chunkSize     int64
	backing       []byte // nil unless WithBacking
	retryInterval time.Duration
	warmup        bool
	noZero        bool
	releaseHook   func(data []byte)
}

// Option configures an allocator at construction time.
type Option func(*allocatorConfig)

// WithChunkSize sets the page-rounded object size of a SlabAllocator.
// The default is 4 KiB chunks.
func WithChunkSize(size int64) Option {
	return func(c *allocatorConfig) { c.chunkSize = size }
}

// WithBacking supplies a page-aligned region for a SlabAllocator or
// SpanAllocator to subdivide instead of mapping its own, such as the Bytes of
// a Memory registered for fixed I/O. Only whole chunks or spans are used. The
// caller keeps the region valid until Close and until every lent Memory is
// released. ExtentAllocator rejects it.
func WithBacking(region []byte) Option {
	return func(c *allocatorConfig) { c.backing = region }
}

// WithRetryInterval sets the delay between SlabAllocator Acquire
// retries when exhausted. The default is 100 ms. Cancellation interrupts the wait.
func WithRetryInterval(interval time.Duration) Option {
	return func(c *allocatorConfig) { c.retryInterval = interval }
}

// WithWarmup commits all backing memory during allocator construction.
// It zeroes one byte per page. By default, pages are faulted in when first used.
func WithWarmup() Option {
	return func(c *allocatorConfig) { c.warmup = true }
}

// WithReleaseHook runs hook on every final Unpin of memory from the
// allocator, with the released memory, before it returns to the allocator.
// The releasing goroutine still owns data while hook runs. Use it to account
// for memory until its last borrower releases it.
func WithReleaseHook(hook func(data []byte)) Option {
	return func(c *allocatorConfig) { c.releaseHook = hook }
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
	data   []byte
	leases *leases
	unit   uint32
	gen    uint32
}

// leases holds the lease state for every unit an allocator lends: a slab
// chunk, an extent, or the first page of a span allocation.
type leases struct {
	words   []atomic.Uint64 // per unit: generation<<32 | reference count
	release func(unit int, data []byte)
}

// newLeases returns lease state for units. A non-nil hook runs before release
// on every final Unpin.
func newLeases(units int, release func(unit int, data []byte), hook func(data []byte)) *leases {
	if hook != nil {
		inner := release
		release = func(unit int, data []byte) {
			hook(data)
			inner(unit, data)
		}
	}
	return &leases{words: make([]atomic.Uint64, units), release: release}
}

// lease starts a lease on unit with one reference. The caller owns the unit.
func (l *leases) lease(unit int, data []byte) Memory {
	gen := uint32(l.words[unit].Load() >> 32)
	l.words[unit].Store(uint64(gen)<<32 | 1)
	return Memory{data: data[:len(data):len(data)], leases: l, unit: uint32(unit), gen: gen}
}

// newMemory leases data from a single-unit owner that calls release.
func newMemory(data []byte, release func()) Memory {
	return newLeases(1, func(int, []byte) { release() }, nil).lease(0, data)
}

// NewMemory maps at least size zeroed bytes, rounded up to a page, without warming it.
// The returned Memory owns one reference and unmaps its storage on the last
// Unpin. Invalid sizes and mapping failures panic.
func NewMemory(size int64) Memory {
	data := align.AllocAligned(size)
	return newMemory(data, func() { align.FreeAligned(data) })
}

// Bytes returns the allocation, including any allocator rounding. Hold a
// reference while using it, including pending I/O. Do not reslice beyond it.
func (m Memory) Bytes() []byte { return m.data }

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
// When true is returned, any release hook and the return of the storage to its
// owner have completed. Pooled
// storage is reusable, but another acquisition may already have claimed it.
// Calling Unpin after the lease has ended is a programming error.
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
			m.leases.release(int(m.unit), m.data)
			return true
		}
	}
}
