// Package mempool provides a fixed-capacity pool of page-aligned mmap buffers
// with reference-counted lifecycle and token-bucket backpressure.
//
// # Design
//
// All buffers are pre-allocated and pre-warmed at construction time so the
// hot path never touches the kernel allocator. The pool stores raw byte
// slices internally and wraps them in a fresh [MmapBuffer] on each
// acquisition. This "pool-the-memory, not-the-struct" approach eliminates
// the ABA problem: a stale *MmapBuffer pointer held by a racing reader
// remains distinct from the new *MmapBuffer produced by the next Acquire
// call, even though both point at the same underlying memory.
//
// # Backpressure
//
// [MmapPool.Acquire] blocks until a buffer is available, providing natural
// token-bucket backpressure. [MmapPool.TryAcquire] is the non-blocking
// variant for callers that prefer to do other work when the pool is empty.
package mempool

import (
	"log/slog"
	"time"
	"fmt"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/align"
)

// panicOnMisuse controls whether detected API misuse causes a panic.
// Defaults to false. See [SetPanicOnMisuse].
var panicOnMisuse atomic.Bool

// SetPanicOnMisuse enables or disables panicking on detected API misuse.
// The default is false — misuse is silently tolerated, which is safe in the
// presence of context-cancellation races where multiple paths may attempt to
// release the same buffer.
//
// Currently detected misuse:
//   - [MmapBuffer.Unpin] called more times than [MmapPool.Acquire] + [MmapBuffer.TryInc]
//     (reference count driven below zero).
//   - [MmapBuffer.Unpin] somehow triggering resetAndRelease on a buffer whose
//     leased flag was already cleared (secondary defence; unreachable in
//     normal usage).
//
// Call with true in your program's init() or TestMain to catch bugs during
// development and testing.
func SetPanicOnMisuse(v bool) {
	panicOnMisuse.Store(v)
}

// MmapBuffer is a reference-counted, page-aligned memory buffer.
//
// Obtain one via [MmapPool.Acquire] or [MmapPool.TryAcquire]; release
// each reference with [MmapBuffer.Unpin]. The underlying memory returns
// to the pool when the reference count reaches zero.
//
// Multiple concurrent readers can safely share a buffer by calling
// [MmapBuffer.TryInc] before accessing it and [MmapBuffer.Unpin] when done.
type MmapBuffer struct {
	raw      []byte
	refCount atomic.Int32
	pool     *MmapPool

	// leased is set true on acquisition and CAS'd to false exactly once in
	// resetAndRelease to catch double-free bugs at the struct level.
	leased atomic.Bool
}

// TryInc atomically increments the reference count.
// Returns false if the buffer has already been released (refCount ≤ 0).
// Safe for concurrent use without external locks.
//
// Typical pattern for a reader racing an eviction:
//
//	if !buf.TryInc() {
//	    // buffer was reclaimed; handle miss
//	}
//	defer buf.Unpin()
func (b *MmapBuffer) TryInc() bool {
	for {
		c := b.refCount.Load()
		if c <= 0 {
			return false
		}
		if b.refCount.CompareAndSwap(c, c+1) {
			return true
		}
	}
}

// Bytes returns the full, page-aligned underlying byte slice.
func (b *MmapBuffer) Bytes() []byte { return b.raw }

// Cap returns the size of the underlying allocation in bytes.
func (b *MmapBuffer) Cap() int { return len(b.raw) }

// IsPooled reports whether the buffer was acquired from a pool (true) or
// allocated as a standalone one-off buffer via [NewMmapBuffer] (false).
func (b *MmapBuffer) IsPooled() bool { return b.pool != nil }

// AlignedBytes returns the smallest [align.BlockSize]-aligned prefix covering
// off bytes of valid data: raw[:align.PageAlign(off)].
// Returns nil for off ≤ 0.
//
// Use this when preparing a buffer for O_DIRECT write, where both the
// buffer length and the write size must be page-aligned.
func (b *MmapBuffer) AlignedBytes(off int64) []byte {
	if off <= 0 {
		return nil
	}
	return b.raw[:align.PageAlign(off)]
}

// Unpin decrements the reference count. When the count reaches zero,
// the raw memory is returned to the pool (or unmapped for unpooled buffers).
//
// If the count was already zero (i.e. this is an extra release), the call is
// silently ignored by default. Enable [SetPanicOnMisuse] to turn
// over-releases into panics during development.
func (b *MmapBuffer) Unpin() {
	v := b.refCount.Add(-1)
	if v == 0 {
		b.resetAndRelease()
	} else if v < 0 && panicOnMisuse.Load() {
		panic("mempool: Unpin called on an already-released MmapBuffer")
	}
}

func (b *MmapBuffer) resetAndRelease() {
	// SAFETY GUARD: the leased CAS ensures this path is entered at most once,
	// even if refCount somehow reaches zero more than once (shouldn't happen
	// through the normal API, but guards against future misuse).
	if !b.leased.CompareAndSwap(true, false) {
		if panicOnMisuse.Load() {
			panic("mempool: MmapBuffer.resetAndRelease called on an already-released buffer")
		}
		return
	}
	if b.pool != nil {
		b.pool.releaseBytes(b.raw)
	} else {
		_ = unix.Munmap(b.raw)
	}
}

// NewMmapBuffer allocates a standalone, unpooled page-aligned mmap buffer of
// at least size bytes. Unlike pool-acquired buffers, it is unmapped directly
// when Unpin reduces the reference count to zero — it never returns to a pool.
//
// Use this for one-off allocations that exceed a pool's slab size.
func NewMmapBuffer(size int64) *MmapBuffer {
	buf := &MmapBuffer{
		raw: align.AllocAligned(int(size)),
		// pool == nil: resetAndRelease will Munmap directly
	}
	buf.refCount.Store(1)
	buf.leased.Store(true)
	return buf
}

// --- MmapPool ---

// MmapPool is a fixed-capacity pool of pre-allocated, page-aligned mmap
// buffers. Create with [NewMmapPool]; close with [Close] after all
// outstanding buffers have been returned via [MmapBuffer.Unpin].
type MmapPool struct {
	buffers     chan []byte
	poolSize    int64
	name        string
	outstanding atomic.Int64
	slowWarn    time.Duration // 0 = disabled
}

// NewMmapPool creates a pool of capacity page-aligned buffers, each of
// bufferSize bytes (rounded up to the nearest 4 KiB page).
//
// All buffers are pre-allocated and pre-warmed at construction time.
// Typical usage: NewMmapPool("writes", 1<<20, 32) — 32 × 1 MiB slabs.
func NewMmapPool(name string, bufferSize int64, capacity int) *MmapPool {
	p := &MmapPool{
		buffers:  make(chan []byte, capacity),
		poolSize: bufferSize,
		name:     name,
	}
	for i := 0; i < capacity; i++ {
		p.buffers <- align.AllocAligned(int(bufferSize))
	}
	return p
}

// SetSlowAcquireWarning enables a diagnostic log when Acquire blocks for
// longer than d. Each time the threshold is crossed, a slog.Warn message is
// emitted with the pool name and outstanding count, then Acquire resumes
// waiting. Pass d=0 to disable (the default).
//
// This is invaluable for detecting an undersized pool in production: if
// Acquire regularly blocks, the pool capacity or buffer size needs tuning.
func (p *MmapPool) SetSlowAcquireWarning(d time.Duration) {
	p.slowWarn = d
}

// Acquire blocks until a buffer is available and returns it with refCount=1.
// The returned *MmapBuffer must eventually be released via [MmapBuffer.Unpin].
//
// Acquire panics if called on a closed pool.
// If [SetSlowAcquireWarning] has been called, a warning is logged each time
// the pool stays empty for longer than the configured duration.
func (p *MmapPool) Acquire() *MmapBuffer {
	if p.slowWarn == 0 {
		raw := <-p.buffers
		if raw == nil {
			panic(fmt.Sprintf("mempool: Acquire on closed pool %q", p.name))
		}
		p.outstanding.Add(1)
		return p.wrap(raw)
	}
	for {
		select {
		case raw := <-p.buffers:
			if raw == nil {
				panic(fmt.Sprintf("mempool: Acquire on closed pool %q", p.name))
			}
			p.outstanding.Add(1)
			return p.wrap(raw)
		case <-time.After(p.slowWarn):
			slog.Warn("mempool: Acquire blocked",
				"pool", p.name,
				"outstanding", p.outstanding.Load(),
				"capacity", cap(p.buffers),
				"threshold", p.slowWarn,
			)
		}
	}
}

// AcquireAligned returns a buffer large enough to hold at least size bytes.
// If size fits within the pool's slab size, a pooled buffer is returned.
// Otherwise an unpooled [NewMmapBuffer] is allocated and returned directly.
//
// Use this when the required size varies and may occasionally exceed the
// pool's standard slab (e.g. writing an oversized record).
func (p *MmapPool) AcquireAligned(size int64) *MmapBuffer {
	if size <= p.poolSize {
		return p.Acquire()
	}
	return NewMmapBuffer(size)
}

// TryAcquire returns a buffer immediately if one is available, or (nil, false)
// if the pool is currently empty. It is the non-blocking counterpart of [Acquire].
func (p *MmapPool) TryAcquire() (*MmapBuffer, bool) {
	select {
	case raw := <-p.buffers:
		if raw == nil {
			return nil, false // pool closed
		}
		p.outstanding.Add(1)
		return p.wrap(raw), true
	default:
		return nil, false
	}
}

// Outstanding returns the number of buffers currently checked out of the pool.
func (p *MmapPool) Outstanding() int64 { return p.outstanding.Load() }

// Capacity returns the total pool capacity (in-use + available).
func (p *MmapPool) Capacity() int { return cap(p.buffers) }

// Name returns the pool's name, useful for diagnostics.
func (p *MmapPool) Name() string { return p.name }

// wrap creates a fresh *MmapBuffer around a raw byte slice.
// ABA safety: each Acquire call produces a new struct, so stale pointers
// held by concurrent readers remain distinct even when memory is reused.
func (p *MmapPool) wrap(raw []byte) *MmapBuffer {
	buf := &MmapBuffer{raw: raw, pool: p}
	buf.refCount.Store(1)
	buf.leased.Store(true)
	return buf
}

// releaseBytes is the internal path called by MmapBuffer.resetAndRelease.
// It decrements the outstanding counter and either returns the raw memory
// to the pool channel, or unmaps it if the pool is unexpectedly full
// (which indicates a programming error — more Unpins than Acquires).
func (p *MmapPool) releaseBytes(raw []byte) {
	p.outstanding.Add(-1)
	select {
	case p.buffers <- raw:
		// Returned to pool.
	default:
		// Pool is full — this should never happen. Reclaim to avoid leak.
		_ = unix.Madvise(raw, unix.MADV_DONTNEED)
		_ = unix.Munmap(raw)
	}
}

// Close drains and releases all buffers remaining in the pool.
// Must be called only after all outstanding buffers have been returned.
func (p *MmapPool) Close() {
	close(p.buffers)
	for raw := range p.buffers {
		_ = unix.Munmap(raw)
	}
}
