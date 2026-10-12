package alloc

import (
	"math"
	"sync/atomic"

	"github.com/miretskiy/dio/v2/align"
)

// Buffer owns a reference to page-aligned memory. Each successful TryInc
// adds a reference; every reference must be released with Unpin. The last
// reference returns the storage to its owner. A buffer must not be copied.
type Buffer struct {
	raw     []byte
	refs    atomic.Int32
	release func()
}

// newBuffer wraps a nonempty, aligned allocation with one reference.
// release returns that allocation to its owner on the last Unpin.
func newBuffer(data []byte, release func()) *Buffer {
	b := &Buffer{raw: data[:len(data):len(data)], release: release}
	b.refs.Store(1)
	return b
}

// NewBuffer maps and prefaults at least size bytes, rounded up to a page.
// The returned buffer owns one reference and unmaps its storage on the last
// Unpin. Invalid sizes and mapping failures panic.
func NewBuffer(size int64) *Buffer {
	data := align.AllocAligned(size)
	return newBuffer(data, func() { align.FreeAligned(data) })
}

// TryInc acquires another reference unless the buffer has been released or
// the reference count would overflow. Safe for concurrent use. This checks
// memory lifetime only; a cache must synchronize lookup-and-pin with eviction.
func (b *Buffer) TryInc() bool {
	for {
		n := b.refs.Load()
		if n == 0 || n == math.MaxInt32 {
			return false
		}
		if b.refs.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// Bytes returns the memory covered by this buffer. Hold a reference for as
// long as the returned slice is in use, including pending I/O. The slice is a
// view, not an independent allocation; release the reference with Unpin.
func (b *Buffer) Bytes() []byte { return b.raw }

// Cap returns the number of bytes covered by this buffer.
func (b *Buffer) Cap() int { return len(b.raw) }

// Unpin releases one reference. The last reference invokes the release
// callback exactly once. Calling Unpin after release is a programming error.
func (b *Buffer) Unpin() {
	for {
		n := b.refs.Load()
		if n == 0 {
			panic("alloc: Unpin called on an already-released buffer")
		}
		if b.refs.CompareAndSwap(n, n-1) {
			if n == 1 {
				b.release()
			}
			return
		}
	}
}
