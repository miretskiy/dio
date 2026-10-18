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
	inline  [1][]byte
	chunks  [][]byte
	refs    atomic.Int32
	release func()
}

// newBuffer wraps a nonempty, aligned allocation with one reference.
func newBuffer(data []byte, release func()) *Buffer {
	b := &Buffer{release: release}
	b.inline[0] = data[:len(data):len(data)]
	b.chunks = b.inline[:]
	b.refs.Store(1)
	return b
}

func newChunkBuffer(chunks [][]byte, release func()) *Buffer {
	b := &Buffer{chunks: chunks, release: release}
	b.refs.Store(1)
	return b
}

func (b *Buffer) clear() {
	for _, data := range b.chunks {
		clear(data)
	}
}

// NewBuffer maps at least size zeroed bytes, rounded up to a page, without warming it.
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

// Chunks returns the memory covered by this buffer, which may be non-contiguous.
// Hold a reference while using the chunks, including pending I/O. Their contents
// may be changed; do not replace or reslice entries in the returned list.
func (b *Buffer) Chunks() [][]byte {
	return b.chunks
}

// Unpin releases one reference. The last reference calls the allocation's
// release function exactly once. Calling Unpin after release is a programming error.
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
