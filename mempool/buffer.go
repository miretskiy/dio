package mempool

import (
	"fmt"
	"io"
	"math"

	"github.com/miretskiy/dio/v2/align"
)

// Allocator supplies page-aligned memory to an [AlignedBuffer]. [*SlabPool]
// and [*MmapPool] implement it.
//
// The caller is responsible for keeping an allocator open while any buffer
// holds its memory.
type Allocator interface {
	// MaxAlloc returns the largest contiguous allocation Alloc supports.
	MaxAlloc() int
	// Alloc returns page-aligned memory of at least size bytes, where
	// size ≤ MaxAlloc. The memory may be longer than size: a pool returns a
	// whole slot or buffer. Whether Alloc blocks or fails when no memory is
	// available is up to the allocator: SlabPool fails, MmapPool blocks.
	Alloc(size int) ([]byte, error)
	// Free returns memory obtained from Alloc.
	Free(mem []byte)
}

// mmapAllocator maps fresh memory for each allocation. AlignedBuffer uses it
// when given no allocator.
type mmapAllocator struct{}

func (mmapAllocator) MaxAlloc() int                  { return math.MaxInt &^ align.BlockMask }
func (mmapAllocator) Alloc(size int) ([]byte, error) { return align.AllocAligned(size), nil }
func (mmapAllocator) Free(mem []byte)                { align.FreeAligned(mem) }

// AlignedBuffer is a growable byte buffer, in the spirit of bytes.Buffer,
// whose memory comes from an [Allocator] in page-aligned chunks. Every chunk
// starts on a page boundary, so a page-multiple range within a chunk is a
// valid O_DIRECT transfer; with a registered [SlabPool], it is also a valid
// fixed-buffer transfer.
//
// The buffer holds Len bytes of data within Cap bytes of capacity. Bytes past
// Len are spare capacity, reachable through [AlignedBuffer.Range] and
// [AlignedBuffer.Slices].
//
// An AlignedBuffer has a single owner, who returns its memory with Release.
// It is not safe for concurrent use.
type AlignedBuffer struct {
	alloc  Allocator
	chunks [][]byte
	inline [1][]byte // backing store for chunks: most buffers have one
	hint   int
	cap    int
	n      int
}

// NewAlignedBuffer returns an empty buffer that allocates from alloc as data
// is written. A nil alloc maps memory directly with align.AllocAligned. The
// first allocation is sizeHint bytes, trimmed to what alloc can supply
// contiguously; capacity grows from there as needed. NewAlignedBuffer itself
// allocates nothing; call Grow to allocate up front.
func NewAlignedBuffer(alloc Allocator, sizeHint int) *AlignedBuffer {
	if alloc == nil {
		alloc = mmapAllocator{}
	}
	b := &AlignedBuffer{alloc: alloc, hint: sizeHint}
	b.chunks = b.inline[:0]
	if n := (sizeHint + alloc.MaxAlloc() - 1) / alloc.MaxAlloc(); n > len(b.inline) {
		b.chunks = make([][]byte, 0, n) // size the chunk list for the hint once
	}
	return b
}

// Len returns the number of data bytes.
func (b *AlignedBuffer) Len() int { return b.n }

// Cap returns the capacity in bytes.
func (b *AlignedBuffer) Cap() int { return b.cap }

// Grow ensures room for n more data bytes past Len, allocating chunks as
// needed. It returns the allocator's error if it cannot.
func (b *AlignedBuffer) Grow(n int) error {
	for spare := b.cap - b.n; spare < n; spare = b.cap - b.n {
		size := n - spare
		if len(b.chunks) == 0 {
			size = max(size, b.hint)
		} else {
			size = max(size, b.cap) // double, so small writes do not mean many chunks
		}
		mem, err := b.alloc.Alloc(min(int(align.PageAlign(int64(size))), b.alloc.MaxAlloc()))
		if err != nil {
			return err
		}
		b.chunks = append(b.chunks, mem)
		b.cap += len(mem)
	}
	return nil
}

// SetLen sets the data length to n, 0 ≤ n ≤ Cap. Use it after filling spare
// capacity directly, for example with a read into [AlignedBuffer.Slices].
func (b *AlignedBuffer) SetLen(n int) {
	if n < 0 || n > b.cap {
		panic(fmt.Sprintf("mempool: SetLen(%d) outside capacity %d", n, b.cap))
	}
	b.n = n
}

// Reset discards the data, keeping the capacity.
func (b *AlignedBuffer) Reset() { b.n = 0 }

// Range returns [off, off+n) as one slice if it lies within a single chunk.
// The range may extend past Len into spare capacity, but not past Cap.
func (b *AlignedBuffer) Range(off, n int) (data []byte, ok bool) {
	if n == 0 {
		return nil, true
	}
	b.each(off, n, func(piece []byte) bool {
		data = piece
		return false
	})
	return data, len(data) == n
}

// Slices appends to dst the pieces of [off, off+n), one per chunk it spans,
// and returns the extended slice. The range may extend past Len into spare
// capacity, but not past Cap.
func (b *AlignedBuffer) Slices(off, n int, dst [][]byte) [][]byte {
	b.each(off, n, func(piece []byte) bool {
		dst = append(dst, piece)
		return true
	})
	return dst
}

// each calls fn for the pieces of [off, off+n), stopping when fn returns false.
func (b *AlignedBuffer) each(off, n int, fn func([]byte) bool) {
	if off < 0 || n < 0 || off+n > b.cap {
		panic(fmt.Sprintf("mempool: range [%d, %d) outside capacity %d", off, off+n, b.cap))
	}
	for _, chunk := range b.chunks {
		if n == 0 {
			return
		}
		if off >= len(chunk) {
			off -= len(chunk)
			continue
		}
		end := min(len(chunk), off+n)
		piece := chunk[off:end:end]
		off = 0
		n -= len(piece)
		if !fn(piece) {
			return
		}
	}
}

// Write appends p, growing the buffer as needed. It implements io.Writer.
func (b *AlignedBuffer) Write(p []byte) (int, error) {
	err := b.Grow(len(p))
	p = p[:min(len(p), b.cap-b.n)]
	written := 0
	b.each(b.n, len(p), func(piece []byte) bool {
		written += copy(piece, p[written:])
		return true
	})
	b.n += written
	return written, err
}

// ReadFrom reads from r until io.EOF, directly into the buffer's spare
// capacity, growing it as needed. It implements io.ReaderFrom, so io.Copy into
// an AlignedBuffer uses no intermediate buffer.
func (b *AlignedBuffer) ReadFrom(r io.Reader) (int64, error) {
	var total int64
	for {
		if b.n == b.cap {
			if err := b.Grow(1); err != nil {
				// Out of memory. A source that ends exactly at capacity has
				// still been read completely; a one-byte probe tells.
				var probe [1]byte
				switch m, perr := io.ReadFull(r, probe[:]); {
				case m == 0 && perr == io.EOF:
					return total, nil
				case m == 0:
					return total, perr
				default:
					return total, err
				}
			}
		}
		var spare []byte
		b.each(b.n, b.cap-b.n, func(piece []byte) bool {
			spare = piece
			return false
		})
		m, err := r.Read(spare)
		b.n += m
		total += int64(m)
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}

// WriteTo writes the data to w. It implements io.WriterTo.
func (b *AlignedBuffer) WriteTo(w io.Writer) (int64, error) {
	var total int64
	var err error
	b.each(0, b.n, func(piece []byte) bool {
		var m int
		m, err = w.Write(piece)
		total += int64(m)
		return err == nil
	})
	return total, err
}

// Release returns every chunk to the allocator. The buffer is empty
// afterwards and may be reused, allocating again as it grows.
func (b *AlignedBuffer) Release() {
	for _, chunk := range b.chunks {
		b.alloc.Free(chunk)
	}
	clear(b.chunks)
	b.chunks = b.chunks[:0]
	b.cap, b.n = 0, 0
}
