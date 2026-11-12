package alloc

import (
	"io"
	"sync/atomic"

	"github.com/miretskiy/dio/v2/align"
)

// Buffer is a byte stream backed by allocator memory, in the spirit of
// bytes.Buffer. Writes acquire Memory as needed without waiting; existing bytes
// never move. Once every byte has been read, the buffer reuses its memory from
// the start. Buffer is not safe for concurrent use.
//
// Buffer owns one reference to each acquired Memory until Release. References
// held elsewhere protect lifetime, not contents: finish reading shared bytes
// before the buffer drains, resets, and writes over them.
// Growth requires exclusive ownership of the allocations it merges; do not
// grow the buffer while its last allocation is pinned elsewhere.
type Buffer struct {
	allocator *Allocator
	mem       []Memory
	capacity  int // sum of the actual Memory lengths
	read      bufferCursor
	write     bufferCursor
}

// bufferCursor locates an offset without rescanning earlier chunks. Offsets
// advance monotonically between resets; base is the byte offset of mem[index].
type bufferCursor struct {
	offset int // byte position in the stream
	index  int // last located allocation
	base   int // byte position of that allocation
}

// at returns the rest of the chunk containing offset. The caller ensures
// offset is within the held memory and does not move backward.
func (b *Buffer) at(c *bufferCursor) []byte {
	data := b.mem[c.index].data
	for c.offset >= c.base+len(data) {
		c.base += len(data)
		c.index++
		data = b.mem[c.index].data
	}
	return data[c.offset-c.base:]
}

// NewAllocatingBuffer returns a buffer that joins raw allocator claims before
// creating Memory leases. Grow and Write acquire their full requested capacity.
func NewAllocatingBuffer(a *Allocator) *Buffer {
	return &Buffer{allocator: a}
}

// Len returns the number of unread bytes.
func (b *Buffer) Len() int { return b.write.offset - b.read.offset }

// Cap returns the bytes held by the buffer, read or not.
func (b *Buffer) Cap() int { return b.capacity }

// Reset discards all contents and retains allocations for reuse.
func (b *Buffer) Reset() {
	b.read, b.write = bufferCursor{}, bufferCursor{}
}

// RawAllocations returns the slice of underlying allocations in this buffer.
// The caller assumes responsibility for managing the safety of the returned allocations.
// The caller may grab a reference to the underlying Memory if it needs to store a copy.
// Growth may merge allocations and invalidate unpinned copies of their handles.
func (b *Buffer) RawAllocations() []Memory {
	return b.mem[:len(b.mem):len(b.mem)]
}

// Release discards all contents and releases the buffer's reference to each
// allocation. The buffer remains usable and acquires memory again on write.
func (b *Buffer) Release() {
	for _, m := range b.mem {
		m.Unpin()
	}
	clear(b.mem)
	b.mem = b.mem[:0]
	b.capacity = 0
	b.Reset()
}

// TakeAllocations transfers the allocation slice and its owned references to the
// caller, leaving b empty. It neither copies handles nor increments references.
// The caller must eventually Unpin each allocation. Existing borrowed views of
// this slice remain valid; unlike Release, taking it does not clear the handles.
func (b *Buffer) TakeAllocations() []Memory {
	memory := b.mem
	b.mem = nil
	b.capacity = 0
	b.Reset()
	return memory
}

// Trim releases unused capacity after the written bytes, rounded up to a page,
// and clears the padding between the written end and that page boundary.
// Contents and read/write positions are preserved. Call before sharing the
// buffer or submitting I/O: shrinking a lease requires exclusive ownership and
// invalidates unpinned copies of that handle, just like growth.
func (b *Buffer) Trim() {
	size := int(align.PageAlign(int64(b.write.offset)))
	if size == 0 {
		b.Release()
		return
	}
	remaining := size
	for i := range b.mem {
		if n := len(b.mem[i].data); remaining >= n {
			remaining -= n
			continue
		}
		if remaining != 0 {
			b.mem[i].trim(remaining)
			i++
		}
		for _, m := range b.mem[i:] {
			m.Unpin()
		}
		clear(b.mem[i:])
		b.mem = b.mem[:i]
		b.capacity = size
		break
	}
	last := b.mem[len(b.mem)-1].data
	clear(last[len(last)-(size-b.write.offset):])
}

// appendRaw consumes a raw range, extending the last lease or publishing a new
// one. Only this boundary between raw storage and Buffer ownership needs leases.
// TrimPrefix removes and releases n bytes from the beginning before any reads.
// n must be page-aligned and no greater than the written length. Remaining
// bytes do not move. Call before sharing allocations; every removed or resized
// lease requires exclusive ownership and invalidates old copies of its handle.
func (b *Buffer) TrimPrefix(n int) {
	if n < 0 || n%align.BlockSize != 0 || n > b.write.offset || b.read.offset != 0 {
		panic("alloc: invalid prefix trim")
	}
	if n == 0 {
		return
	}
	left, removed := n, 0
	for left > 0 {
		m := &b.mem[removed]
		if left < len(m.data) {
			m.trimPrefix(left)
			break
		}
		left -= len(m.data)
		m.Unpin()
		removed++
	}
	kept := copy(b.mem, b.mem[removed:])
	clear(b.mem[kept:])
	b.mem = b.mem[:kept]
	b.capacity -= n
	b.write = bufferCursor{offset: b.write.offset - n}
}

func (b *Buffer) appendRaw(m rawMemory) {
	if last := len(b.mem) - 1; last < 0 || !b.mem[last].extend(m) {
		b.mem = append(b.mem, m.lease(b.allocator.arena))
	}
	b.capacity += m.n * align.BlockSize
}

// acquire adds one raw claim when ReadFrom does not know the remaining demand.
func (b *Buffer) acquire(size int) error {
	m, err := b.allocator.acquireRaw(int64(min(size, MaxAllocationSize)))
	if err != nil {
		return err
	}
	b.appendRaw(m)
	return nil
}

// Grow ensures space for n more bytes without waiting for allocator capacity.
// Memory acquired before an error is retained.
func (b *Buffer) Grow(n int) error {
	available := b.capacity - b.write.offset
	if n <= available {
		return nil
	}
	remaining := n - available
	a := b.allocator
	batch := a.arena.startBatch()
	var pending rawMemory
	var err error
	for remaining > 0 {
		requested := int64(min(remaining, MaxAllocationSize))
		claimed, ok := batch.claim(a.arena, allocationPages(requested))
		if !ok {
			err = ErrExhausted
			break
		}
		m := rawMemory(claimed)
		a.prepareRaw(m, requested)
		if pending.n == 0 {
			pending = m
		} else if !pending.join(m) {
			b.appendRaw(pending)
			pending = m
		}
		remaining -= m.n * align.BlockSize
	}
	if pending.n != 0 {
		b.appendRaw(pending)
	}
	batch.save()
	return err
}

// free returns the unwritten space in the Memory at the write offset.
// The caller ensures there is unwritten capacity.
func (b *Buffer) free() []byte {
	return b.at(&b.write)
}

// unread returns the unread bytes in the Memory at the read offset.
// The caller ensures there are unread bytes.
func (b *Buffer) unread() []byte {
	data := b.at(&b.read)
	return data[:min(len(data), b.Len())]
}

// Write appends p. An allocation error writes nothing.
func (b *Buffer) Write(p []byte) (int, error) {
	if err := b.Grow(len(p)); err != nil {
		return 0, err
	}
	for n := 0; n < len(p); {
		c := copy(b.free(), p[n:])
		b.write.offset += c
		n += c
	}
	return len(p), nil
}

// Read returns io.EOF only when the buffer is empty and p is not.
func (b *Buffer) Read(p []byte) (int, error) {
	if b.read.offset == b.write.offset && len(p) > 0 {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && b.read.offset < b.write.offset {
		c := copy(p[n:], b.unread())
		b.read.offset += c
		n += c
	}
	if b.read.offset == b.write.offset {
		b.Reset()
	}
	return n, nil
}

// ReadFrom reads from r directly into allocator memory until EOF or an error.
// Bytes read before a source or allocation error remain in the buffer.
func (b *Buffer) ReadFrom(r io.Reader) (int64, error) {
	var n int64
	for {
		if b.write.offset == b.Cap() {
			// The stream's remaining length is unknown; use the request limit.
			if err := b.acquire(MaxAllocationSize); err != nil {
				return n, err
			}
		}
		c, err := r.Read(b.free())
		b.write.offset += c
		n += int64(c)
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
	}
}

// WriteTo writes unread bytes to w, consuming only the bytes w accepts.
func (b *Buffer) WriteTo(w io.Writer) (int64, error) {
	var n int64
	for b.read.offset < b.write.offset {
		p := b.unread()
		c, err := w.Write(p)
		b.read.offset += c
		n += int64(c)
		if err != nil {
			return n, err
		}
		if c != len(p) {
			return n, io.ErrShortWrite
		}
	}
	b.Reset()
	return n, nil
}

var (
	_ io.Reader     = (*Buffer)(nil)
	_ io.Writer     = (*Buffer)(nil)
	_ io.ReaderFrom = (*Buffer)(nil)
	_ io.WriterTo   = (*Buffer)(nil)
)

// ReadOnlyBuffer owns a frozen buffer. Retain acquires one reference to the
// whole buffer; Release frees its allocations when the last owner leaves.
// Borrowed slices must not be modified and remain valid while a reference is held.
type ReadOnlyBuffer struct {
	buffer   *Buffer
	capacity int
	refs     atomic.Int64
}

// Freeze transfers ownership of b's allocations. Do not write, grow, trim or
// release b afterward. Outstanding I/O may initialize the allocations while
// holding a reference; wait for it before reading views. After initialization,
// contents and allocation boundaries stay fixed.
func (b *Buffer) Freeze() *ReadOnlyBuffer {
	r := &ReadOnlyBuffer{buffer: b, capacity: b.Cap()}
	r.refs.Store(1)
	return r
}

func (b *ReadOnlyBuffer) Cap() int { return b.capacity }

// Retain requires an existing reference and returns another independently owned reference.
func (b *ReadOnlyBuffer) Retain() *ReadOnlyBuffer {
	b.refs.Add(1)
	return b
}

// Release returns the number of allocation bytes actually freed.
func (b *ReadOnlyBuffer) Release() (freed int64) {
	if b.refs.Add(-1) != 0 {
		return 0
	}
	for _, m := range b.buffer.mem {
		size := len(m.data)
		if m.Unpin() {
			freed += int64(size)
		}
	}
	clear(b.buffer.mem)
	b.buffer.mem = nil
	b.buffer.capacity = 0
	b.buffer.Reset()
	return freed
}

// First returns a borrowed view of the first allocation, or nil for an empty buffer.
func (b *ReadOnlyBuffer) First() []byte {
	if len(b.buffer.mem) == 0 {
		return nil
	}
	p := b.buffer.mem[0].data
	return p[:len(p):len(p)]
}

// View creates an independent cursor over a byte range within Cap(). It borrows
// b's reference. The caller must initialize the range before reading it; this
// permits views of buffers filled directly by I/O rather than Buffer.Write.
func (b *ReadOnlyBuffer) View(offset, length int) BufferView {
	if offset < 0 || length < 0 || offset > b.capacity-length {
		panic("alloc: invalid buffer view")
	}
	return BufferView{buffer: b.buffer, cursor: bufferCursor{offset: offset}, end: offset + length, size: length}
}

// BufferView is a read-only cursor. Copies have independent positions and borrow
// the same storage. Keep its ReadOnlyBuffer alive until all views are finished.
type BufferView struct {
	buffer *Buffer
	cursor bufferCursor
	end    int
	size   int
}

func (v *BufferView) Size() int64 { return int64(v.size) }
func (v *BufferView) Len() int    { return v.end - v.cursor.offset }

func (v *BufferView) readable() []byte {
	p := v.buffer.at(&v.cursor)
	n := min(len(p), v.Len())
	return p[:n:n]
}

// Next consumes the next contiguous chunk without copying. A returned slice
// remains valid until the owning ReadOnlyBuffer is released, including across
// subsequent Next or Read calls. Exhaustion returns io.EOF and no bytes.
func (v *BufferView) Next() ([]byte, error) {
	if v.Len() == 0 {
		return nil, io.EOF
	}
	p := v.readable()
	v.cursor.offset += len(p)
	return p, nil
}

func (v *BufferView) Read(p []byte) (int, error) {
	if len(p) > 0 && v.Len() == 0 {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && v.Len() > 0 {
		size := copy(p[n:], v.readable())
		v.cursor.offset += size
		n += size
	}
	return n, nil
}

func (v *BufferView) WriteTo(w io.Writer) (int64, error) {
	var total int64
	for v.Len() > 0 {
		p := v.readable()
		n, err := w.Write(p)
		v.cursor.offset += n
		total += int64(n)
		if err != nil {
			return total, err
		}
		if n != len(p) {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}
