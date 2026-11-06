package alloc

import (
	"io"

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

// appendRaw consumes a raw range, extending the last lease or publishing a new
// one. Only this boundary between raw storage and Buffer ownership needs leases.
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
