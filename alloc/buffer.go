package alloc

import "io"

// Buffer is a byte stream backed by allocator memory, in the spirit of
// bytes.Buffer. Writes acquire Memory as needed without waiting; existing bytes
// never move. Once every byte has been read, the buffer reuses its memory from
// the start. Buffer is not safe for concurrent use.
//
// Buffer owns one reference to each acquired Memory until Release. References
// held elsewhere protect lifetime, not contents: finish reading shared bytes
// before the buffer drains, resets, and writes over them.
type Buffer struct {
	allocator Allocator
	mem       []Memory
	unit      int // bytes per Memory; uniform for one allocator
	r, w      int // read and write offsets across mem
}

// NewAllocatingBuffer returns a buffer using specified allocator for its allocations.
func NewAllocatingBuffer(a Allocator) *Buffer {
	return &Buffer{allocator: a}
}

// Len returns the number of unread bytes.
func (b *Buffer) Len() int { return b.w - b.r }

// Cap returns the bytes held by the buffer, read or not.
func (b *Buffer) Cap() int { return len(b.mem) * b.unit }

// Reset discards all contents and retains allocations for reuse.
func (b *Buffer) Reset() { b.r, b.w = 0, 0 }

// RawAllocations returns the slice of underlying allocations in this buffer.
// The caller assumes responsibility for managing the safety of the returned allocations.
// The caller may grab a reference to the underlying Memory if it needs to store a copy.
func (b *Buffer) RawAllocations() []Memory {
	return b.mem
}

// Release discards all contents and releases the buffer's reference to each
// allocation. The buffer remains usable and acquires memory again on write.
func (b *Buffer) Release() {
	for _, m := range b.mem {
		m.Unpin()
	}
	clear(b.mem)
	b.mem = b.mem[:0]
	b.Reset()
}

// Grow ensures space for n more bytes without waiting for allocator capacity.
// Memory acquired before an error is retained.
func (b *Buffer) Grow(n int) error {
	for b.Cap()-b.w < n {
		m, err := b.allocator.TryAcquire()
		if err != nil {
			return err
		}
		b.mem = append(b.mem, m)
		b.unit = len(m.data)
	}
	return nil
}

// free returns the unwritten space in the Memory at the write offset.
// The caller ensures w < Cap().
func (b *Buffer) free() []byte {
	return b.mem[b.w/b.unit].data[b.w%b.unit:]
}

// unread returns the unread bytes in the Memory at the read offset.
// The caller ensures r < w.
func (b *Buffer) unread() []byte {
	i := b.r / b.unit
	return b.mem[i].data[b.r%b.unit : min(b.unit, b.w-i*b.unit)]
}

// Write appends p. An allocation error writes nothing.
func (b *Buffer) Write(p []byte) (int, error) {
	if err := b.Grow(len(p)); err != nil {
		return 0, err
	}
	for n := 0; n < len(p); {
		c := copy(b.free(), p[n:])
		b.w += c
		n += c
	}
	return len(p), nil
}

// Read returns io.EOF only when the buffer is empty and p is not.
func (b *Buffer) Read(p []byte) (int, error) {
	if b.r == b.w && len(p) > 0 {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && b.r < b.w {
		c := copy(p[n:], b.unread())
		b.r += c
		n += c
	}
	if b.r == b.w {
		b.Reset()
	}
	return n, nil
}

// ReadFrom reads from r directly into allocator memory until EOF or an error.
// Bytes read before a source or allocation error remain in the buffer.
func (b *Buffer) ReadFrom(r io.Reader) (int64, error) {
	var n int64
	for {
		if err := b.Grow(1); err != nil {
			return n, err
		}
		c, err := r.Read(b.free())
		b.w += c
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
	for b.r < b.w {
		p := b.unread()
		c, err := w.Write(p)
		b.r += c
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
