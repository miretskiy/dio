package mempool

import (
	"bytes"
	"io"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
)

const testChunk = 32 << 10

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/251)
	}
	return b
}

func contents(t *testing.T, b *AlignedBuffer) []byte {
	t.Helper()
	var out bytes.Buffer
	_, err := b.WriteTo(&out)
	require.NoError(t, err)
	return append([]byte{}, out.Bytes()...)
}

// allocators returns each Allocator under test, with a check that all of its
// memory has been returned.
func allocators(t *testing.T) map[string]struct {
	alloc   Allocator
	drained func() bool
} {
	slab, err := NewSlabPool(align.HugepageSize, testChunk)
	require.NoError(t, err)
	mmap := NewMmapPool("test", testChunk, 64)
	t.Cleanup(func() {
		slab.Close()
		mmap.Close()
	})
	return map[string]struct {
		alloc   Allocator
		drained func() bool
	}{
		"default": {nil, func() bool { return true }},
		"slab": {slab, func() bool {
			for i := range slab.activeShards {
				if slab.shards[i].mask.Load() != 0 {
					return false
				}
			}
			return true
		}},
		"mmap": {mmap, func() bool { return mmap.Outstanding() == 0 }},
	}
}

func TestAlignedBufferAllocators(t *testing.T) {
	for name, a := range allocators(t) {
		t.Run(name, func(t *testing.T) {
			for _, size := range []int{0, 1, testChunk - 1, testChunk, testChunk + 1, 7*testChunk + 13} {
				b := NewAlignedBuffer(a.alloc, size)
				data := pattern(size)
				// Write in odd-sized pieces so writes straddle chunk boundaries.
				for off := 0; off < size; off += 1000 {
					n, err := b.Write(data[off:min(size, off+1000)])
					require.NoError(t, err)
					require.Equal(t, min(1000, size-off), n)
				}
				require.Equal(t, size, b.Len())
				require.Equal(t, data, contents(t, b), "size %d", size)
				for _, piece := range b.Slices(0, b.Cap(), nil) {
					require.True(t, align.IsAligned(piece))
					require.Zero(t, len(piece)%align.BlockSize)
				}
				b.Release()
				require.True(t, a.drained(), "size %d: memory not returned", size)
			}
		})
	}
}

func TestAlignedBufferHintSizesFirstChunk(t *testing.T) {
	// Without a pool, the hint is honored as one contiguous allocation.
	b := NewAlignedBuffer(nil, 5*align.BlockSize+1)
	require.Zero(t, b.Cap(), "construction allocates nothing")
	_, err := b.Write([]byte{1})
	require.NoError(t, err)
	require.Equal(t, 6*align.BlockSize, b.Cap())
	require.Len(t, b.Slices(0, b.Cap(), nil), 1)
	b.Release()

	// A pool trims the hint to what it can supply contiguously.
	slab, err := NewSlabPool(align.HugepageSize, testChunk)
	require.NoError(t, err)
	defer slab.Close()
	b = NewAlignedBuffer(slab, 10*testChunk)
	require.NoError(t, b.Grow(1))
	require.Equal(t, testChunk, b.Cap())
	require.NoError(t, b.Grow(10*testChunk)) // room for 10 chunks past Len
	require.Equal(t, 10*testChunk, b.Cap())
	b.Release()
}

func TestAlignedBufferGrowDoublesWithoutPool(t *testing.T) {
	b := NewAlignedBuffer(nil, 0)
	defer b.Release()
	for range 100 {
		_, err := b.Write(pattern(100))
		require.NoError(t, err)
	}
	require.LessOrEqual(t, len(b.Slices(0, b.Cap(), nil)), 3, "small writes must not mean many chunks")
}

func TestAlignedBufferReadFrom(t *testing.T) {
	for name, a := range allocators(t) {
		t.Run(name, func(t *testing.T) {
			for _, size := range []int{0, 1, testChunk, 9*testChunk + 17} {
				b := NewAlignedBuffer(a.alloc, 0)
				data := pattern(size)
				// A plain io.Reader, read one byte at a time, so io.Copy
				// must use ReadFrom and every boundary is crossed.
				n, err := io.Copy(b, &oneByteReader{r: bytes.NewReader(data)})
				require.NoError(t, err)
				require.Equal(t, int64(size), n)
				require.Equal(t, data, contents(t, b))
				b.Release()
				require.True(t, a.drained())
			}
		})
	}
}

func TestAlignedBufferExhaustedSlab(t *testing.T) {
	slab, err := NewSlabPool(align.HugepageSize, align.HugepageSize/4) // 4 slots
	require.NoError(t, err)
	defer slab.Close()
	slot := slab.SlotSize()

	// A source that ends exactly at capacity reads completely.
	b := NewAlignedBuffer(slab, 0)
	n, err := b.ReadFrom(bytes.NewReader(pattern(4 * slot)))
	require.NoError(t, err)
	require.Equal(t, int64(4*slot), n)
	b.Release()

	// One byte more does not fit: the allocator's error surfaces.
	b = NewAlignedBuffer(slab, 0)
	_, err = b.ReadFrom(bytes.NewReader(pattern(4*slot + 1)))
	require.ErrorIs(t, err, ErrSlabExhausted)
	b.Reset()
	n2, err := b.Write(pattern(4*slot + 1))
	require.ErrorIs(t, err, ErrSlabExhausted)
	require.Equal(t, 4*slot, n2, "Write stores what fits")
	b.Release()
}

func TestAlignedBufferRangeAndSpareCapacity(t *testing.T) {
	slab, err := NewSlabPool(align.HugepageSize, testChunk)
	require.NoError(t, err)
	defer slab.Close()
	b := NewAlignedBuffer(slab, 0)
	defer b.Release()
	require.NoError(t, b.Grow(2*testChunk))
	_, err = b.Write([]byte("hello"))
	require.NoError(t, err)

	// Spare capacity is addressable without changing Len.
	spare, ok := b.Range(b.Len(), 5)
	require.True(t, ok)
	copy(spare, "world")
	require.Equal(t, 5, b.Len())
	b.SetLen(10)
	require.Equal(t, []byte("helloworld"), contents(t, b))
	data, ok := b.Range(0, b.Len())
	require.True(t, ok)
	require.Equal(t, []byte("helloworld"), data)

	_, ok = b.Range(testChunk-1, 2)
	require.False(t, ok, "a range crossing chunks is not one slice")
	require.Len(t, b.Slices(testChunk-1, 2, nil), 2)
	whole, ok := b.Range(testChunk, testChunk)
	require.True(t, ok)
	require.Len(t, whole, testChunk)
	require.Equal(t, testChunk, cap(whole), "a piece's capacity ends at its chunk")
}

func TestSlabPoolFreeRejectsForeignMemory(t *testing.T) {
	slab, err := NewSlabPool(align.HugepageSize, testChunk)
	require.NoError(t, err)
	defer slab.Close()
	mem, err := slab.Alloc(1)
	require.NoError(t, err)
	require.Panics(t, func() { slab.Free(mem[align.BlockSize:]) }, "not the start of a slot")
	require.Panics(t, func() { slab.Free(make([]byte, testChunk)) }, "not from this pool")
	slab.Free(mem)
	_, err = slab.Alloc(testChunk + 1)
	require.Error(t, err)
}

type oneByteReader struct{ r io.Reader }

func (o *oneByteReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return o.r.Read(p[:1])
}

func TestAlignedBufferReuseAfterRelease(t *testing.T) {
	mmap := NewMmapPool("reuse", testChunk, 4)
	defer mmap.Close()
	b := NewAlignedBuffer(mmap, 0)
	_, err := b.Write(pattern(3 * testChunk))
	require.NoError(t, err)
	require.Equal(t, int64(3), mmap.Outstanding())
	b.Release()
	require.Zero(t, mmap.Outstanding())
	require.Zero(t, b.Len())
	require.Zero(t, b.Cap())

	data := pattern(testChunk + 5)
	_, err = b.Write(data)
	require.NoError(t, err)
	require.Equal(t, data, contents(t, b))
	b.Release()
	require.Zero(t, mmap.Outstanding())
}
