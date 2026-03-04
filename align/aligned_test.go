package align

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ─── PageAlign ───────────────────────────────────────────────────────────────

func TestPageAlign(t *testing.T) {
	tests := []struct {
		in, want int64
	}{
		{0, 0},
		{1, 4096},
		{4095, 4096},
		{4096, 4096},
		{4097, 8192},
		{8192, 8192},
		{12345, 16384},
		{1 << 20, 1 << 20},
		{(1 << 20) + 1, (1 << 20) + 4096},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, PageAlign(tt.in), "PageAlign(%d)", tt.in)
	}
}

// ─── IsAligned ───────────────────────────────────────────────────────────────

func TestIsAligned_AllocatedBuffer(t *testing.T) {
	buf := AllocAligned(4096)
	defer FreeAligned(buf)

	require.True(t, IsAligned(buf), "AllocAligned should return aligned buffer")
	// Subslice from offset 0 is still aligned.
	require.True(t, IsAligned(buf[:100]), "buf[:100] should be aligned")
}

func TestIsAligned_Empty(t *testing.T) {
	require.True(t, IsAligned(nil), "nil should be considered aligned")
	require.True(t, IsAligned([]byte{}), "empty slice should be considered aligned")
}

func TestIsAligned_Unaligned(t *testing.T) {
	buf := AllocAligned(4096)
	defer FreeAligned(buf)

	// Any subslice not starting at a page boundary is unaligned.
	require.False(t, IsAligned(buf[1:]), "buf[1:] must not be aligned")
	require.False(t, IsAligned(buf[BlockSize-1:]), "buf[BlockSize-1:] must not be aligned")
}

// ─── AlignRange ──────────────────────────────────────────────────────────────

func TestAlignRange(t *testing.T) {
	tests := []struct {
		offset              int64
		length              int
		wantOff, wantLength int64
	}{
		// Already aligned.
		{0, 4096, 0, 4096},
		{4096, 4096, 4096, 4096},
		// Offset rounds down, length rounds up to cover the original range.
		{1, 4095, 0, 4096},
		{1, 4096, 0, 8192},
		{4097, 1, 4096, 4096},
		// Multi-block range.
		{100, 3*4096 + 200, 0, 4 * 4096},
	}
	for _, tt := range tests {
		gotOff, gotLen := AlignRange(tt.offset, tt.length)
		require.Equal(t, tt.wantOff, gotOff, "AlignRange(%d,%d) offset", tt.offset, tt.length)
		require.Equal(t, tt.wantLength, gotLen, "AlignRange(%d,%d) length", tt.offset, tt.length)
		// The returned region must fully contain the original range.
		require.LessOrEqual(t, gotOff, tt.offset)
		require.GreaterOrEqual(t, gotOff+gotLen, tt.offset+int64(tt.length))
		// Both values must be page-aligned.
		require.Zero(t, gotOff%BlockSize)
		require.Zero(t, gotLen%BlockSize)
	}
}

// ─── AllocAligned / FreeAligned ──────────────────────────────────────────────

func TestAllocAligned_SizeRoundsUp(t *testing.T) {
	tests := []struct {
		request, wantLen int
	}{
		{1, 4096},
		{100, 4096},
		{4096, 4096},
		{4097, 8192},
		{8192, 8192},
		{16384, 16384},
		{16385, 20480},
	}
	for _, tt := range tests {
		buf := AllocAligned(tt.request)
		require.Equal(t, tt.wantLen, len(buf),
			"AllocAligned(%d): want len %d", tt.request, tt.wantLen)
		require.True(t, IsAligned(buf),
			"AllocAligned(%d): buffer must be page-aligned", tt.request)
		FreeAligned(buf)
	}
}

func TestAllocAligned_DataIsWritable(t *testing.T) {
	buf := AllocAligned(8192)
	defer FreeAligned(buf)
	// Write a recognisable pattern and read it back.
	for i := range buf {
		buf[i] = byte(i % 251)
	}
	for i := range buf {
		require.Equal(t, byte(i%251), buf[i], "byte %d mismatch", i)
	}
}

func TestFreeAligned_EmptyIsNoOp(t *testing.T) {
	// Must not panic.
	FreeAligned(nil)
	FreeAligned([]byte{})
}

// ─── GrowAligned ─────────────────────────────────────────────────────────────

func TestGrowAligned_AlignsNewSize(t *testing.T) {
	// Before the fix, GrowAligned(buf, 5) with cap(buf)=4096 would return
	// buf[:5] — a 5-byte, non-O_DIRECT-safe slice.
	// After the fix, it must return a full page (4096 bytes).
	buf := AllocAligned(4096)
	result := GrowAligned(buf[:0], 5)
	defer FreeAligned(result)

	require.Equal(t, 4096, len(result), "must be rounded up to page size")
	require.True(t, IsAligned(result), "result must be page-aligned")
}

func TestGrowAligned_ReusesCapacity(t *testing.T) {
	// When existing capacity is sufficient, no new allocation occurs.
	big := AllocAligned(8192) // 8 KiB
	defer FreeAligned(big)

	// Ask for 1 page — fits in the 8 KiB buffer.
	result := GrowAligned(big[:0], 4096)
	require.Equal(t, 4096, len(result))
	require.True(t, IsAligned(result))
	// Must be the same backing array.
	require.Equal(t, &big[0], &result[0], "should reuse existing allocation")
}

func TestGrowAligned_ReusesCapacity_FullPage(t *testing.T) {
	// cap exactly equals alignedSize — must reuse.
	buf := AllocAligned(4096)
	result := GrowAligned(buf[:0], 4096)
	require.Equal(t, 4096, len(result))
	require.Equal(t, &buf[0], &result[0])
	FreeAligned(result) // same memory, free once
}

func TestGrowAligned_AllocatesNewWhenTooSmall(t *testing.T) {
	// Existing buffer is too small → free old, allocate new.
	old := AllocAligned(4096) // 4 KiB
	result := GrowAligned(old, 4097)
	defer FreeAligned(result)
	// old is now freed inside GrowAligned; do NOT access it.

	require.Equal(t, 8192, len(result), "must be rounded up to next page")
	require.True(t, IsAligned(result))
	// Backing array must differ from the freed one.
	// (We can't dereference old, but we can compare the addresses if
	// the allocator happened to return the same page — that's fine too.)
}

func TestGrowAligned_NilBuf(t *testing.T) {
	// A nil buf (len=0) should always allocate.
	result := GrowAligned(nil, 1)
	defer FreeAligned(result)
	require.Equal(t, 4096, len(result))
	require.True(t, IsAligned(result))
}

func TestGrowAligned_ExactPageMultiple(t *testing.T) {
	cases := []int{4096, 8192, 16384, 1 << 20}
	for _, n := range cases {
		buf := AllocAligned(n)
		result := GrowAligned(buf[:0], n)
		require.Equal(t, n, len(result), "GrowAligned(buf, %d)", n)
		require.True(t, IsAligned(result))
		FreeAligned(result)
	}
}
