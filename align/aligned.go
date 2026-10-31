// Package align allocates page-aligned memory and performs range arithmetic
// for direct I/O.
//
// O_DIRECT operations (Linux) generally require
// the buffer address, transfer size, and file offset to be aligned to the
// filesystem block size — 4 KiB on most systems.
//
// Memory returned by [AllocAligned] has an aligned address and length.
// Use [FreeAligned] to release it; the buffer is not managed by the Go GC.
package align

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Block alignment constants for O_DIRECT I/O.
const (
	// BlockSize is the required alignment boundary (4 KiB) for O_DIRECT I/O.
	BlockSize = 4096
	// BlockMask is the AND-complement mask for fast alignment arithmetic.
	BlockMask = BlockSize - 1
)

// PageAlign rounds size up to the nearest [BlockSize] boundary.
func PageAlign(size int64) int64 {
	return (size + BlockMask) &^ BlockMask
}

// WriteBuffers returns the leading chunks covering n bytes rounded up to a
// whole page, for one direct-I/O write. Bytes from n to the page boundary are
// zeroed. The chunks must hold at least PageAlign(n) bytes.
func WriteBuffers(chunks [][]byte, n int64) [][]byte {
	end := PageAlign(n)
	out := make([][]byte, 0, len(chunks))
	for start := int64(0); start < end; {
		chunk := chunks[len(out)]
		chunk = chunk[:min(int64(len(chunk)), end-start)]
		if start+int64(len(chunk)) > n {
			clear(chunk[max(0, n-start):])
		}
		out = append(out, chunk)
		start += int64(len(chunk))
	}
	return out
}

// IsAligned reports whether the first byte of block is aligned to [BlockSize].
// An empty or nil slice is considered aligned.
func IsAligned(block []byte) bool {
	if len(block) == 0 {
		return true
	}
	return uintptr(unsafe.Pointer(&block[0]))&uintptr(BlockMask) == 0
}

// AlignRange computes O_DIRECT-compliant parameters for a byte range.
//
// It returns:
//   - alignedOffset: offset rounded DOWN to a [BlockSize] boundary.
//   - alignedLength: the smallest [BlockSize]-multiple covering [offset, offset+length).
//
// The leading bytes to skip inside the returned region: offset - alignedOffset.
func AlignRange(offset int64, length int) (alignedOffset, alignedLength int64) {
	alignedOffset = offset &^ BlockMask
	alignedEnd := PageAlign(offset + int64(length))
	return alignedOffset, alignedEnd - alignedOffset
}

// AllocAligned allocates a page-aligned byte slice backed by anonymous mmap.
//
// The returned slice length is rounded up to the nearest [BlockSize] page.
//
// On Linux, if the aligned size is an exact multiple of [HugepageSize] (2 MiB),
// AllocAligned first attempts MAP_HUGETLB, which draws on the hugetlb pool an
// administrator reserves (vm.nr_hugepages). Without free pool pages, it maps
// standard pages and advises transparent huge pages (MADV_HUGEPAGE) instead.
//
// WARNING: The buffer is NOT managed by the Go GC. The caller MUST call
// [FreeAligned] when done to avoid a memory leak. Invalid sizes and mmap
// failures panic.
func AllocAligned(size int64) []byte {
	alignedSize := int(PageAlign(size))
	flags := unix.MAP_ANON | unix.MAP_PRIVATE
	if HugepageSupported && alignedSize%HugepageSize == 0 {
		flags |= hugePageMmapExtraFlags
	}
	data, err := unix.Mmap(-1, 0, alignedSize, unix.PROT_READ|unix.PROT_WRITE, flags)
	if err != nil && flags&hugePageMmapExtraFlags != 0 {
		data, err = unix.Mmap(-1, 0, alignedSize, unix.PROT_READ|unix.PROT_WRITE, flags&^hugePageMmapExtraFlags)
		if err == nil {
			adviseHugepages(data)
		}
	}
	if err != nil {
		panic(fmt.Errorf("align: mmap(%d bytes): %w", size, err))
	}
	return data
}

// Warmup commits every page of page-aligned data before use: one
// madvise(MADV_POPULATE_WRITE) on Linux 5.14 and later, otherwise a write
// every stride bytes, which should be the mapping's page size.
func Warmup(data []byte, stride int) {
	if populate(data) {
		return
	}
	for i := 0; i < len(data); i += stride {
		data[i] = 0
	}
}

// FreeAligned releases memory allocated by [AllocAligned].
// You MUST pass the original allocation obtained from AllocAligned.
func FreeAligned(buf []byte) {
	if err := unix.Munmap(buf[:cap(buf)]); err != nil {
		panic(fmt.Errorf("align: munmap failed: %w", err))
	}
}

// GrowAligned grows an aligned buffer to at least newSize bytes.
// newSize is rounded up to the nearest [BlockSize] page before the capacity
// check, so the returned slice is always page-aligned in both address and
// length. If cap(buf) ≥ alignedSize the existing buffer is returned
// resliced; otherwise a new aligned buffer is allocated, the contents copied,
// and the old one freed. Allocation failure panics and leaves buf intact.
// buf must satisfy the same ownership requirements as [FreeAligned].
func GrowAligned(buf []byte, newSize int64) []byte {
	alignedSize := int(PageAlign(newSize))
	if cap(buf) >= alignedSize {
		return buf[:alignedSize]
	}
	data := AllocAligned(newSize)
	copy(data, buf)
	FreeAligned(buf)
	return data
}
