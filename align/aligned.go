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
	"math"
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
// The returned slice length is rounded up to the nearest [BlockSize] page;
// callers needing exactly n bytes may reslice. Each page is pre-touched to
// commit physical RAM, avoiding first-access page faults during I/O.
//
// On Linux, if the aligned size is an exact multiple of [HugepageSize] (2 MiB),
// AllocAligned first attempts MAP_HUGETLB. If huge pages are unavailable
// (ENOMEM — no pages reserved — or EOPNOTSUPP), it falls back to standard
// 4 KiB pages transparently.
//
// WARNING: The buffer is NOT managed by the Go GC. The caller MUST call
// [FreeAligned] when done to avoid a memory leak. Invalid sizes and mmap
// failures panic.
func AllocAligned(size int64) []byte {
	if size <= 0 || size > int64(math.MaxInt)-BlockMask {
		panic(fmt.Errorf("align: invalid allocation size %d", size))
	}
	alignedSize := int(PageAlign(size))
	stdFlags := unix.MAP_ANON | unix.MAP_PRIVATE

	if HugepageSupported && alignedSize%HugepageSize == 0 {
		data, err := unix.Mmap(-1, 0, alignedSize,
			unix.PROT_READ|unix.PROT_WRITE,
			stdFlags|hugePageMmapExtraFlags)
		if err == nil {
			// Pre-warm with hugepage stride.
			for i := 0; i < len(data); i += HugepageSize {
				data[i] = 0
			}
			return data
		}
		// Hugepages unavailable; fall through to standard allocation below.
	}

	data, err := unix.Mmap(-1, 0, alignedSize,
		unix.PROT_READ|unix.PROT_WRITE,
		stdFlags)
	if err != nil {
		panic(fmt.Errorf("align: mmap(%d bytes): %w", size, err))
	}
	// Pre-warm: touch each page to commit physical RAM.
	for i := 0; i < len(data); i += BlockSize {
		data[i] = 0
	}
	return data
}

// FreeAligned releases memory allocated by [AllocAligned].
// Pass the original allocation, optionally resliced without changing its base
// or capacity (including buf[:0]). The slice must not be used after this call.
func FreeAligned(buf []byte) {
	if cap(buf) == 0 {
		return
	}
	_ = unix.Munmap(buf[:cap(buf)])
}

// GrowAligned grows an aligned buffer to at least newSize bytes.
// newSize is rounded up to the nearest [BlockSize] page before the capacity
// check, so the returned slice is always page-aligned in both address and
// length. If cap(buf) ≥ alignedSize the existing buffer is returned
// resliced; otherwise a new aligned buffer is allocated, the contents copied,
// and the old one freed. Allocation failure panics and leaves buf intact.
// buf must satisfy the same ownership requirements as [FreeAligned].
func GrowAligned(buf []byte, newSize int64) []byte {
	if newSize < 0 || newSize > int64(math.MaxInt)-BlockMask {
		panic("align: invalid growth size")
	}
	alignedSize := int(PageAlign(newSize))
	if cap(buf) >= alignedSize {
		return buf[:alignedSize]
	}
	data := AllocAligned(newSize)
	copy(data, buf)
	FreeAligned(buf)
	return data
}
