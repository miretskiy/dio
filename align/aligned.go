// Package align provides page-aligned memory primitives for O_DIRECT I/O.
//
// All O_DIRECT operations (Linux) and F_NOCACHE operations (Darwin) require
// the buffer address, transfer size, and file offset to be aligned to the
// filesystem block size — 4 KiB on most systems.
//
// Memory returned by [AllocAligned] satisfies all three constraints.
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
// WARNING: The buffer is NOT managed by the Go GC. The caller MUST call
// [FreeAligned] when done to avoid a memory leak.
func AllocAligned(size int) []byte {
	alignedSize := int(PageAlign(int64(size)))
	data, err := unix.Mmap(
		-1, 0, alignedSize,
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_ANON|unix.MAP_PRIVATE,
	)
	if err != nil {
		panic(fmt.Sprintf("align: mmap(%d bytes): %v", size, err))
	}
	// Pre-warm: touch each page to commit physical RAM.
	for i := 0; i < len(data); i += BlockSize {
		data[i] = 0
	}
	return data
}

// FreeAligned releases memory allocated by [AllocAligned].
// The slice must not be used after this call.
func FreeAligned(buf []byte) {
	if len(buf) == 0 {
		return
	}
	alignedSize := int(PageAlign(int64(cap(buf))))
	_ = unix.Munmap(buf[:alignedSize])
}

// GrowAligned grows an aligned buffer to at least newSize bytes.
// newSize is rounded up to the nearest [BlockSize] page before the capacity
// check, so the returned slice is always page-aligned in both address and
// length. If cap(buf) ≥ alignedSize the existing buffer is returned
// resliced; otherwise a new aligned buffer is allocated and the old one freed.
func GrowAligned(buf []byte, newSize int) []byte {
	alignedSize := int(PageAlign(int64(newSize)))
	if cap(buf) >= alignedSize {
		return buf[:alignedSize]
	}
	if len(buf) > 0 {
		FreeAligned(buf)
	}
	return AllocAligned(newSize)
}
