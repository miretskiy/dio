//go:build darwin

package sys

import (
	"os"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Platform capability constants.
const (
	// UseFadvise reports whether posix_fadvise is effective on this platform.
	// Darwin's F_RDAHEAD is less capable than Linux's posix_fadvise; callers
	// should skip fadvise-based optimizations when this is false.
	UseFadvise = false

	// RequiresAlignment reports whether O_DIRECT requires aligned buffers.
	// Darwin's F_NOCACHE does not enforce strict alignment like Linux O_DIRECT.
	RequiresAlignment = false

	// RequiresExplicitSync reports whether explicit sync calls are needed.
	// On Darwin, O_DSYNC/O_SYNC are unavailable at open time; callers must
	// invoke Fdatasync/f.Sync explicitly after writes.
	RequiresExplicitSync = true
)

// Fdatasync syncs file data to physical storage.
// Darwin has no fdatasync(2); we use F_FULLFSYNC which ensures data reaches
// the physical drive (not just the drive's write cache).
// Retries automatically on EINTR.
func Fdatasync(f *os.File) error {
	fd := f.Fd()
	var errno syscall.Errno
	for {
		_, _, errno = syscall.Syscall(syscall.SYS_FCNTL, fd, uintptr(syscall.F_FULLFSYNC), 0)
		if errno != syscall.EINTR {
			break
		}
	}
	runtime.KeepAlive(f)
	if errno != 0 {
		return errno
	}
	return nil
}

// Fallocate pre-allocates disk space for a file using F_PREALLOCATE via fcntl.
// Tries contiguous allocation first, falls back to non-contiguous.
// Explicitly truncates to size after allocation because F_PREALLOCATE only
// reserves disk blocks without updating the logical file size.
// Retries automatically on EINTR.
func Fallocate(f *os.File, size int64) error {
	fstore := syscall.Fstore_t{
		Flags:   syscall.F_ALLOCATECONTIG, // Prefer contiguous blocks.
		Posmode: syscall.F_PEOFPOSMODE,    // Allocate from current EOF.
		Offset:  0,
		Length:  size,
	}

	fd := f.Fd()
	var errno syscall.Errno

	// Try contiguous allocation.
	for {
		_, _, errno = syscall.Syscall(
			syscall.SYS_FCNTL, fd,
			uintptr(syscall.F_PREALLOCATE),
			uintptr(unsafe.Pointer(&fstore)),
		)
		if errno != syscall.EINTR {
			break
		}
	}

	// Fall back to non-contiguous if contiguous failed.
	if errno != 0 {
		fstore.Flags = syscall.F_ALLOCATEALL
		for {
			_, _, errno = syscall.Syscall(
				syscall.SYS_FCNTL, fd,
				uintptr(syscall.F_PREALLOCATE),
				uintptr(unsafe.Pointer(&fstore)),
			)
			if errno != syscall.EINTR {
				break
			}
		}
	}

	// Both fd-using loops are done; one KeepAlive covers both.
	runtime.KeepAlive(f)

	if errno != 0 {
		return errno
	}

	// F_PREALLOCATE reserves disk blocks but does not update logical file size.
	// Truncate explicitly to set the logical EOF.
	return f.Truncate(size)
}

// fpunchhole_t matches the C struct used by fcntl(F_PUNCHHOLE) on macOS.
type fpunchhole_t struct {
	FP_flags    uint32
	FP_reserved uint32 // Padding for 8-byte struct alignment.
	FP_offset   int64
	FP_length   int64
}

// PunchHole deallocates a byte range within a file using F_PUNCHHOLE.
// Requires APFS; returns (0, nil) silently on HFS+ (F_PUNCHHOLE is a no-op).
//
// The range is aligned inward to filesystem block boundaries. Returns the
// number of bytes reclaimed after alignment, or (0, nil) if the range is
// smaller than one block. Retries automatically on EINTR.
func PunchHole(f *os.File, offset, length int64) (int64, error) {
	alignedOffset, alignedLength, canPunch := AlignForHolePunch(offset, length)
	if !canPunch {
		return 0, nil
	}
	ph := fpunchhole_t{
		FP_flags:    0, // Must be 0.
		FP_reserved: 0, // Must be 0.
		FP_offset:   alignedOffset,
		FP_length:   alignedLength,
	}
	fd := f.Fd()
	var errno syscall.Errno
	for {
		_, _, errno = syscall.Syscall(
			syscall.SYS_FCNTL, fd,
			uintptr(unix.F_PUNCHHOLE),
			uintptr(unsafe.Pointer(&ph)),
		)
		if errno != syscall.EINTR {
			break
		}
	}
	runtime.KeepAlive(f)
	if errno != 0 {
		return 0, errno
	}
	return alignedLength, nil
}

// Fadvise advises the kernel of the expected access pattern for f.
// Darwin's fcntl-based equivalents are coarser than Linux posix_fadvise:
// FadvSequential enables F_RDAHEAD; FadvRandom/FadvDontNeed use F_NOCACHE.
// The offset and length parameters are ignored (Darwin applies hints fd-wide).
// Retries automatically on EINTR.
func Fadvise(f *os.File, _ Offset_t, _ int64, hint FadviseHint) error {
	var cmd int
	var arg uintptr
	switch hint {
	case FadvDontNeed:
		cmd = syscall.F_NOCACHE
		arg = 1 // 1 = bypass cache ON
	case FadvSequential:
		cmd = syscall.F_RDAHEAD
		arg = 1 // 1 = enable read-ahead
	case FadvRandom:
		cmd = syscall.F_RDAHEAD
		arg = 0 // 0 = disable read-ahead
	default:
		return nil
	}
	fd := f.Fd()
	var errno syscall.Errno
	for {
		_, _, errno = syscall.Syscall(syscall.SYS_FCNTL, fd, uintptr(cmd), arg)
		if errno != syscall.EINTR {
			break
		}
	}
	runtime.KeepAlive(f)
	if errno != 0 {
		return errno
	}
	return nil
}

// CreateDirect creates path for writing. Always uses O_CREATE|O_WRONLY|O_TRUNC.
// If FlDirectIO is set, applies F_NOCACHE via fcntl after open to bypass the
// page cache. FlDSync/FlSync are not supported at open time; callers must use
// SyncFile after writes.
func CreateDirect(path string, flags OpenFlag) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	if flags&FlDirectIO != 0 {
		if _, err := unix.FcntlInt(f.Fd(), unix.F_NOCACHE, 1); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	return f, nil
}

// OpenDirect opens an existing file for reading with optional F_NOCACHE.
func OpenDirect(path string, flags OpenFlag) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	if flags&FlDirectIO != 0 {
		if _, err := unix.FcntlInt(f.Fd(), unix.F_NOCACHE, 1); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	return f, nil
}

// CopyFileRange copies length bytes from srcFile at *srcOff to dstFile at *dstOff.
// Darwin has no copy_file_range(2); falls back to a portable ReadAt/WriteAt loop.
func CopyFileRange(srcFile, dstFile *os.File, srcOff, dstOff *int64, length int) (int, error) {
	return copyFileRangeEmulated(srcFile, dstFile, srcOff, dstOff, length)
}

// copyFileRangeEmulated copies data between files using ReadAt/WriteAt.
// Used as a fallback on platforms without kernel copy_file_range support.
func copyFileRangeEmulated(src, dst *os.File, srcOff, dstOff *int64, length int) (int, error) {
	const maxChunk = 1 << 20 // 1 MiB
	buf := make([]byte, min(length, maxChunk))
	var total int
	for total < length {
		toRead := min(length-total, len(buf))
		n, err := src.ReadAt(buf[:toRead], *srcOff)
		if n > 0 {
			nw, werr := dst.WriteAt(buf[:n], *dstOff)
			*srcOff += int64(nw)
			*dstOff += int64(nw)
			total += nw
			if werr != nil {
				return total, werr
			}
		}
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// PreadAligned reads len(buf) bytes from f at offset.
// Darwin's F_NOCACHE does not enforce strict alignment, but we validate when
// FlDirectIO is set for API consistency with the Linux implementation.
func PreadAligned(f *os.File, buf []byte, offset int64, flags OpenFlag) (int, error) {
	if flags&FlDirectIO != 0 && !IsAligned(buf) {
		return 0, ErrAlignment
	}
	n, err := f.ReadAt(buf, offset)
	runtime.KeepAlive(f)
	return n, err
}

// OpenFlags returns 0 on Darwin: FlDirectIO is applied via fcntl(F_NOCACHE)
// after open, and FlDSync/FlSync have no open-time equivalent.
func (fl OpenFlag) OpenFlags() int { return 0 }
