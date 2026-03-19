//go:build linux

package sys

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/align"
)

// alignmentError is a detailed error for O_DIRECT alignment constraint violations.
type alignmentError struct {
	field string
	value int64
	block int
}

func (e *alignmentError) Error() string {
	return fmt.Sprintf("dio/sys: %s %d not aligned to %d", e.field, e.value, e.block)
}

func (e *alignmentError) Unwrap() error { return ErrAlignment }

// Platform capability constants.
const (
	// UseFadvise reports whether posix_fadvise is effective on this platform.
	UseFadvise = true

	// RequiresAlignment reports whether O_DIRECT requires 4 KiB-aligned
	// buffers, offsets, and transfer sizes. Always true on Linux.
	RequiresAlignment = true

	// RequiresExplicitSync reports whether explicit sync calls are needed
	// for durable writes. On Linux, O_DSYNC/O_SYNC at open time suffices.
	RequiresExplicitSync = false
)

// Fdatasync syncs file data to storage without syncing metadata.
// Prefers fdatasync(2) over fsync(2) for better performance on Linux.
// Retries automatically on EINTR (Go runtime SIGURG preemption).
func Fdatasync(f *os.File) error {
	fd := int(f.Fd())
	var err error
	for {
		err = unix.Fdatasync(fd)
		if err != unix.EINTR {
			break
		}
	}
	runtime.KeepAlive(f)
	return err
}

// Fallocate pre-allocates disk space for a file to reduce fragmentation
// and improve sequential write performance.
// Retries automatically on EINTR.
func Fallocate(f *os.File, size int64) error {
	fd := int(f.Fd())
	var err error
	for {
		err = unix.Fallocate(fd, 0, 0, size)
		if err != unix.EINTR {
			break
		}
	}
	runtime.KeepAlive(f)
	return err
}

// PunchHole deallocates a byte range within a file, creating a sparse file.
// Uses FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE to reclaim disk space
// without changing the logical file size.
//
// The range is aligned inward to filesystem block boundaries so that
// adjacent data is never punched. Returns the number of bytes reclaimed
// after alignment, or (0, nil) if the range is smaller than one block.
// Retries automatically on EINTR.
func PunchHole(f *os.File, offset, length int64) (int64, error) {
	alignedOffset, alignedLength, canPunch := AlignForHolePunch(offset, length)
	if !canPunch {
		return 0, nil
	}
	mode := uint32(unix.FALLOC_FL_PUNCH_HOLE | unix.FALLOC_FL_KEEP_SIZE)
	fd := int(f.Fd())
	var err error
	for {
		err = unix.Fallocate(fd, mode, alignedOffset, alignedLength)
		if err != unix.EINTR {
			break
		}
	}
	runtime.KeepAlive(f)
	if err != nil {
		return 0, err
	}
	return alignedLength, nil
}

// Fadvise advises the kernel of the expected access pattern for f.
// Maps the cross-platform [FadviseHint] to Linux posix_fadvise constants.
// Retries automatically on EINTR.
func Fadvise(f *os.File, offset Offset_t, length int64, hint FadviseHint) error {
	var linuxHint int
	switch hint {
	case FadvSequential:
		linuxHint = unix.FADV_SEQUENTIAL
	case FadvDontNeed:
		linuxHint = unix.FADV_DONTNEED
	case FadvRandom:
		linuxHint = unix.FADV_RANDOM
	default:
		return nil
	}
	fd := int(f.Fd())
	var err error
	for {
		err = unix.Fadvise(fd, int64(offset), length, linuxHint)
		if err != unix.EINTR {
			break
		}
	}
	runtime.KeepAlive(f)
	return err
}

// CreateDirect creates path for writing. Always uses O_CREATE|O_WRONLY|O_TRUNC.
// Pass [FlDirectIO] to add O_DIRECT; [FlDSync] / [FlSync] to add O_DSYNC / O_SYNC.
// Falls back to buffered I/O if the filesystem rejects O_DIRECT.
func CreateDirect(path string, flags OpenFlag) (*os.File, error) {
	return openDirect(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, flags)
}

// OpenDirect opens an existing file for reading with optional O_DIRECT.
// Falls back to buffered I/O if the filesystem rejects O_DIRECT.
func OpenDirect(path string, flags OpenFlag) (*os.File, error) {
	return openDirect(path, os.O_RDONLY, flags)
}

// openDirect opens path with baseFlags|flags.OpenFlags(), falling back to
// baseFlags|(flags&^FlDirectIO).OpenFlags() on EINVAL/EOPNOTSUPP.
func openDirect(path string, baseFlags int, flags OpenFlag) (*os.File, error) {
	f, err := os.OpenFile(path, baseFlags|flags.OpenFlags(), 0644)
	if err == nil || flags&FlDirectIO == 0 {
		return f, err
	}
	var errno syscall.Errno
	if errors.As(err, &errno) && (errno == syscall.EINVAL || errno == syscall.EOPNOTSUPP) {
		return os.OpenFile(path, baseFlags|(flags&^FlDirectIO).OpenFlags(), 0644)
	}
	return nil, err
}

// CopyFileRange copies length bytes from srcFile at *srcOff to dstFile at
// *dstOff using the copy_file_range(2) syscall. On XFS with reflinks this
// is a metadata-only operation (zero physical I/O).
// Retries automatically on EINTR.
func CopyFileRange(srcFile, dstFile *os.File, srcOff, dstOff *int64, length int) (int, error) {
	srcFd, dstFd := int(srcFile.Fd()), int(dstFile.Fd())
	var n int
	var err error
	for {
		n, err = unix.CopyFileRange(srcFd, srcOff, dstFd, dstOff, length, 0)
		if err != unix.EINTR {
			break
		}
	}
	runtime.KeepAlive(srcFile)
	runtime.KeepAlive(dstFile)
	return n, err
}

// PreadAligned reads len(buf) bytes from f at offset into buf.
// When FlDirectIO is set, validates that buf, offset, and len(buf) all satisfy
// the [align.BlockSize] alignment requirements of O_DIRECT.
func PreadAligned(f *os.File, buf []byte, offset int64, flags OpenFlag) (int, error) {
	if flags&FlDirectIO != 0 {
		if !IsAligned(buf) {
			return 0, ErrAlignment
		}
		if offset&align.BlockMask != 0 {
			return 0, &alignmentError{field: "offset", value: offset, block: align.BlockSize}
		}
		if int64(len(buf))&align.BlockMask != 0 {
			return 0, &alignmentError{field: "length", value: int64(len(buf)), block: align.BlockSize}
		}
	}
	n, err := f.ReadAt(buf, offset)
	runtime.KeepAlive(f)
	return n, err
}

// OpenFlags returns the Linux-specific os.OpenFile flags for the given OpenFlag.
func (fl OpenFlag) OpenFlags() int {
	var flags int
	if fl&FlDirectIO != 0 {
		flags |= unix.O_DIRECT
	}
	if fl&FlDSync != 0 {
		flags |= unix.O_DSYNC
	}
	if fl&FlSync != 0 {
		flags |= unix.O_SYNC
	}
	return flags
}
