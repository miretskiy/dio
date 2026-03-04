// Package sys provides OS-level file descriptor primitives for page-cache bypass.
//
// # KeepAlive mandate
//
// Every exported function that calls a raw syscall via an fd extracted from
// *os.File calls runtime.KeepAlive(f) unconditionally immediately after the
// syscall — before any error checking. This prevents the Go GC from
// finalizing f (and closing its fd) while the kernel still has it open:
//
//	err := unix.Fdatasync(int(f.Fd()))
//	runtime.KeepAlive(f) // <— unconditional, before error check
//	if err != unix.EINTR { … }
//
// # Cross-platform
//
// Build-tagged files (io_linux.go, io_darwin.go) supply platform implementations
// for Fdatasync, Fallocate, PunchHole, Fadvise, CreateDirect, OpenDirect, and
// CopyFileRange. This file supplies shared types and pure-computation helpers.
package sys

import (
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"

	"github.com/miretskiy/dio/align"
)

// ErrAlignment is returned when a buffer is not properly aligned for O_DIRECT.
var ErrAlignment = errors.New("dio/sys: buffer not aligned for O_DIRECT")

// IsAligned reports whether the first byte of block is aligned to [align.BlockSize].
// Delegates to [align.IsAligned]; provided here so callers need only import sys.
func IsAligned(block []byte) bool { return align.IsAligned(block) }

// OpenFlag controls file opening and sync behavior.
type OpenFlag int

const (
	// FlDirectIO bypasses the page cache (O_DIRECT on Linux, F_NOCACHE on Darwin).
	FlDirectIO OpenFlag = 1 << iota

	// FlDSync syncs data before each write returns (O_DSYNC on Linux).
	// On Darwin, explicit Fdatasync is needed after writes.
	FlDSync

	// FlSync syncs data and metadata before each write returns (O_SYNC on Linux).
	// On Darwin, explicit f.Sync is needed after writes.
	FlSync
)

// Common flag combinations.
const (
	SyncNone OpenFlag = 0
	SyncData          = FlDSync
	SyncFull          = FlSync
)

// FadviseHint describes the expected access pattern for posix_fadvise.
type FadviseHint int

const (
	FadvSequential FadviseHint = iota
	FadvDontNeed
	FadvRandom
)

// Offset_t is a signed file offset, matching the C off_t type.
type Offset_t = int64

// SyncFile syncs f according to flags after a write completes.
//
// On Linux, O_DSYNC/O_SYNC at open time makes this a no-op.
// On Darwin, where those flags are unavailable at open time,
// RequiresExplicitSync is true and this function issues the call.
func SyncFile(f *os.File, flags OpenFlag) error {
	if !RequiresExplicitSync {
		return nil
	}
	if flags&FlSync != 0 {
		return f.Sync()
	}
	if flags&FlDSync != 0 {
		return Fdatasync(f)
	}
	return nil
}

// CreateAndAllocate creates path with flags and reserves allocSize bytes via Fallocate.
func CreateAndAllocate(path string, flags OpenFlag, allocSize int64) (*os.File, error) {
	f, err := CreateDirect(path, flags)
	if err != nil {
		return nil, err
	}
	if err := Fallocate(f, allocSize); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return f, nil
}

// WriteFile atomically writes data to a new file.
// data must be [align.BlockSize]-aligned when FlDirectIO is set.
func WriteFile(path string, data []byte, flags OpenFlag) (retErr error) {
	if flags&FlDirectIO != 0 && !IsAligned(data) {
		return ErrAlignment
	}
	f, err := CreateAndAllocate(path, flags, int64(len(data)))
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	if _, err := f.Write(data); err != nil {
		return err
	}
	return SyncFile(f, flags)
}

// AlignForHolePunch computes filesystem-block-aligned boundaries for
// FALLOC_FL_PUNCH_HOLE / F_PUNCHHOLE so that adjacent blobs are not punched.
//
// Returns (alignedOffset, alignedLength, canPunch).
// canPunch is false when the adjusted range is smaller than one block.
func AlignForHolePunch(offset, length int64) (int64, int64, bool) {
	// Round offset UP so we do not punch into the preceding blob.
	alignedOffset := (offset + align.BlockMask) &^ align.BlockMask
	length -= alignedOffset - offset
	if length < align.BlockSize {
		return 0, 0, false
	}
	// Round length DOWN so we do not punch into the following blob.
	length &^= align.BlockMask
	return alignedOffset, length, true
}

// IsTransientIOError reports whether err represents a temporary condition
// that may succeed on retry.
func IsTransientIOError(err error) bool {
	if err == nil {
		return false
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.EINTR,
			syscall.EAGAIN,
			syscall.EBUSY,
			syscall.EMFILE,
			syscall.ENFILE,
			syscall.ENOMEM:
			return true
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

// alignmentError is a detailed error for alignment constraint violations.
type alignmentError struct {
	field string
	value int64
	block int
}

func (e *alignmentError) Error() string {
	return fmt.Sprintf("dio/sys: %s %d not aligned to %d", e.field, e.value, e.block)
}

func (e *alignmentError) Unwrap() error { return ErrAlignment }
