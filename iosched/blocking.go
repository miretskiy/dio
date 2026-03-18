package iosched

import (
	"os"
	"runtime"
	"syscall"

	"github.com/miretskiy/dio/sys"
)

// BlockingIO wraps an optional [Scheduler] with synchronous methods.
// It works on all platforms.
//
// When S is non-nil (io_uring available), each method routes through
// Submit+Wait, fully using the async ring pipeline.
//
// When S is nil, methods invoke POSIX syscalls directly with no indirection.
//
// # File lifetime
//
// Methods take *os.File, not raw fd integers. This ensures the GC can trace
// the reference and the fd cannot be closed or reused while I/O is in flight.
// See [Op] for a detailed explanation.
type BlockingIO struct {
	S Scheduler // nil for direct POSIX fallback
}

// NewBlockingIO wraps s. Pass nil to use direct POSIX syscalls.
func NewBlockingIO(s Scheduler) *BlockingIO {
	return &BlockingIO{S: s}
}

// ReadAt performs a positioned read.
func (b *BlockingIO) ReadAt(f *os.File, buf []byte, offset int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if b.S != nil {
		ops := [1]Op{ReadOp(f, buf, offset)}
		t, err := b.S.Submit(ops[:])
		if err != nil {
			return 0, err
		}
		var res [1]Result
		if _, err := b.S.Wait(t, res[:]); err != nil {
			return 0, err
		}
		return res[0].N, res[0].Err
	}
	n, err := syscall.Pread(int(f.Fd()), buf, offset)
	runtime.KeepAlive(f)
	return n, err
}

// WriteAt performs a positioned write.
func (b *BlockingIO) WriteAt(f *os.File, buf []byte, offset int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if b.S != nil {
		ops := [1]Op{WriteOp(f, buf, offset)}
		t, err := b.S.Submit(ops[:])
		if err != nil {
			return 0, err
		}
		var res [1]Result
		if _, err := b.S.Wait(t, res[:]); err != nil {
			return 0, err
		}
		return res[0].N, res[0].Err
	}
	n, err := syscall.Pwrite(int(f.Fd()), buf, offset)
	runtime.KeepAlive(f)
	return n, err
}

// Fsync flushes data and metadata for f.
func (b *BlockingIO) Fsync(f *os.File) error {
	if b.S != nil {
		ops := [1]Op{FsyncOp(f)}
		t, err := b.S.Submit(ops[:])
		if err != nil {
			return err
		}
		var res [1]Result
		if _, err := b.S.Wait(t, res[:]); err != nil {
			return err
		}
		return res[0].Err
	}
	return f.Sync()
}

// Fdatasync flushes data for f, skipping metadata unless required for
// data retrieval. On Darwin, this uses F_FULLFSYNC for correctness.
func (b *BlockingIO) Fdatasync(f *os.File) error {
	if b.S != nil {
		ops := [1]Op{FdatasyncOp(f)}
		t, err := b.S.Submit(ops[:])
		if err != nil {
			return err
		}
		var res [1]Result
		if _, err := b.S.Wait(t, res[:]); err != nil {
			return err
		}
		return res[0].Err
	}
	return sys.Fdatasync(f)
}

// Close closes the underlying Scheduler if present.
func (b *BlockingIO) Close() error {
	if b.S != nil {
		return b.S.Close()
	}
	return nil
}
