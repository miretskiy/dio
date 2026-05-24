package iosched

import (
	"os"
	"runtime"
	"syscall"

	"github.com/miretskiy/dio/sys"
)

// BlockingIO wraps an optional Scheduler with synchronous methods.
type BlockingIO struct {
	S Scheduler // nil for direct POSIX fallback
}

// NewBlockingIO wraps s. Pass nil to use direct POSIX syscalls.
func NewBlockingIO(s Scheduler) *BlockingIO {
	return &BlockingIO{S: s}
}

func (b *BlockingIO) submitOne(op Op) Result {
	t, err := b.S.Submit(op)
	if err != nil {
		return Result{Err: err}
	}
	t.Wait()
	defer t.Release()
	return t.Op.Result
}

// ReadAt performs a positioned read.
func (b *BlockingIO) ReadAt(f *os.File, buf []byte, offset int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if b.S != nil {
		r := b.submitOne(ReadOp(f, buf, offset))
		return r.N, r.Err
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
		r := b.submitOne(WriteOp(f, buf, offset))
		return r.N, r.Err
	}
	n, err := syscall.Pwrite(int(f.Fd()), buf, offset)
	runtime.KeepAlive(f)
	return n, err
}

// Fsync flushes data and metadata for f.
func (b *BlockingIO) Fsync(f *os.File) error {
	if b.S != nil {
		return b.submitOne(FsyncOp(f)).Err
	}
	return f.Sync()
}

// Fdatasync flushes data for f, skipping metadata unless required for
// data retrieval. On Darwin, this uses F_FULLFSYNC for correctness.
func (b *BlockingIO) Fdatasync(f *os.File) error {
	if b.S != nil {
		return b.submitOne(FdatasyncOp(f)).Err
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
