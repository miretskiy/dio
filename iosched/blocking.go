//go:build !windows

package iosched

import (
	"io"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

// BlockingIO provides synchronous positioned I/O on every platform. See the
// package documentation for how it picks its backend; construct one with
// [NewDefaultIO], [NewBlockingIO], or [NewPosixIO].
//
// ReadAt and WriteAt implement full io.ReaderAt/io.WriterAt semantics:
// they loop on short transfers and ReadAt returns io.EOF when the file ends
// before the buffer is full.
type BlockingIO struct {
	sched Scheduler
}

// NewBlockingIO returns a BlockingIO that routes every call through s.
func NewBlockingIO(s Scheduler) *BlockingIO {
	return &BlockingIO{sched: s}
}

// NewPosixIO returns a BlockingIO that invokes POSIX syscalls directly —
// no indirection, no scheduler.
func NewPosixIO() *BlockingIO {
	return &BlockingIO{}
}

// Scheduler returns the underlying scheduler, or nil for the direct POSIX
// path. Useful for [RegisterDMASlab] or for submitting op batches directly.
func (b *BlockingIO) Scheduler() Scheduler { return b.sched }

// Close releases the underlying scheduler, if any.
func (b *BlockingIO) Close() error {
	if b.sched != nil {
		return b.sched.Close()
	}
	return nil
}

// ReadAt reads len(p) bytes from f at offset off, looping on short reads.
// It returns io.EOF if the file ends first (per io.ReaderAt).
func (b *BlockingIO) ReadAt(f *os.File, p []byte, off int64) (int, error) {
	fd := int(f.Fd())
	var (
		total int
		err   error
	)
	if b.sched == nil {
		total, err = preadFull(fd, p, off)
	} else {
		total, err = b.submitFull(opRead, fd, p, off)
	}
	runtime.KeepAlive(f)
	return total, err
}

// WriteAt writes len(p) bytes to f at offset off, looping on short writes.
func (b *BlockingIO) WriteAt(f *os.File, p []byte, off int64) (int, error) {
	fd := int(f.Fd())
	var (
		total int
		err   error
	)
	if b.sched == nil {
		total, err = pwriteFull(fd, p, off)
	} else {
		total, err = b.submitFull(opWrite, fd, p, off)
	}
	runtime.KeepAlive(f)
	return total, err
}

// Fsync flushes f's data and metadata to stable storage.
func (b *BlockingIO) Fsync(f *os.File) error {
	return b.sync(f, opFsync)
}

// Fdatasync flushes f's data (and only the metadata needed to retrieve it).
// On platforms without fdatasync it degrades to fsync.
func (b *BlockingIO) Fdatasync(f *os.File) error {
	return b.sync(f, opFdatasync)
}

func (b *BlockingIO) sync(f *os.File, code opCode) error {
	fd := int(f.Fd())
	var err error
	if b.sched == nil {
		for {
			if code == opFsync {
				err = unix.Fsync(fd)
			} else {
				err = fdatasync(fd)
			}
			if err != unix.EINTR {
				break
			}
		}
	} else {
		ops := []Op{{code: code, fd: int32(fd)}}
		err = b.sched.Submit(ops)
		if err == nil {
			err = ops[0].Result().Err
		}
	}
	runtime.KeepAlive(f)
	return err
}

// submitFull routes one transfer through the scheduler, resubmitting the
// remainder after short transfers.
func (b *BlockingIO) submitFull(code opCode, fd int, p []byte, off int64) (int, error) {
	total := 0
	for total < len(p) {
		ops := []Op{{code: code, fd: int32(fd), buf: p[total:], off: off + int64(total)}}
		if err := b.sched.Submit(ops); err != nil {
			return total, err
		}
		r := ops[0].Result()
		if r.Err != nil {
			return total, r.Err
		}
		if r.N == 0 {
			if code == opRead {
				return total, io.EOF
			}
			return total, io.ErrShortWrite
		}
		total += r.N
	}
	return total, nil
}

func preadFull(fd int, p []byte, off int64) (int, error) {
	total := 0
	for total < len(p) {
		n, err := unix.Pread(fd, p[total:], off+int64(total))
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.EOF
		}
		total += n
	}
	return total, nil
}

func pwriteFull(fd int, p []byte, off int64) (int, error) {
	total := 0
	for total < len(p) {
		n, err := unix.Pwrite(fd, p[total:], off+int64(total))
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
		total += n
	}
	return total, nil
}
