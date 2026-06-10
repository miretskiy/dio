//go:build !windows

package iosched

import (
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// PosixScheduler executes ops synchronously with positioned POSIX syscalls
// (pread, pwrite, fsync). It exists for tests and for operating systems
// without io_uring; it adds no batching, queues, or background resources —
// parallelism comes solely from goroutines calling Submit concurrently.
//
// Link semantics match io_uring: a [Op.Linked] op that fails or transfers
// fewer bytes than requested cancels the rest of its chain with ECANCELED;
// [Op.HardLinked] chains continue regardless. Fixed-buffer ops behave exactly
// like their regular counterparts (there is nothing to pre-register).
type PosixScheduler struct {
	closed atomic.Bool
}

var _ Scheduler = (*PosixScheduler)(nil)

// NewPosixScheduler returns a ready-to-use POSIX scheduler. It holds no
// resources; Close only stops further submissions.
func NewPosixScheduler() *PosixScheduler {
	return &PosixScheduler{}
}

// Submit implements [Scheduler]. Ops execute in order on the calling
// goroutine; independent ops in the same call are not parallelized.
func (s *PosixScheduler) Submit(ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	if s.closed.Load() {
		return ErrClosed
	}
	if err := validateChains(ops, 0); err != nil {
		return err
	}
	for start := 0; start < len(ops); {
		end := chainEnd(ops, start)
		runChain(ops[start:end])
		start = end
	}
	return nil
}

// Close implements [io.Closer]. It does not wait for concurrent Submit calls;
// ops already executing run to completion.
func (s *PosixScheduler) Close() error {
	s.closed.Store(true)
	return nil
}

// runChain executes one link chain in order, applying io_uring link rules.
func runChain(chain []Op) {
	broken := false
	for i := range chain {
		op := &chain[i]
		if broken {
			op.completeErr(unix.ECANCELED)
			continue
		}
		op.res = execPosix(op)
		if op.sqeFlags&sqeLink != 0 {
			short := op.res >= 0 && isTransfer(op.code) && int(op.res) != len(op.buf)
			broken = op.res < 0 || short
		}
	}
}

func isTransfer(code opCode) bool {
	return code != opFsync && code != opFdatasync
}

// execPosix runs op's syscall, retrying EINTR, and returns the result in
// CQE convention: byte count on success, negated errno on failure.
func execPosix(op *Op) int32 {
	for {
		var (
			n   int
			err error
		)
		switch op.code {
		case opRead, opReadFixed:
			n, err = unix.Pread(int(op.fd), op.buf, op.off)
		case opWrite, opWriteFixed:
			n, err = unix.Pwrite(int(op.fd), op.buf, op.off)
		case opFsync:
			err = unix.Fsync(int(op.fd))
		case opFdatasync:
			err = fdatasync(int(op.fd))
		default:
			err = unix.EINVAL
		}
		switch {
		case err == unix.EINTR:
			continue
		case err != nil:
			errno, ok := err.(unix.Errno)
			if !ok {
				errno = unix.EIO
			}
			return -int32(errno)
		default:
			return int32(n)
		}
	}
}
