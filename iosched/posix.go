package iosched

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/sys"
)

// POSIXScheduler executes each submitted operation synchronously with POSIX
// file operations and returns an already-completed Ticket.
//
// It also emulates the URingScheduler's virtual-file ops (VOpenatOp and friends)
// with a userspace descriptor table: VOpenat opens a real fd and records it
// under the requested index; subsequent fixed-file ops resolve the fd from the
// table. This keeps virtual-file consumers portable to non-io_uring hosts (and
// runnable in tests) — it does not reproduce io_uring's per-op file pinning, so
// the caller must not close a slot with ops still in flight, the same quiescence
// contract the uring backend's barrier already implies.
type POSIXScheduler struct {
	closed atomic.Bool

	mu     sync.Mutex
	vfiles map[uint32]*os.File // virtual descriptor table, lazily created
}

// NewPOSIXScheduler creates a synchronous scheduler backed by POSIX file I/O.
func NewPOSIXScheduler() *POSIXScheduler {
	return &POSIXScheduler{}
}

// Submit executes op synchronously and returns a completed Ticket.
func (s *POSIXScheduler) Submit(op Op) (*Ticket, error) {
	if s.closed.Load() {
		return nil, errSchedulerClosed
	}

	n, err := countAndValidateOps(&op)
	if err != nil {
		return nil, err
	}

	t := getTicket(op, n)
	for p := &t.Op; p != nil; p = p.linked {
		if p.isVirtual() {
			s.runVirtualOp(p)
		} else {
			runPOSIXOp(p, p.f)
		}
		completeTicket(t)
		if p.Result.Err != nil && p.isLinked() {
			for p = p.linked; p != nil; p = p.linked {
				p.Result.Err = syscall.ECANCELED
				completeTicket(t)
			}
			break
		}
	}
	return t, nil
}

// Close marks the scheduler closed and closes any descriptors still held in the
// virtual-file table.
func (s *POSIXScheduler) Close() error {
	s.closed.Store(true)
	s.mu.Lock()
	for vfd, f := range s.vfiles {
		_ = f.Close()
		delete(s.vfiles, vfd)
	}
	s.mu.Unlock()
	return nil
}

// runVirtualOp emulates a virtual-file op against the userspace descriptor
// table. Open and close mutate the table; every other op just resolves its
// descriptor and runs the same I/O as a regular op. The descriptor is resolved
// under the lock but the I/O runs outside it, so virtual ops are as concurrent
// as regular ones (the lock guards only the table) — this backend must not
// serialize I/O, or it would mask the races the uring backend exposes.
func (s *POSIXScheduler) runVirtualOp(op *Op) {
	switch op.base() {
	case OpOpenat:
		// op.path is NUL-terminated (VOpenatOp appends it for the uring path);
		// Openat wants a Go string.
		name := string(op.path[:len(op.path)-1])
		fd, err := unix.Openat(op.dfd, name, op.openFlag, op.mode)
		if err != nil {
			op.Result.Err = err
			return
		}
		s.mu.Lock()
		if s.vfiles == nil {
			s.vfiles = make(map[uint32]*os.File)
		}
		if s.vfiles[op.vfd] != nil {
			// io_uring openat_direct rejects an occupied slot; mirror that so a
			// double-open is a loud bug, not a silent fd leak.
			s.mu.Unlock()
			_ = os.NewFile(uintptr(fd), name).Close()
			op.Result.Err = syscall.EBADF
			return
		}
		s.vfiles[op.vfd] = os.NewFile(uintptr(fd), name)
		s.mu.Unlock()
	case OpClose:
		s.mu.Lock()
		f := s.vfiles[op.vfd]
		delete(s.vfiles, op.vfd)
		s.mu.Unlock()
		if f == nil {
			op.Result.Err = syscall.EBADF
			return
		}
		op.Result.Err = f.Close()
	default:
		// Resolve the descriptor under the lock, but run the I/O outside it.
		if f := s.vFile(op.vfd); f != nil {
			runPOSIXOp(op, f)
		} else {
			op.Result.Err = syscall.EBADF
		}
	}
}

// vFile returns the descriptor installed at vfd, or nil if the slot is empty.
func (s *POSIXScheduler) vFile(vfd uint32) *os.File {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.vfiles[vfd]
}

// runPOSIXOp executes a positioned-I/O or sync op against f synchronously. f is
// op.f for regular ops and the table-resolved descriptor for virtual ops.
func runPOSIXOp(op *Op, f *os.File) {
	switch op.base() {
	case OpRead, OpReadFixed:
		if len(op.buf) == 0 {
			return
		}
		op.Result.N, op.Result.Err = ignoringEINTR(syscall.Pread, int(f.Fd()), op.buf, op.offset)
	case OpWrite, OpWriteFixed:
		if len(op.buf) == 0 {
			return
		}
		op.Result.N, op.Result.Err = ignoringEINTR(syscall.Pwrite, int(f.Fd()), op.buf, op.offset)
	case OpFsync:
		op.Result.Err = f.Sync()
	case OpFdatasync:
		op.Result.Err = sys.Fdatasync(f)
	case OpFallocate:
		op.Result.Err = sys.Fallocate(f, op.length)
	default:
		op.Result.Err = fmt.Errorf("iosched: invalid opcode %d", op.opcode)
	}
}

// ioFn is the signature shared by syscall.Pread and syscall.Pwrite: positioned
// I/O of p at offset on fd. Both pass as plain function values — no closure, no
// alloc, one indirect call on this non-io_uring path.
type ioFn func(fd int, p []byte, offset int64) (int, error)

// ignoringEINTR runs a positioned-I/O syscall (pread/pwrite), retrying while
// interrupted by a signal. Go's async preemption (SIGURG, since 1.14) interrupts
// blocking syscalls routinely, so — like the standard library's internal/poll —
// the raw syscall must loop on EINTR.
//
// EAGAIN is intentionally not retried: it cannot occur on the blocking
// regular-file fds this serves, and blind-retrying a non-blocking fd would
// busy-spin — waiting for readiness is the poller's job.
func ignoringEINTR(fn ioFn, fd int, p []byte, offset int64) (int, error) {
	for {
		n, err := fn(fd, p, offset)
		if err != syscall.EINTR {
			return n, err
		}
	}
}
