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
// runnable in tests). Because each op runs synchronously to completion before
// Submit returns, the scheduler's ordering guarantees hold here for free: a close
// submitted after a slot's writes runs after them, and later operations run
// after an open. Those guarantees are synchronous consequences here and are
// explicitly coordinated by the io_uring backend.
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
func (s *POSIXScheduler) Submit(op Op) (Ticket, error) {
	if s.closed.Load() {
		return Ticket{}, errSchedulerClosed
	}

	_, err := countAndValidateOps(&op)
	if err != nil {
		return Ticket{}, err
	}

	ticket := op.prepareSubmission()
	root := &op
	for node := root; node != nil; node = node.linked {
		p := node
		var n int
		var opErr error
		if p.isVirtual() {
			n, opErr = s.runVirtualOp(p)
		} else {
			n, opErr = runPOSIXOp(p, p.f)
		}
		softLink := p.sqeFlags&sqeLink != 0
		recordResult(root, p, n, opErr)
		if opErr != nil && softLink {
			for canceled := node.linked; canceled != nil; canceled = canceled.linked {
				recordResult(root, canceled, 0, syscall.ECANCELED)
			}
			break
		}
	}
	root.done.Done()
	return ticket, nil
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
func (s *POSIXScheduler) runVirtualOp(op *Op) (int, error) {
	switch op.kind() {
	case OpOpenat:
		// op.path is NUL-terminated (VOpenatOp appends it for the uring path);
		// Openat wants a Go string.
		name := string(op.path[:len(op.path)-1])
		fd, err := unix.Openat(op.dfd, name, op.openFlag, op.mode)
		if err != nil {
			return 0, err
		}
		s.mu.Lock()
		if s.vfiles == nil {
			s.vfiles = make(map[uint32]*os.File)
		}
		old := s.vfiles[op.vfd]
		s.vfiles[op.vfd] = os.NewFile(uintptr(fd), name)
		s.mu.Unlock()
		if old != nil {
			_ = old.Close()
		}
	case OpClose:
		s.mu.Lock()
		f := s.vfiles[op.vfd]
		delete(s.vfiles, op.vfd)
		s.mu.Unlock()
		if f == nil {
			return 0, syscall.EBADF
		}
		return 0, f.Close()
	default:
		// Resolve the descriptor under the lock, but run the I/O outside it.
		if f := s.vFile(op.vfd); f != nil {
			return runPOSIXOp(op, f)
		}
		return 0, syscall.EBADF
	}
	return 0, nil
}

// vFile returns the descriptor installed at vfd, or nil if the slot is empty.
func (s *POSIXScheduler) vFile(vfd uint32) *os.File {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.vfiles[vfd]
}

// runPOSIXOp executes a positioned-I/O or sync op against f synchronously. f is
// op.f for regular ops and the table-resolved descriptor for virtual ops.
func runPOSIXOp(op *Op, f *os.File) (n int, err error) {
	switch op.kind() {
	case OpRead:
		if len(op.buf) == 0 {
			return 0, nil
		}
		n, err = ignoringEINTR(syscall.Pread, int(f.Fd()), op.buf, op.offset)
	case OpWrite:
		if len(op.buf) == 0 {
			return 0, nil
		}
		n, err = ignoringEINTR(syscall.Pwrite, int(f.Fd()), op.buf, op.offset)
		err = posixDurable(op, f, err)
	case OpFsync:
		err = f.Sync()
	case OpFdatasync:
		err = sys.Fdatasync(f)
	case OpFallocate:
		err = sys.Fallocate(f, op.length)
	case OpClose:
		err = f.Close()
	case OpReadv:
		n, err = vectoredPOSIX(syscall.Pread, int(f.Fd()), op.bufs, op.offset)
	case OpWritev:
		n, err = vectoredPOSIX(syscall.Pwrite, int(f.Fd()), op.bufs, op.offset)
		err = posixDurable(op, f, err)
	case OpOpenat:
		// Plain openat: open the path and return the raw fd through Ticket.N; the
		// caller owns it. (The virtual/direct form is handled in runVirtualOp.)
		name := string(op.path[:len(op.path)-1])
		fd, err := unix.Openat(op.dfd, name, op.openFlag, op.mode)
		if err != nil {
			return 0, err
		}
		n = fd
	default:
		err = fmt.Errorf("iosched: invalid opcode %d", op.opcode)
	}
	return n, err
}

// posixDurable applies a write's requested post-write sync (Op.Durable)
// inline — the POSIX backend has no linked-sync mechanism, so it syncs here after
// the write completes. It is a no-op if the write errored or wanted no sync.
func posixDurable(op *Op, f *os.File, err error) error {
	if err != nil || !op.durable() {
		return err
	}
	return sys.Fdatasync(f)
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

// vectoredPOSIX emulates preadv/pwritev with per-buffer positioned scalar I/O,
// keeping the POSIX backend portable (darwin lacks positioned vectored I/O in
// x/sys/unix). Buffers map to consecutive file offsets starting at offset; a
// short transfer (EOF on read, or a short write) stops the loop, matching a
// single preadv/pwritev's short-count semantics.
func vectoredPOSIX(fn ioFn, fd int, bufs [][]byte, offset int64) (int, error) {
	total := 0
	for _, b := range bufs {
		if len(b) == 0 {
			continue
		}
		n, err := ignoringEINTR(fn, fd, b, offset+int64(total))
		total += n
		if err != nil {
			return total, err
		}
		if n < len(b) {
			break
		}
	}
	return total, nil
}
