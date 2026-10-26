package iosched

import (
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/v2/sys"
)

// POSIXScheduler executes each submitted operation synchronously with POSIX
// file operations and returns an already-completed Ticket.
//
// WithVFiles(n) enables a userspace virtual-file table with the io_uring
// backend's semantics: VOpenatOp installs a descriptor in its slot, AllocateVFD
// reserves a free slot for the chain (ENFILE when full), and a successful
// virtual close frees it. Without a table, virtual operations fail with
// EOPNOTSUPP. The table lock guards only slot state; I/O runs outside it.
type POSIXScheduler struct {
	closed atomic.Bool

	mu     sync.Mutex
	vfiles []posixVFile
}

type posixVFile struct {
	file     *os.File
	reserved bool // an AllocateVFD chain owns the slot before its open completes
}

// NewPOSIXScheduler creates a synchronous scheduler backed by POSIX file I/O.
// It honors WithVFiles and ignores the io_uring-specific options.
func NewPOSIXScheduler(opts ...Option) *POSIXScheduler {
	cfg := makeSchedulerConfig(opts)
	return &POSIXScheduler{vfiles: make([]posixVFile, cfg.vfiles)}
}

// Submit executes op synchronously and returns a completed Ticket.
func (s *POSIXScheduler) Submit(op Op) (Ticket, error) {
	if s.closed.Load() {
		return Ticket{}, errSchedulerClosed
	}

	work, err := s.prepareSubmission(&op)
	if err != nil {
		return Ticket{}, err
	}
	work.markIssued()
	s.allocateVFD(work)
	for i := range work.opCount() {
		current := work.op(i)
		n, err := s.run(current)
		if current.opFlags&opAllocatedVFD != 0 {
			if err != nil {
				current.opFlags |= opReleaseVFD
			} else {
				n = int(current.vfd)
			}
		}
		work.recordResult(current, n, err)
		if err != nil && current.sqeFlags&sqeLink != 0 {
			for j := i + 1; j < work.opCount(); j++ {
				work.recordResult(work.op(j), 0, syscall.ECANCELED)
			}
			break
		}
	}
	// Like the io_uring backend, a failed open's slot stays reserved until its
	// chain ends so followers cannot address a slot reallocated meanwhile.
	if work.root.opFlags&opReleaseVFD != 0 {
		s.mu.Lock()
		s.vfiles[work.root.vfd] = posixVFile{}
		s.mu.Unlock()
	}
	work.finish()
	return Ticket{&work.completion}, nil
}

func (s *POSIXScheduler) prepareSubmission(op *Op) (*submission, error) {
	work, err := copySubmission(op, maxChainLength)
	if err != nil {
		return nil, err
	}
	for i := range work.opCount() {
		current := work.op(i)
		if !current.isVirtual() {
			continue
		}
		if len(s.vfiles) == 0 {
			return nil, syscall.EOPNOTSUPP
		}
		if current.vfd != AllocateVFD && int(current.vfd) >= len(s.vfiles) {
			return nil, fmt.Errorf("iosched: virtual file index %d outside configured table", current.vfd)
		}
	}
	return work, nil
}

// allocateVFD resolves AllocateVFD placeholders when the root is a virtual open.
// On exhaustion the placeholders remain: the open reports ENFILE and followers EBADF.
func (s *POSIXScheduler) allocateVFD(work *submission) {
	root := &work.root
	if !root.isVirtual() || root.kind() != OpOpenat || root.vfd != AllocateVFD {
		return
	}
	s.mu.Lock()
	i := slices.IndexFunc(s.vfiles, func(v posixVFile) bool { return v.file == nil && !v.reserved })
	if i >= 0 {
		s.vfiles[i].reserved = true
	}
	s.mu.Unlock()
	if i < 0 {
		return
	}
	root.opFlags |= opAllocatedVFD
	for j := range work.opCount() {
		if op := work.op(j); op.isVirtual() && op.vfd == AllocateVFD {
			op.vfd = uint32(i)
		}
	}
}

// run executes one operation, resolving a virtual descriptor from the table.
func (s *POSIXScheduler) run(op *operation) (int, error) {
	if !op.isVirtual() {
		return runPOSIXOp(op, op.f)
	}
	if op.vfd == AllocateVFD {
		if op.kind() == OpOpenat {
			return 0, syscall.ENFILE
		}
		return 0, syscall.EBADF
	}
	switch op.kind() {
	case OpOpenat:
		name := string(op.path[:len(op.path)-1])
		fd, err := unix.Openat(op.dfd, name, op.openFlag, op.mode)
		if err != nil {
			return 0, err
		}
		s.mu.Lock()
		old := s.vfiles[op.vfd].file
		s.vfiles[op.vfd] = posixVFile{file: os.NewFile(uintptr(fd), name)}
		s.mu.Unlock()
		if old != nil {
			_ = old.Close()
		}
		return 0, nil
	case OpClose:
		s.mu.Lock()
		f := s.vfiles[op.vfd].file
		if f != nil {
			s.vfiles[op.vfd] = posixVFile{}
		}
		s.mu.Unlock()
		if f == nil {
			return 0, syscall.EBADF
		}
		return 0, f.Close()
	default:
		s.mu.Lock()
		f := s.vfiles[op.vfd].file
		s.mu.Unlock()
		if f == nil {
			return 0, syscall.EBADF
		}
		return runPOSIXOp(op, f)
	}
}

// Close marks the scheduler closed and closes descriptors left in the table.
func (s *POSIXScheduler) Close() error {
	s.closed.Store(true)
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, v := range s.vfiles {
		if v.file != nil {
			_ = v.file.Close()
		}
		s.vfiles[i] = posixVFile{}
	}
	return nil
}

// runPOSIXOp executes an operation synchronously.
func runPOSIXOp(op *operation, f *os.File) (n int, err error) {
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
		err = writeResultError(op, n, err)
		err = posixDurable(op, f, err)
	case OpFsync:
		err = f.Sync()
	case OpFdatasync:
		err = sys.Fdatasync(f)
	case OpFallocate:
		err = sys.Fallocate(f, op.length)
	case OpClose:
		// Regular-file close ownership stays with the caller. POSIX Submit is
		// synchronous, so reaching this operation has already drained earlier
		// submissions and linked predecessors.
		err = nil
	case OpReadv:
		n, err = vectoredPOSIX(syscall.Pread, int(f.Fd()), op.bufs, op.offset)
	case OpWritev:
		n, err = vectoredPOSIX(syscall.Pwrite, int(f.Fd()), op.bufs, op.offset)
		err = writeResultError(op, n, err)
		err = posixDurable(op, f, err)
	case OpUnlinkat:
		err = unix.Unlinkat(op.dfd, string(op.path[:len(op.path)-1]), 0)
	case OpOpenat:
		// Plain openat: open the path and return the raw fd through Ticket.Wait;
		// the caller owns it.
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
func posixDurable(op *operation, f *os.File, err error) error {
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
