package iosched

import (
	"fmt"
	"sync/atomic"
	"syscall"

	"github.com/miretskiy/dio/sys"
)

// POSIXScheduler executes each submitted operation synchronously with POSIX
// file operations and returns an already-completed Ticket.
type POSIXScheduler struct {
	closed atomic.Bool
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
		runPOSIXOp(p)
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

// Close marks the scheduler closed. It does not own any OS resources.
func (s *POSIXScheduler) Close() error {
	s.closed.Store(true)
	return nil
}

func runPOSIXOp(op *Op) {
	switch op.opcode {
	case OpRead, OpReadFixed:
		if len(op.buf) == 0 {
			return
		}
		op.Result.N, op.Result.Err = syscall.Pread(int(op.f.Fd()), op.buf, op.offset)
	case OpWrite, OpWriteFixed:
		if len(op.buf) == 0 {
			return
		}
		op.Result.N, op.Result.Err = syscall.Pwrite(int(op.f.Fd()), op.buf, op.offset)
	case OpFsync:
		op.Result.Err = op.f.Sync()
	case OpFdatasync:
		op.Result.Err = sys.Fdatasync(op.f)
	default:
		op.Result.Err = fmt.Errorf("iosched: invalid opcode %d", op.opcode)
	}
}
