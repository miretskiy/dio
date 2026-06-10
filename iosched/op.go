package iosched

import (
	"errors"
	"fmt"
	"sync/atomic"
	"syscall"
)

// ErrClosed is returned by [Scheduler.Submit] after the scheduler has been
// closed.
var ErrClosed = errors.New("iosched: scheduler closed")

// opCode identifies the kind of I/O an [Op] performs.
type opCode uint8

const (
	opNop opCode = iota
	opRead
	opWrite
	opReadFixed
	opWriteFixed
	opFsync
	opFdatasync
)

// SQE flag bits for link chains. The values mirror the kernel's
// IOSQE_IO_LINK / IOSQE_IO_HARDLINK; uring_linux.go statically asserts they
// match giouring's constants. Defined here (rather than referencing giouring)
// so Op compiles on non-Linux platforms.
const (
	sqeLink     uint8 = 1 << 2
	sqeHardlink uint8 = 1 << 3
	sqeLinkMask       = sqeLink | sqeHardlink
)

// Op describes one positioned I/O operation. Build Ops with [ReadOp],
// [WriteOp], [ReadFixedOp], [WriteFixedOp], [FsyncOp] or [FdatasyncOp],
// pass them to [Scheduler.Submit], then read the outcome via [Op.Result].
//
// An Op is a plain value; a fresh Op (or a reused one) carries no state from
// previous submissions other than its result field, which Submit resets.
type Op struct {
	buf      []byte
	off      int64
	g        *submitGroup // owning submission; set by the scheduler
	fd       int32
	res      int32 // completion result: byte count, or negated errno
	code     opCode
	sqeFlags uint8 // SQE flag bits (link/hardlink)
}

// ReadOp returns an Op that reads len(buf) bytes from fd at offset off
// (pread semantics: the file cursor is not used or moved).
func ReadOp(fd int, buf []byte, off int64) Op {
	return Op{code: opRead, fd: int32(fd), buf: buf, off: off}
}

// WriteOp returns an Op that writes len(buf) bytes to fd at offset off
// (pwrite semantics).
func WriteOp(fd int, buf []byte, off int64) Op {
	return Op{code: opWrite, fd: int32(fd), buf: buf, off: off}
}

// ReadFixedOp is like [ReadOp] but uses a pre-registered DMA buffer: buf must
// lie within the [mempool.SlabPool] registered via [RegisterDMASlab],
// eliminating per-I/O page-pinning overhead. On schedulers without registered
// buffer support (POSIX) it behaves exactly like ReadOp.
func ReadFixedOp(fd int, buf []byte, off int64) Op {
	return Op{code: opReadFixed, fd: int32(fd), buf: buf, off: off}
}

// WriteFixedOp is like [WriteOp] but uses a pre-registered DMA buffer.
// See [ReadFixedOp] for the buffer requirements.
func WriteFixedOp(fd int, buf []byte, off int64) Op {
	return Op{code: opWriteFixed, fd: int32(fd), buf: buf, off: off}
}

// FsyncOp returns an Op that flushes fd's data and metadata to stable storage.
func FsyncOp(fd int) Op {
	return Op{code: opFsync, fd: int32(fd)}
}

// FdatasyncOp returns an Op that flushes fd's data (and only the metadata
// required to retrieve it) to stable storage. On platforms without
// fdatasync it degrades to fsync.
func FdatasyncOp(fd int) Op {
	return Op{code: opFdatasync, fd: int32(fd)}
}

// Linked marks op as the predecessor of the next op in the same Submit call:
// the kernel executes the next op only after this op completes fully. A
// failed or short transfer breaks the chain, completing the remaining linked
// ops with ECANCELED. The last op of a chain must not be Linked.
func (op Op) Linked() Op {
	op.sqeFlags = (op.sqeFlags &^ sqeLinkMask) | sqeLink
	return op
}

// HardLinked is like [Linked], but the chain keeps executing even if this op
// fails or transfers fewer bytes than requested.
func (op Op) HardLinked() Op {
	op.sqeFlags = (op.sqeFlags &^ sqeLinkMask) | sqeHardlink
	return op
}

// Result holds the outcome of a completed [Op].
type Result struct {
	// N is the number of bytes transferred (0 for fsync ops). A read of
	// N == 0 with a non-empty buffer indicates end-of-file.
	N int
	// Err is nil on success, the syscall error otherwise. Ops canceled by a
	// broken link chain report syscall.ECANCELED.
	Err error
}

// Result returns op's completion outcome. Valid only after the
// [Scheduler.Submit] call that carried op has returned nil.
func (op *Op) Result() Result {
	if op.res < 0 {
		return Result{Err: syscall.Errno(-op.res)}
	}
	return Result{N: int(op.res)}
}

// completeErr records err as op's completion result.
func (op *Op) completeErr(err syscall.Errno) {
	op.res = -int32(err)
}

// chainEnd returns the index just past the link chain starting at ops[start].
// An op without a link flag terminates its chain.
func chainEnd(ops []Op, start int) int {
	end := start
	for end < len(ops)-1 && ops[end].sqeFlags&sqeLinkMask != 0 {
		end++
	}
	return end + 1
}

// validateChains rejects op lists that no scheduler can execute: a trailing
// op that still links forward (it would chain into an unrelated submission),
// and chains longer than maxChain (0 = unlimited), which could never be
// placed contiguously in the submission ring.
func validateChains(ops []Op, maxChain int) error {
	if ops[len(ops)-1].sqeFlags&sqeLinkMask != 0 {
		return errors.New("iosched: last op of a Submit call must not be Linked/HardLinked")
	}
	for start := 0; start < len(ops); {
		end := chainEnd(ops, start)
		if maxChain > 0 && end-start > maxChain {
			return fmt.Errorf("iosched: link chain of %d ops exceeds scheduler capacity %d", end-start, maxChain)
		}
		start = end
	}
	return nil
}

// submitGroup tracks one Submit call's outstanding ops and parks its caller.
// remaining is decremented once per completed op; the caller waits on sema,
// which is also used by the io_uring scheduler to hand off ring leadership.
type submitGroup struct {
	remaining atomic.Int32
	sema      chan struct{} // cap 1; tokens coalesce via signal()
	// intrusive wait-list links, guarded by the scheduler mutex
	next, prev *submitGroup
	inList     bool
}

// signal wakes the group's waiter. Non-blocking: a token is dropped if one is
// already pending, so done- and leadership-signals coalesce. Waiters treat
// every wakeup as a hint and re-check state.
func (g *submitGroup) signal() {
	select {
	case g.sema <- struct{}{}:
	default:
	}
}

// drain removes a pending token, if any, so a pooled group starts clean.
func (g *submitGroup) drain() {
	select {
	case <-g.sema:
	default:
	}
}
