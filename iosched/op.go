package iosched

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/miretskiy/dio/internal/buildutil"
)

// Opcode identifies the type of I/O operation.
const (
	OpRead       uint8 = iota // pread - positioned read
	OpWrite                   // pwrite - positioned write
	OpFsync                   // fsync - flush data and metadata
	OpFdatasync               // fdatasync - flush data
	OpReadFixed               // pread via IORING_OP_READ_FIXED
	OpWriteFixed              // pwrite via IORING_OP_WRITE_FIXED
)

// Op describes a single I/O operation submitted to a Scheduler.
//
// Op is a plain value. Construct it with ReadOp, WriteOp, FsyncOp, or one of
// the fixed-buffer constructors, then optionally chain another op with Link.
// The scheduler copies the root Op into a pooled Ticket; linked ops are the
// only exception path and are heap-allocated by Link.
//
// Always pass *os.File, never a raw fd. Keeping the file in the Op pins it
// until the kernel has completed the SQE, preventing a finalizer from closing
// and recycling the fd while io_uring still references it.
type Op struct {
	opcode   uint8
	sqeFlags uint8
	f        *os.File
	buf      []byte
	offset   int64
	opFlags  uint32

	Result struct {
		N   int
		Err error
	}

	// The exception path for linked SQEs.
	linked *Op
}

// Result is the public shape of Op.Result.
type Result = struct {
	N   int
	Err error
}

// SQE linking flag bits stored in Op.sqeFlags.
const (
	sqeLink uint8 = 1 << 0 // IOSQE_IO_LINK
)

// ReadOp constructs a positioned-read operation.
func ReadOp(f *os.File, buf []byte, offset int64) Op {
	return Op{opcode: OpRead, f: f, buf: buf, offset: offset}
}

// WriteOp constructs a positioned-write operation.
func WriteOp(f *os.File, buf []byte, offset int64) Op {
	return Op{opcode: OpWrite, f: f, buf: buf, offset: offset}
}

// FsyncOp constructs a full fsync operation.
func FsyncOp(f *os.File) Op {
	return Op{opcode: OpFsync, f: f}
}

// FdatasyncOp constructs an fdatasync operation.
func FdatasyncOp(f *os.File) Op {
	return Op{opcode: OpFdatasync, f: f}
}

// ReadFixedOp constructs a positioned read using a pre-registered fixed buffer.
func ReadFixedOp(f *os.File, buf []byte, offset int64) Op {
	return Op{opcode: OpReadFixed, f: f, buf: buf, offset: offset}
}

// WriteFixedOp constructs a positioned write using a pre-registered fixed buffer.
func WriteFixedOp(f *os.File, buf []byte, offset int64) Op {
	return Op{opcode: OpWriteFixed, f: f, buf: buf, offset: offset}
}

// Link returns a copy of o linked to next with IOSQE_IO_LINK semantics.
func (o Op) Link(next Op) Op {
	tail := &o
	for tail.linked != nil {
		tail = tail.linked
	}
	tail.sqeFlags |= sqeLink
	tail.linked = &next
	return o
}

func (o Op) isLinked() bool {
	return o.sqeFlags&sqeLink != 0 && o.linked != nil
}

// Ticket is the per-Submit synchronization handle returned by Scheduler.
type Ticket struct {
	// Intrusive lock-free MPSC link.
	next *Ticket

	// The embedded operation.
	Op Op

	wg       sync.WaitGroup
	pending  atomic.Int32
	released atomic.Bool
}

var ticketPool = sync.Pool{
	New: func() any { return new(Ticket) },
}

func getTicket(op Op, pending int32) *Ticket {
	t := ticketPool.Get().(*Ticket)
	t.next = nil
	t.Op = op
	t.pending.Store(pending)
	t.released.Store(false)
	t.wg.Add(1)
	return t
}

func putTicket(t *Ticket) {
	clearResults(&t.Op)
	t.Op = Op{}
	t.next = nil
	t.pending.Store(0)
	t.released.Store(true)
	ticketPool.Put(t)
}

// Wait blocks until all ops in the Ticket's linked chain have completed.
func (t *Ticket) Wait() {
	if t == nil {
		return
	}
	t.wg.Wait()
}

// Release returns t to the shared ticket pool.
//
// It is nil-safe and idempotent so callers can use defer without having to
// special-case failed Submit calls or accidental double release.
func (t *Ticket) Release() {
	if t == nil || t.released.Swap(true) {
		return
	}
	// If this ticket is not clean, we can't return it to the pool
	// lest wg has incorrect state.
	if t.pending.Load() > 0 {
		return
	}

	putTicket(t)
}

// Error returns the first operation error in io_uring linked-chain order.
func (t *Ticket) Error() error {
	if t == nil {
		return nil
	}
	for op := &t.Op; op != nil; op = op.linked {
		if op.Result.Err != nil {
			return op.Result.Err
		}
	}
	return nil
}

func countAndValidateOps(op *Op) (int32, error) {
	var n int32
	for p := op; p != nil; p = p.linked {
		if p.f == nil {
			return 0, fmt.Errorf("iosched: op %d has nil file", n)
		}
		n++
	}
	return n, nil
}

func clearResults(op *Op) {
	for p := op; p != nil; p = p.linked {
		p.Result = Result{}
	}
}

func completeTicket(t *Ticket) {
	remaining := t.pending.Add(-1)
	if err := buildutil.Assert(remaining >= 0); err != nil {
		panic(err)
	}
	if remaining == 0 {
		t.wg.Done()
	}
}
