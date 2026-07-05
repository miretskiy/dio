package iosched

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/miretskiy/dio/internal/buildutil"
)

// Opcode identifies the type of I/O operation. The low bits name the operation;
// the opVirtual and opFixed high bits are addressing/buffer flags. kind() returns
// the operation with those flags masked off.
const (
	OpRead  uint8 = iota // pread - positioned read
	OpReadv              // preadv - vectored positioned read

	OpWrite  // pwrite - positioned write
	OpWritev // pwritev - vectored positioned write

	OpOpenat // openat - installs into a vfd slot, or returns a fd
	OpClose  // close - frees a vfd slot, or closes a regular fd

	OpFsync     // fsync - flush data and metadata
	OpFdatasync // fdatasync - flush data
	OpFallocate // fallocate - preallocate file space
)

// Opcode flags OR'd into an opcode. opVirtual selects virtual-file addressing
// (the file is a registered-table index, not an *os.File); opFixed selects a
// pre-registered fixed buffer (IORING_OP_*_FIXED). They compose; kind() masks
// both off.
const (
	opFixed   uint8 = 1 << 6
	opVirtual uint8 = 1 << 7
)

// Op describes a single I/O operation submitted to a Scheduler.
//
// Op is a plain value. Construct it with ReadOp, WriteOp, FsyncOp, or one of
// the fixed-buffer constructors, then optionally chain another op with Link.
// The scheduler copies the root Op into a pooled Ticket; linked ops are the
// only exception path and are heap-allocated by Link.
//
// Always pass *os.File for normal file operations, never a raw fd. Keeping the
// file in the Op pins it until the kernel has completed the SQE, preventing a
// finalizer from closing and recycling the fd while io_uring still references
// it. Virtual-file operations use a registered-file table index instead
// because those descriptors are private to an io_uring registered file table,
// not process file descriptors.
type Op struct {
	opcode   uint8
	sqeFlags uint8
	f        *os.File
	vfd      uint32
	dfd      int
	path     []byte
	openFlag int
	mode     uint32
	buf      []byte
	bufs     [][]byte // vectored buffers (readv/writev); iovecs are built at placement
	offset   int64
	length   int64 // byte count for ops without a buffer (fallocate)
	opFlags  uint32

	// result is the op's completion outcome, valid once the ticket is ready. It is
	// unexported; callers read it through Ticket.Result, Ticket.Error, Ticket.N.
	result Result

	// The exception path for linked SQEs.
	linked *Op
}

// Result is an op's completion outcome: N bytes transferred (or, for a plain
// openat, the returned file descriptor) and Err.
type Result struct {
	N   int
	Err error
	// seq is a test-only completion-order counter (dio's reap sequence, stamped in
	// reap); it lets a test confirm a write completes before its linked fdatasync.
	seq uint64
}

// SQE linking flag bits stored in Op.sqeFlags, marking how a linked follower is
// chained after this op.
const (
	sqeLink     uint8 = 1 << 0 // IOSQE_IO_LINK: a failure cancels the rest of the chain
	sqeHardLink uint8 = 1 << 1 // IOSQE_IO_HARDLINK: the chain survives a failure
)

// opFlags bits stored in Op.opFlags.
const (
	opDurable uint32 = 1 << 0 // flush the write (fdatasync) before the ticket completes
)

// Durable requests that this write's data be flushed to stable storage
// (fdatasync) before the ticket completes: the io_uring backend links the sync
// after the write (once per coalesced run — see linkDurableSyncs); the POSIX
// backend syncs inline. It has no effect on non-write ops.
func (o Op) Durable() Op {
	o.opFlags |= opDurable
	return o
}

// durable reports whether o requests a post-write fdatasync.
func (o *Op) durable() bool { return o.opFlags&opDurable != 0 }

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
	return Op{opcode: OpRead | opFixed, f: f, buf: buf, offset: offset}
}

// WriteFixedOp constructs a positioned write using a pre-registered fixed buffer.
func WriteFixedOp(f *os.File, buf []byte, offset int64) Op {
	return Op{opcode: OpWrite | opFixed, f: f, buf: buf, offset: offset}
}

// ReadvOp constructs a vectored positioned read: bufs are filled from
// consecutive file offsets starting at offset (preadv semantics). Result.N is
// the total bytes read across all buffers.
func ReadvOp(f *os.File, bufs [][]byte, offset int64) Op {
	return Op{opcode: OpReadv, f: f, bufs: bufs, offset: offset}
}

// WritevOp constructs a vectored positioned write: bufs are written to
// consecutive file offsets starting at offset (pwritev semantics). Result.N is
// the total bytes written across all buffers.
func WritevOp(f *os.File, bufs [][]byte, offset int64) Op {
	return Op{opcode: OpWritev, f: f, bufs: bufs, offset: offset}
}

// VReadvOp constructs a vectored positioned read against an io_uring virtual
// descriptor.
func VReadvOp(vfd uint32, bufs [][]byte, offset int64) Op {
	return Op{opcode: OpReadv | opVirtual, vfd: vfd, bufs: bufs, offset: offset}
}

// VWritevOp constructs a vectored positioned write against an io_uring virtual
// descriptor.
func VWritevOp(vfd uint32, bufs [][]byte, offset int64) Op {
	return Op{opcode: OpWritev | opVirtual, vfd: vfd, bufs: bufs, offset: offset}
}

// OpenatOp constructs a plain openat operation that opens path and returns the
// new regular file descriptor in Result.N. The caller owns that fd and must
// close it. To open directly into the registered-file table instead — no
// process fd — use VOpenatOp.
//
// The dfd argument is the directory fd passed to openat(2), for example
// unix.AT_FDCWD.
func OpenatOp(dfd int, path string, flags int, mode uint32) Op {
	return Op{
		opcode:   OpOpenat,
		dfd:      dfd,
		path:     append([]byte(path), 0),
		openFlag: flags,
		mode:     mode,
	}
}

// VOpenatOp constructs an openat operation that installs the opened file
// into fd's virtual descriptor slot.
//
// The dfd argument is the regular directory fd passed to openat(2), for
// example unix.AT_FDCWD. vfd is the registered-file table index.
func VOpenatOp(dfd int, path string, flags int, mode uint32, vfd uint32) Op {
	return Op{
		opcode:   OpOpenat | opVirtual,
		vfd:      vfd,
		dfd:      dfd,
		path:     append([]byte(path), 0),
		openFlag: flags,
		mode:     mode,
	}
}

// VCloseOp constructs a close operation for an io_uring virtual descriptor.
func VCloseOp(vfd uint32) Op {
	return Op{opcode: OpClose | opVirtual, vfd: vfd}
}

// CloseOp closes f's descriptor through the scheduler; the scheduler drains f's
// in-flight ops before the close runs. Op holds f for the duration to keep the
// descriptor alive. From here dio owns the close: closing or otherwise using f
// again is a use-after-close, which — as with any descriptor — dio does not
// guard against.
func CloseOp(f *os.File) Op {
	return Op{opcode: OpClose, f: f}
}

// VReadOp constructs a positioned read using an io_uring virtual descriptor.
func VReadOp(vfd uint32, buf []byte, offset int64) Op {
	return Op{opcode: OpRead | opVirtual, vfd: vfd, buf: buf, offset: offset}
}

// VWriteOp constructs a positioned write using an io_uring virtual descriptor.
func VWriteOp(vfd uint32, buf []byte, offset int64) Op {
	return Op{opcode: OpWrite | opVirtual, vfd: vfd, buf: buf, offset: offset}
}

// VFsyncOp constructs an fsync operation using an io_uring virtual descriptor.
func VFsyncOp(vfd uint32) Op {
	return Op{opcode: OpFsync | opVirtual, vfd: vfd}
}

// VFdatasyncOp constructs an fdatasync operation using an io_uring virtual descriptor.
func VFdatasyncOp(vfd uint32) Op {
	return Op{opcode: OpFdatasync | opVirtual, vfd: vfd}
}

// VReadFixedOp constructs a positioned read using both a virtual descriptor
// and a pre-registered fixed buffer.
func VReadFixedOp(vfd uint32, buf []byte, offset int64) Op {
	return Op{opcode: OpRead | opVirtual | opFixed, vfd: vfd, buf: buf, offset: offset}
}

// VWriteFixedOp constructs a positioned write using both a virtual
// descriptor and a pre-registered fixed buffer.
func VWriteFixedOp(vfd uint32, buf []byte, offset int64) Op {
	return Op{opcode: OpWrite | opVirtual | opFixed, vfd: vfd, buf: buf, offset: offset}
}

// FallocateOp constructs an operation that preallocates size bytes of file
// space from offset 0 (fallocate mode 0: reserve blocks and grow the file to
// at least size). It mirrors sys.Fallocate.
func FallocateOp(f *os.File, size int64) Op {
	return Op{opcode: OpFallocate, f: f, length: size}
}

// VFallocateOp constructs a FallocateOp targeting an io_uring virtual
// descriptor. Compose it into an open→fallocate→write chain with Op.Link when the
// file must exist and be sized before its first write; the coordinator imposes no
// slot ordering of its own (see Scheduler.Submit).
func VFallocateOp(vfd uint32, size int64) Op {
	return Op{opcode: OpFallocate | opVirtual, vfd: vfd, length: size}
}

// Link returns a copy of o with next chained after it (IOSQE_IO_LINK): if any op
// in the chain fails, the rest are canceled.
func (o Op) Link(next Op) Op { return o.chain(next, sqeLink) }

// HardLink is like Link but with IOSQE_IO_HARDLINK: a failure does not cancel the
// rest of the chain — each linked op runs regardless of the previous result.
func (o Op) HardLink(next Op) Op { return o.chain(next, sqeHardLink) }

func (o Op) chain(next Op, flag uint8) Op {
	tail := &o
	for tail.linked != nil {
		tail = tail.linked
	}
	tail.sqeFlags |= flag
	tail.linked = &next
	return o
}

func (o *Op) isLinked() bool { return o.linked != nil }

// isVirtual reports whether o addresses its file by virtual-table index (vfd)
// rather than *os.File — i.e. the opVirtual bit is set.
func (o *Op) isVirtual() bool { return o.opcode&opVirtual != 0 }

// isFixed reports whether o uses a pre-registered fixed buffer — the opFixed bit.
func (o *Op) isFixed() bool { return o.opcode&opFixed != 0 }

// kind returns the operation with the opVirtual/opFixed flags masked off, so the
// scheduler switches on the operation regardless of addressing or buffer mode.
func (o *Op) kind() uint8 { return o.opcode &^ (opVirtual | opFixed) }

// Ticket is the per-Submit synchronization handle returned by Scheduler.
type Ticket struct {
	// Intrusive lock-free MPSC link.
	next *Ticket

	// group, when non-nil, is the head of a chain (linked via next) of follower
	// tickets whose writes were coalesced into this leader's single writev. The
	// leader's one completion fans out to the whole group. See coalesce.go.
	group *Ticket

	// The embedded operation.
	Op Op

	// pending is the ticket's single counter, used for three things in order:
	//
	//  1. At submit it holds the op count — how many SQEs the ticket will place —
	//     so the coordinator reserves that many ring slots before placing any
	//     (see filterRunnableAfter, fillSlots).
	//  2. As each linked op's CQE is reaped it is decremented. io_uring posts one
	//     CQE per linked SQE, including canceled ones after an error, so the count
	//     is exact; reaching zero means every op finished and the ticket is done.
	//  3. Release reads zero to know the coordinator is finished with the ticket
	//     and it is safe to return to the pool.
	//
	// wg is not a second counter: it is the blocking-wait latch, set once (Add(1)
	// at submit, Done() when pending hits zero). The count lives in an atomic so
	// Release can read it without blocking; the wait lives in a WaitGroup so a
	// ticket can be pool-reused without a per-Submit allocation.
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
	t.group = nil
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
		if op.result.Err != nil {
			return op.result.Err
		}
	}
	return nil
}

// Result returns the completion outcome of the ticket's root op, valid after
// Wait. For a linked chain this is the first op; Error reports the first error
// across the whole chain.
func (t *Ticket) Result() Result { return t.Op.result }

// N returns the bytes transferred by the ticket's root op (or, for a plain
// openat, the returned file descriptor), valid after Wait.
func (t *Ticket) N() int { return t.Op.result.N }

func countAndValidateOps(op *Op) (int32, error) {
	var n int32
	for p := op; p != nil; p = p.linked {
		switch {
		case p.kind() == OpOpenat:
			// openat carries a path and no *os.File in either addressing mode.
			if len(p.path) <= 1 {
				return 0, fmt.Errorf("iosched: op %d has empty open path", n)
			}
		case p.isVirtual():
			// virtual read/write/fsync/close name a registered slot; the index is
			// range-checked against the table at submission time.
		case p.f == nil:
			return 0, fmt.Errorf("iosched: op %d has nil file", n)
		}
		n++
	}
	return n, nil
}

func clearResults(op *Op) {
	for p := op; p != nil; p = p.linked {
		p.result = Result{}
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
