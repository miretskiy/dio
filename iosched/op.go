package iosched

import (
	"os"
	"sync"

	"github.com/miretskiy/dio/mempool"
)

// Opcode identifies the type of I/O operation.
type Opcode uint8

const (
	OpRead      Opcode = iota // pread — positioned read
	OpWrite                   // pwrite — positioned write
	OpFsync                   // fsync — flush data and metadata
	OpFdatasync               // fdatasync — flush data (skip metadata unless needed)
	OpFallocate               // fallocate — preallocate or punch holes
	OpReadFixed               // pread via IORING_OP_READ_FIXED (pre-registered buffer)
	OpWriteFixed              // pwrite via IORING_OP_WRITE_FIXED (pre-registered buffer)
)

// Op describes a single I/O operation submitted to a [Scheduler].
//
// Op is a plain value type — construct with a constructor such as [WriteOp],
// [ReadFixedOp], etc., then optionally chain [Op.Linked] or [Op.HardLinked]
// before placing it into a [Submission].
//
//	ops := []Op{
//	    WriteOp(f, buf, 0).Linked(), // must complete before the next op
//	    FdatasyncOp(f),
//	}
//	sub := PrepareWaitableSubmission(ops)
//	if err := s.Submit(&sub); err != nil { ... }
//	<-sub.Done()
//	for i := range sub.Ops {
//	    res := sub.Ops[i].Result()
//	    ...
//	}
//
// # File lifetime
//
// Op borrows the *os.File without taking ownership. The File (and any buffer
// or slot referenced by the Op) must remain valid until the Submission's
// Done channel has been received from (for waitable submissions) or until
// the caller is otherwise certain the scheduler has completed all ops.
//
// In practice the scheduler keeps the [Submission] (and through it, the Op
// slice and its referenced buffers/files) reachable from its own internal
// state for the kernel's window of use, so the caller is not required to
// keep the *Submission reachable for buffer-lifetime reasons. Callers retain
// the *Submission only when they need to read [Op.Result] after completion.
//
// Always pass *os.File, never a raw fd: the fd can be closed and recycled by
// the runtime before the kernel completes the I/O if the File is GC'd first.
//
// # Fixed-buffer ops
//
// [ReadFixedOp] and [WriteFixedOp] use slots from a [mempool.SlabPool]
// pre-registered with the ring via [RegisterDMASlab]. The kernel keeps the
// slab pinned for the ring lifetime, avoiding per-I/O DMA setup overhead.
//
// Acquire a slot before constructing the op; release it after Done fires:
//
//	slot, err := pool.Acquire()
//	if err != nil { ... }
//	defer slot.Release()                       // released when function exits
//	op := WriteFixedOp(f, slot, offset)
//	sub := PrepareWaitableSubmission([]Op{op})
//	s.Submit(&sub)
//	<-sub.Done()                               // slot.Release() fires after this
//
// # Linking (ordered chains)
//
// Ops submitted together can be ordered using SQE links. A linked op is held
// by the kernel until its predecessor completes:
//
//   - [Op.Linked]: soft link — if this op fails, the next is cancelled (ECANCELED).
//   - [Op.HardLinked]: hard link — the next op runs regardless of this op's result.
//
// Only intermediate ops in a chain carry a link flag; the last op must not be
// linked. Example — atomic write-then-sync:
//
//	WriteOp(f, data, off).Linked(), // run first; cancel sync on failure
//	FdatasyncOp(f),                 // run second (no link on final op)
type Op struct {
	opcode   Opcode
	f        *os.File     // always set; retained by Submission until Done fires
	buf      []byte       // read/write buffer; nil for fsync/fallocate
	slot     mempool.Slot // non-zero for fixed-buffer ops; anchors slot lifetime
	offset   int64        // file offset for read/write/fallocate
	length   int64        // range length for fallocate
	opFlags  uint32       // op-specific flags (fallocate mode)
	sqeFlags uint8        // SQE linking flags (sqeLink, sqeHardLink)
	result   Result       // populated by scheduler before Submission.Done is closed
}

// SQE linking flag bits stored in Op.sqeFlags.
const (
	sqeLink     uint8 = 1 << 0 // soft link to next op (IOSQE_IO_LINK)
	sqeHardLink uint8 = 1 << 1 // hard link to next op (IOSQE_IO_HARDLINK)
)

// ReadOp constructs a positioned-read operation.
// f and buf must remain valid until the enclosing Submission's Done fires.
func ReadOp(f *os.File, buf []byte, offset int64) Op {
	return Op{opcode: OpRead, f: f, buf: buf, offset: offset}
}

// WriteOp constructs a positioned-write operation.
// f and buf must remain valid until the enclosing Submission's Done fires.
func WriteOp(f *os.File, buf []byte, offset int64) Op {
	return Op{opcode: OpWrite, f: f, buf: buf, offset: offset}
}

// FsyncOp constructs a full fsync operation (flushes data and metadata).
// f must remain valid until the enclosing Submission's Done fires.
func FsyncOp(f *os.File) Op {
	return Op{opcode: OpFsync, f: f}
}

// FdatasyncOp constructs an fdatasync operation (flushes data; skips
// metadata unless required for data retrieval).
// f must remain valid until the enclosing Submission's Done fires.
func FdatasyncOp(f *os.File) Op {
	return Op{opcode: OpFdatasync, f: f}
}

// FallocateOp constructs a fallocate operation.
// mode is the FALLOC_FL_* flags (0 = preallocate and zero-fill).
// f must remain valid until the enclosing Submission's Done fires.
func FallocateOp(f *os.File, mode uint32, offset, length int64) Op {
	return Op{opcode: OpFallocate, f: f, offset: offset, length: length, opFlags: mode}
}

// ReadFixedOp constructs a positioned-read using a pre-registered fixed buffer.
// slot must be obtained from the scheduler's [mempool.SlabPool].
// If the op is submitted, slot and f must remain valid until the enclosing
// Submission's Done fires. If the op is never submitted, call slot.Release()
// to return the slot to the pool.
func ReadFixedOp(f *os.File, slot mempool.Slot, offset int64) Op {
	return Op{opcode: OpReadFixed, f: f, buf: slot.Data, slot: slot, offset: offset}
}

// WriteFixedOp constructs a positioned-write using a pre-registered fixed buffer.
// slot must be obtained from the scheduler's [mempool.SlabPool].
// If the op is submitted, slot and f must remain valid until the enclosing
// Submission's Done fires. If the op is never submitted, call slot.Release()
// to return the slot to the pool.
func WriteFixedOp(f *os.File, slot mempool.Slot, offset int64) Op {
	return Op{opcode: OpWriteFixed, f: f, buf: slot.Data, slot: slot, offset: offset}
}

// Linked returns a copy of op with a soft link to the next op in the
// submission slice. If this op fails, the next linked op is cancelled.
// The final op in a chain must NOT be linked.
func (o Op) Linked() Op {
	o.sqeFlags = (o.sqeFlags &^ sqeHardLink) | sqeLink
	return o
}

// HardLinked returns a copy of op with a hard link to the next op.
// Unlike [Op.Linked], the next op executes regardless of this op's result.
func (o Op) HardLinked() Op {
	o.sqeFlags = (o.sqeFlags &^ sqeLink) | sqeHardLink
	return o
}

// isLinked reports whether this op links to the next one (soft or hard).
func (o Op) isLinked() bool {
	return o.sqeFlags&(sqeLink|sqeHardLink) != 0
}

// Result returns the outcome of this op. Valid only after the enclosing
// [Submission]'s [Submission.Done] channel has been received from, or after
// the scheduler has shut down. Reading earlier is a race.
func (o *Op) Result() Result { return o.result }

// Result holds the outcome of a single completed Op.
//
// For read/write ops, N is the number of bytes transferred.
// For fsync/fdatasync/fallocate, N is 0 on success.
// Err is non-nil on failure (typically a [syscall.Errno]).
type Result struct {
	N   int
	Err error
}

// Ticket is the per-Submit handle. Acquire with [NewTicket]; populate Ops;
// call [Scheduler.Submit]; call [Ticket.Wait] to block for completion;
// then [Ticket.Release] to return the Ticket to the pool.
//
// Lifetime contract (the caller MUST follow):
//
//   - Acquire via [NewTicket].
//   - Set Ops; pass to Submit.
//   - Call Wait exactly once; only after Wait returns may you read
//     op.Result(), Release, or reuse the Ticket.
//   - Call Release (typically via defer) once you are done reading
//     results. Forgetting Release leaks one Ticket to the GC per omission
//     — soft failure, but a real one.
//
// Do NOT fire-and-forget: dropping the Ticket without Wait is unsafe
// (the scheduler still owns a reference until completion; failing to wait
// allows the scheduler and caller to race on Op state).
//
// The scheduler uses a [sync.WaitGroup] internally; Wait composes neither
// with select{} nor with context cancellation. Callers needing cancellation
// must accept the limitation or wrap Wait in their own goroutine.
type Ticket struct {
	// Ops is the slice of ops to execute. Caller-owned: keep it alive
	// (don't mutate, don't recycle) from Submit until Wait returns. The
	// scheduler writes each op.result before signaling completion.
	Ops []Op

	// unexported coordinator state. Single-writer (coordinator goroutine);
	// safe to read after Wait returns via wg's happens-before.
	pending int32
	nextOp  uint16
	wg      sync.WaitGroup
}

// Wait blocks until all ops in the Ticket have completed (or the scheduler
// is shutting down — in which case each op carries an errSchedulerClosed
// in its Result).
//
// Call exactly once per Submit. Calling Wait without a prior successful
// Submit blocks forever (the WaitGroup counter is 0 with no pending Add,
// so Wait returns immediately — but that's only by accident; don't rely on
// it).
func (t *Ticket) Wait() { t.wg.Wait() }

// Release returns the Ticket to the pool. Call after Wait. Safe to defer
// immediately after [NewTicket]:
//
//	t := iosched.NewTicket()
//	defer t.Release()
//	t.Ops = ops
//	sched.Submit(t)
//	t.Wait()
//	// read t.Ops[i].Result()
//
// Calling Release before Wait, or twice, is a caller bug.
func (t *Ticket) Release() {
	t.Ops = nil
	// pending and nextOp are 0 after a successful Wait (the WG counter
	// hitting 0 implies all ops accounted for).
	ticketPool.Put(t)
}

var ticketPool sync.Pool

// NewTicket returns a Ticket from the pool. The Ticket is zero-valued
// except for the embedded WaitGroup, which is at counter 0. Caller MUST
// call [Ticket.Release] (typically via defer) after [Ticket.Wait] returns.
func NewTicket() *Ticket {
	if t, _ := ticketPool.Get().(*Ticket); t != nil {
		return t
	}
	return &Ticket{}
}
