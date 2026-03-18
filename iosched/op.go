package iosched

import "os"

// Opcode identifies the type of I/O operation.
type Opcode uint8

const (
	OpRead      Opcode = iota // pread — positioned read
	OpWrite                   // pwrite — positioned write
	OpFsync                   // fsync — flush data and metadata
	OpFdatasync               // fdatasync — flush data (skip metadata unless needed)
	OpFallocate               // fallocate — preallocate or punch holes
)

// Op describes a single I/O operation.
//
// Op is a value type — construct with [ReadOp], [WriteOp], [FsyncOp],
// [FdatasyncOp], or [FallocateOp], then optionally chain [Op.Linked] or
// [Op.HardLinked].
//
// # File lifetime
//
// Op borrows the *os.File; it does not take ownership. The File (and any
// buffer referenced by the Op) must remain valid until [Scheduler.Wait]
// returns for the ticket under which the Op was submitted. The scheduler
// retains the Op internally and keeps the File reachable for the GC until
// that point.
//
// Passing a raw fd (obtained from f.Fd()) and then letting f be GC'd is
// incorrect: the fd can be closed and reused before the kernel finishes the
// I/O. Always pass the *os.File.
//
// # Linking
//
// When multiple Ops are passed to [Scheduler.Submit], ops with [Op.Linked]
// create a soft dependency on the next op in the slice: the next op is not
// dispatched until this one completes. If this op fails, subsequent linked
// ops are cancelled (ECANCELED). [Op.HardLinked] is similar but the next op
// runs regardless of this op's result.
type Op struct {
	opcode   Opcode
	f        *os.File // always set; retained by group until Wait returns
	buf      []byte   // read/write buffer; nil for fsync/fallocate
	offset   int64    // file offset for read/write/fallocate
	length   int64    // range length for fallocate
	opFlags  uint32   // op-specific flags (fallocate mode)
	sqeFlags uint8    // SQE linking flags (sqeLink, sqeHardLink)
}

// SQE linking flag bits stored in Op.sqeFlags.
const (
	sqeLink     uint8 = 1 << 0 // soft link to next op (IOSQE_IO_LINK)
	sqeHardLink uint8 = 1 << 1 // hard link to next op (IOSQE_IO_HARDLINK)
)

// ReadOp constructs a positioned-read operation.
// f and buf must remain valid until [Scheduler.Wait] returns.
func ReadOp(f *os.File, buf []byte, offset int64) Op {
	return Op{opcode: OpRead, f: f, buf: buf, offset: offset}
}

// WriteOp constructs a positioned-write operation.
// f and buf must remain valid until [Scheduler.Wait] returns.
func WriteOp(f *os.File, buf []byte, offset int64) Op {
	return Op{opcode: OpWrite, f: f, buf: buf, offset: offset}
}

// FsyncOp constructs a full fsync operation (flushes data and metadata).
// f must remain valid until [Scheduler.Wait] returns.
func FsyncOp(f *os.File) Op {
	return Op{opcode: OpFsync, f: f}
}

// FdatasyncOp constructs an fdatasync operation (flushes data; skips
// metadata unless required for data retrieval).
// f must remain valid until [Scheduler.Wait] returns.
func FdatasyncOp(f *os.File) Op {
	return Op{opcode: OpFdatasync, f: f}
}

// FallocateOp constructs a fallocate operation.
// mode is the FALLOC_FL_* flags (0 = preallocate and zero-fill).
// f must remain valid until [Scheduler.Wait] returns.
func FallocateOp(f *os.File, mode uint32, offset, length int64) Op {
	return Op{opcode: OpFallocate, f: f, offset: offset, length: length, opFlags: mode}
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

// Result holds the outcome of a single completed Op.
//
// For read/write ops, N is the number of bytes transferred.
// For fsync/fdatasync/fallocate, N is 0 on success.
// Err is non-nil on failure (typically a [syscall.Errno]).
type Result struct {
	N   int
	Err error
}

// Ticket is a zero-allocation completion token returned by [Scheduler.Submit].
//
// Ticket is a value type (uint64) — store on the stack, pass by value.
// Call [Scheduler.Wait] exactly once; a second call returns [ErrTicketConsumed].
type Ticket uint64

func makeTicket(groupIdx uint16, gen uint32) Ticket {
	return Ticket(uint64(gen)<<16 | uint64(groupIdx))
}

func (t Ticket) groupIdx() uint16 { return uint16(t) }
func (t Ticket) gen() uint32      { return uint32(t >> 16) }
