package iosched

import (
	"os"

	"github.com/miretskiy/dio/mempool"
)

// Opcode identifies the type of I/O operation.
type Opcode uint8

const (
	OpRead       Opcode = iota // pread — positioned read
	OpWrite                    // pwrite — positioned write
	OpFsync                    // fsync — flush data and metadata
	OpFdatasync                // fdatasync — flush data (skip metadata unless needed)
	OpFallocate                // fallocate — preallocate or punch holes
	OpReadFixed                // pread via IORING_OP_READ_FIXED (pre-registered buffer)
	OpWriteFixed               // pwrite via IORING_OP_WRITE_FIXED (pre-registered buffer)
)

// Op describes a single I/O operation submitted to a [Scheduler].
//
// Op is a plain value type — construct with a constructor such as [WriteOp],
// [ReadFixedOp], etc., then optionally chain [Op.Linked] or [Op.HardLinked]
// before passing into [Scheduler.Submit].
//
//	ops := []Op{
//	    WriteOp(f, buf, 0).Linked(), // must complete before the next op
//	    FdatasyncOp(f),
//	}
//	if err := s.Submit(ops); err != nil { ... }
//	for i := range ops {
//	    res := ops[i].Result()
//	    ...
//	}
//
// # File lifetime
//
// Op borrows the *os.File without taking ownership. The File (and any buffer
// or slot referenced by the Op) must remain valid until [Scheduler.Submit]
// returns. Since Submit blocks until all ops complete, the caller's stack
// frame keeps these references alive naturally.
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
//	slot, err := pool.Acquire()
//	if err != nil { ... }
//	defer slot.Release()
//	if err := s.Submit([]Op{WriteFixedOp(f, slot, offset)}); err != nil { ... }
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
	f        *os.File     // always set; retained by caller's slice across Submit
	buf      []byte       // read/write buffer; nil for fsync/fallocate
	slot     mempool.Slot // non-zero for fixed-buffer ops; anchors slot lifetime
	offset   int64        // file offset for read/write/fallocate
	length   int64        // range length for fallocate
	opFlags  uint32       // op-specific flags (fallocate mode)
	sqeFlags uint8        // SQE linking flags (sqeLink, sqeHardLink)
	result   Result       // populated by scheduler before Submit returns
}

// SQE linking flag bits stored in Op.sqeFlags.
const (
	sqeLink     uint8 = 1 << 0 // soft link to next op (IOSQE_IO_LINK)
	sqeHardLink uint8 = 1 << 1 // hard link to next op (IOSQE_IO_HARDLINK)
)

// ReadOp constructs a positioned-read operation.
// f and buf must remain valid until Submit returns.
func ReadOp(f *os.File, buf []byte, offset int64) Op {
	return Op{opcode: OpRead, f: f, buf: buf, offset: offset}
}

// WriteOp constructs a positioned-write operation.
// f and buf must remain valid until Submit returns.
func WriteOp(f *os.File, buf []byte, offset int64) Op {
	return Op{opcode: OpWrite, f: f, buf: buf, offset: offset}
}

// FsyncOp constructs a full fsync operation (flushes data and metadata).
// f must remain valid until Submit returns.
func FsyncOp(f *os.File) Op {
	return Op{opcode: OpFsync, f: f}
}

// FdatasyncOp constructs an fdatasync operation (flushes data; skips
// metadata unless required for data retrieval).
// f must remain valid until Submit returns.
func FdatasyncOp(f *os.File) Op {
	return Op{opcode: OpFdatasync, f: f}
}

// FallocateOp constructs a fallocate operation.
// mode is the FALLOC_FL_* flags (0 = preallocate and zero-fill).
// f must remain valid until Submit returns.
func FallocateOp(f *os.File, mode uint32, offset, length int64) Op {
	return Op{opcode: OpFallocate, f: f, offset: offset, length: length, opFlags: mode}
}

// ReadFixedOp constructs a positioned-read using a pre-registered fixed buffer.
// slot must be obtained from the scheduler's [mempool.SlabPool].
// If the op is submitted, slot and f must remain valid until Submit returns.
// If the op is never submitted, call slot.Release() to return the slot to the
// pool.
func ReadFixedOp(f *os.File, slot mempool.Slot, offset int64) Op {
	return Op{opcode: OpReadFixed, f: f, buf: slot.Data, slot: slot, offset: offset}
}

// WriteFixedOp constructs a positioned-write using a pre-registered fixed buffer.
// slot must be obtained from the scheduler's [mempool.SlabPool].
// If the op is submitted, slot and f must remain valid until Submit returns.
// If the op is never submitted, call slot.Release() to return the slot to the
// pool.
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

// Result returns the outcome of this op. Valid only after the call to
// [Scheduler.Submit] returns; reading earlier is a race.
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
