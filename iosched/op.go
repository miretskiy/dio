package iosched

import (
	"fmt"
	"io"
	"os"
	"sync"
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

// Op describes one I/O operation. Use Link or HardLink to form a chain.
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
	linked   *Op

	*completion
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

// Durable requests that a standalone write's data be flushed to stable storage
// before its Ticket completes. The io_uring backend appends fdatasync once per
// coalesced run; the POSIX backend syncs inline. For a linked chain, include an
// explicit FdatasyncOp instead. Durable has no effect on non-write operations.
func (o Op) Durable() Op {
	o.opFlags |= opDurable
	return o
}

// durable reports whether o requests a post-write fdatasync.
func (o *Op) durable() bool { return o.opFlags&opDurable != 0 }

func (o Op) syncOp() Op {
	if o.isVirtual() {
		return VFdatasyncOp(o.vfd)
	}
	return FdatasyncOp(o.f)
}

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
// consecutive file offsets starting at offset (preadv semantics). Ticket.N is
// the total bytes read across all buffers.
func ReadvOp(f *os.File, bufs [][]byte, offset int64) Op {
	return Op{opcode: OpReadv, f: f, bufs: bufs, offset: offset}
}

// WritevOp constructs a vectored positioned write: bufs are written to
// consecutive file offsets starting at offset (pwritev semantics). Ticket.N is
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
// new regular file descriptor in Ticket.N. The caller owns that fd and must
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

// VOpenatOp constructs an openat operation that installs the opened file into
// vfd's virtual descriptor slot. Separately submitted operations accepted later
// for the slot wait for the entire chain containing this open to complete, but
// do not inherit its error; use Link when an open failure must cancel its
// follower. If an idle slot already contains a file, a successful open replaces
// and closes it, matching io_uring direct-open semantics.
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

// VCloseOp constructs a close operation for an io_uring virtual descriptor. It
// waits for previously accepted operations on the slot. Do not separately submit
// more work for the slot until the close-containing Ticket completes.
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
// descriptor. Compose it into an open→fallocate→write chain with Op.Link to fuse a
// slab's creation and first write into one submission. Independent later writes to
// the slot need not be linked: the scheduler orders them after the slot's open (see
// Scheduler.Submit).
func VFallocateOp(vfd uint32, size int64) Op {
	return Op{opcode: OpFallocate | opVirtual, vfd: vfd, length: size}
}

// Link returns a copy of o with next chained after it (IOSQE_IO_LINK): if any op
// in the chain fails, the rest are canceled. Any chains in next are flattened
// into the returned chain. Passing the whole tail in one call builds the chain
// with one allocation; repeated calls remain supported but rebuild the immutable
// chain each time.
func (o Op) Link(next ...Op) Op { return o.chain(next, sqeLink) }

// HardLink is like Link but with IOSQE_IO_HARDLINK: a failure does not cancel the
// rest of the chain — each linked op runs regardless of the previous result.
func (o Op) HardLink(next ...Op) Op { return o.chain(next, sqeHardLink) }

func (o Op) chain(next []Op, flag uint8) Op {
	base := o.opCount()
	total := base
	for i := range next {
		total += next[i].opCount()
	}
	if total == base {
		return o
	}

	// The nodes are one allocation, while linked pointers expose the flat chain
	// to the scheduler. Rebuilding makes Op value copies independent: extending a
	// copied Op cannot mutate the source's tail flags or next pointer.
	root := o
	root.linked = nil
	nodes := make([]Op, total-1)
	tail := &root
	nextNode := 0
	appendChain := func(src *Op) {
		for src != nil {
			nodes[nextNode] = *src
			nodes[nextNode].linked = nil
			tail.linked = &nodes[nextNode]
			tail = &nodes[nextNode]
			nextNode++
			src = src.linked
		}
	}
	appendChain(o.linked)
	for i := range next {
		tail.sqeFlags |= flag
		appendChain(&next[i])
	}
	return root
}

func (o *Op) opCount() int {
	count := 0
	for p := o; p != nil; p = p.linked {
		count++
	}
	return count
}

func (o *Op) isLinked() bool { return o.sqeFlags&(sqeLink|sqeHardLink) != 0 }

// isVirtual reports whether o addresses its file by virtual-table index (vfd)
// rather than *os.File — i.e. the opVirtual bit is set.
func (o *Op) isVirtual() bool { return o.opcode&opVirtual != 0 }

// isFixed reports whether o uses a pre-registered fixed buffer — the opFixed bit.
func (o *Op) isFixed() bool { return o.opcode&opFixed != 0 }

// kind returns the operation with the opVirtual/opFixed flags masked off, so the
// scheduler switches on the operation regardless of addressing or buffer mode.
func (o *Op) kind() uint8 { return o.opcode &^ (opVirtual | opFixed) }

type completion struct {
	n    int
	err  error
	done sync.WaitGroup
}

// Ticket is the completion handle returned by a successful Scheduler.Submit.
type Ticket struct {
	*completion
}

func (o *Op) prepareSubmission() Ticket {
	o.completion = new(completion)
	o.done.Add(1)
	return Ticket{o.completion}
}

// Wait blocks until the Ticket completes.
func (t Ticket) Wait() {
	t.done.Wait()
}

// Error returns an operation or scheduler error, valid after Wait.
func (t Ticket) Error() error { return t.err }

// N returns the root operation's byte count or opened file descriptor, valid
// after Wait.
func (t Ticket) N() int { return t.n }

func countAndValidateOps(op *Op, cfg *schedulerConfig) (int32, error) {
	var count int32
	linked := op.linked != nil
	for p := op; p != nil; p = p.linked {
		kind := p.kind()
		if linked && p.durable() && (kind == OpWrite || kind == OpWritev) {
			return 0, fmt.Errorf("iosched: Durable cannot be used in a linked chain; add FdatasyncOp explicitly")
		}
		if kind == OpOpenat {
			if len(p.path) <= 1 {
				return 0, fmt.Errorf("iosched: op %d has empty open path", count)
			}
		}
		if p.isVirtual() {
			if cfg != nil {
				if cfg.vfiles == 0 {
					return 0, fmt.Errorf("iosched: virtual file ops require WithVFiles")
				}
				if p.vfd >= cfg.vfiles {
					return 0, fmt.Errorf("iosched: virtual file index %d outside configured table", p.vfd)
				}
			}
		} else if kind != OpOpenat && p.f == nil {
			return 0, fmt.Errorf("iosched: op %d has nil file", count)
		}
		count++
	}
	return count, nil
}

func opBytes(op *Op) int {
	switch op.kind() {
	case OpWritev, OpReadv:
		total := 0
		for _, buf := range op.bufs {
			total += len(buf)
		}
		return total
	default:
		return len(op.buf)
	}
}

func writeResultError(op *Op, n int, err error) error {
	if err != nil {
		return err
	}
	switch op.kind() {
	case OpWrite, OpWritev:
		if n < opBytes(op) {
			return io.ErrShortWrite
		}
	}
	return nil
}

func recordResult(root, op *Op, n int, err error) {
	if op == root {
		root.n = n
	}
	if err != nil && root.err == nil {
		root.err = err
	}
}
