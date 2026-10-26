package iosched

import (
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
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
	OpClose  // close a vfd slot, or drain operations on a regular file

	OpFsync     // fsync - flush data and metadata
	OpFdatasync // fdatasync - flush data
	OpFallocate // fallocate - preallocate file space
	OpUnlinkat  // unlinkat - remove a directory entry
)

// Opcode flags OR'd into an opcode. opVirtual selects virtual-file addressing
// (the file is a registered-table index, not an *os.File); opFixed selects a
// pre-registered fixed buffer (IORING_OP_*_FIXED). They compose; kind() masks
// both off.
const (
	opFixed   uint8 = 1 << 6
	opVirtual uint8 = 1 << 7
)

// AllocateVFD asks a root VOpenatOp to allocate a free virtual-file slot in the
// coordinator. Its linked I/O operations and final virtual close use AllocateVFD
// to address the same slot. Ticket.Wait returns the allocated index and any chain
// error; Ticket.AllResults distinguishes open success from a follower's failure.
// Use allocation consistently within a scheduler: do not combine it with opens
// into caller-chosen slots. Closing an allocated slot relinquishes its index;
// obtain a new allocation before using a virtual file again.
// Allocation failure completes the open with ENFILE: Link cancels followers,
// while HardLink continues them. Followers using an unallocated slot fail with
// EBADF.
// Submission copies the operands; the coordinator resolves the placeholders
// in that private copy. The caller keeps buffers valid through completion.
const AllocateVFD uint32 = math.MaxUint32

// Op is the caller-facing description of an I/O operation and its followers.
// Submit copies the operation records into a private submission; it does not
// retain the Op. Referenced buffers and files remain caller-owned and must stay
// valid until the ticket completes.
//
// Link and HardLink return independent values; short chains use inline storage.
type Op struct {
	operation
	linked    []operation // overflow beyond the inline slots
	linkCount uint8       // number of inline followers
	linkAlloc [3]operation
	whenDone  func(n int, err error) // construction input from SubmitNotify
}

// operation contains only the operands and flags for one I/O operation.
// It carries neither a chain nor completion state.
type operation struct {
	opcode      uint8
	sqeFlags    uint8
	resultIndex uint8
	f           *os.File
	vfd         uint32
	dfd         int
	path        []byte
	openFlag    int
	mode        uint32
	buf         []byte
	bufs        [][]byte
	offset      int64
	length      int64
	opFlags     uint32
}

// SQE linking flag bits stored in Op.sqeFlags, marking how a linked follower is
// chained after this op.
const (
	sqeLink     uint8 = 1 << 0 // IOSQE_IO_LINK: a failure cancels the rest of the chain
	sqeHardLink uint8 = 1 << 1 // IOSQE_IO_HARDLINK: the chain survives a failure
)

// opFlags bits stored in Op.opFlags.
const (
	opDurable      uint32 = 1 << 0 // flush the write (fdatasync) before the ticket completes
	opAllocatedVFD uint32 = 1 << 1 // root open reserved a virtual-file slot
	opReleaseVFD   uint32 = 1 << 2 // failed root open returns its slot after the chain
	opMustAdmit    uint32 = 1 << 3 // root bypasses admission limits for the whole submission
	opWriteSync    uint32 = 1 << 4 // synthesized fdatasync after a non-final Durable write
)

// MustAdmit exempts the whole submission from every DiskPolicy throttle: it is
// never rejected and never waits for the in-flight service-time allowance. Its
// I/O is not charged, so DiskPolicy.Stats excludes it. Apply it to the root of
// a chain. Other Submit errors (closed scheduler, invalid operands) and
// execution failures remain possible.
func (o Op) MustAdmit() Op {
	o.opFlags |= opMustAdmit
	return o
}

// Durable requests that a write's data be flushed to stable storage before its
// Ticket completes. Call it on the write itself, before linking: on a chain it
// marks the root. Durable has no effect on non-write operations.
//
// A final durable write gets an fdatasync after it completes; the io_uring
// backend shares that sync among the durable writes on the same file that
// completed before it was issued. A durable write followed by other operations
// (write → Durable → close) gets its own fdatasync, linked the way the write
// links to its successor, so those operations run after the flush. That sync
// occupies one of the chain's eight entries and reports into the write's result.
func (o Op) Durable() Op {
	o.opFlags |= opDurable
	return o
}

// durable reports whether o requests a post-write fdatasync.
func (o *operation) durable() bool { return o.opFlags&opDurable != 0 }

func (o operation) syncOp() operation {
	if o.isVirtual() {
		return VFdatasyncOp(o.vfd).operation
	}
	return FdatasyncOp(o.f).operation
}

// ReadOp constructs a positioned-read operation.
func ReadOp(f *os.File, buf []byte, offset int64) Op {
	return Op{operation: operation{opcode: OpRead, f: f, buf: buf, offset: offset}}
}

// WriteOp constructs a positioned-write operation.
func WriteOp(f *os.File, buf []byte, offset int64) Op {
	return Op{operation: operation{opcode: OpWrite, f: f, buf: buf, offset: offset}}
}

// FsyncOp constructs a full fsync operation.
func FsyncOp(f *os.File) Op {
	return Op{operation: operation{opcode: OpFsync, f: f}}
}

// FdatasyncOp constructs an fdatasync operation.
func FdatasyncOp(f *os.File) Op {
	return Op{operation: operation{opcode: OpFdatasync, f: f}}
}

// ReadFixedOp constructs a positioned read using a pre-registered fixed buffer.
func ReadFixedOp(f *os.File, buf []byte, offset int64) Op {
	return Op{operation: operation{opcode: OpRead | opFixed, f: f, buf: buf, offset: offset}}
}

// WriteFixedOp constructs a positioned write using a pre-registered fixed buffer.
func WriteFixedOp(f *os.File, buf []byte, offset int64) Op {
	return Op{operation: operation{opcode: OpWrite | opFixed, f: f, buf: buf, offset: offset}}
}

// ReadvOp constructs a vectored positioned read: bufs are filled from
// consecutive file offsets starting at offset (preadv semantics). Ticket.Wait
// returns the total bytes read across all buffers.
func ReadvOp(f *os.File, bufs [][]byte, offset int64) Op {
	return Op{operation: operation{opcode: OpReadv, f: f, bufs: bufs, offset: offset}}
}

// WritevOp constructs a vectored positioned write: bufs are written to
// consecutive file offsets starting at offset (pwritev semantics). Ticket.Wait
// returns the total bytes written across all buffers.
func WritevOp(f *os.File, bufs [][]byte, offset int64) Op {
	return Op{operation: operation{opcode: OpWritev, f: f, bufs: bufs, offset: offset}}
}

// VReadvOp constructs a vectored positioned read against an io_uring virtual
// descriptor.
func VReadvOp(vfd uint32, bufs [][]byte, offset int64) Op {
	return Op{operation: operation{opcode: OpReadv | opVirtual, vfd: vfd, bufs: bufs, offset: offset}}
}

// VWritevOp constructs a vectored positioned write against an io_uring virtual
// descriptor.
func VWritevOp(vfd uint32, bufs [][]byte, offset int64) Op {
	return Op{operation: operation{opcode: OpWritev | opVirtual, vfd: vfd, bufs: bufs, offset: offset}}
}

// OpenatOp constructs a plain openat operation that opens path and returns the
// new regular file descriptor as Ticket.Wait's integer result. The caller owns
// that fd and must close it. To open directly into the registered-file table
// instead — no process fd — use VOpenatOp.
//
// The dfd argument is the directory fd passed to openat(2), for example
// unix.AT_FDCWD.
func OpenatOp(dfd int, path string, flags int, mode uint32) Op {
	return Op{operation: operation{
		opcode:   OpOpenat,
		dfd:      dfd,
		path:     append([]byte(path), 0),
		openFlag: flags,
		mode:     mode,
	}}
}

// UnlinkatOp removes path relative to dfd. It does not wait for operations on
// already-open files; those descriptors remain usable until closed. Missing
// paths report ENOENT. Keep dfd open until the ticket completes.
func UnlinkatOp(dfd int, path string) Op {
	return Op{operation: operation{opcode: OpUnlinkat, dfd: dfd, path: append([]byte(path), 0)}}
}

// VOpenatOp constructs an openat operation that installs the opened file into
// vfd's virtual descriptor slot. Use AllocateVFD on the root operation to allocate
// a free slot before placing the chain and return its index as the open result.
// The caller orders all uses of the slot with links or completion waits.
// Opening directly into an occupied slot replaces its file, as in io_uring.
//
// The dfd argument is the regular directory fd passed to openat(2), for
// example unix.AT_FDCWD. vfd is the registered-file table index or AllocateVFD.
func VOpenatOp(dfd int, path string, flags int, mode uint32, vfd uint32) Op {
	return Op{operation: operation{
		opcode:   OpOpenat | opVirtual,
		vfd:      vfd,
		dfd:      dfd,
		path:     append([]byte(path), 0),
		openFlag: flags,
		mode:     mode,
	}}
}

// VCloseOp closes a virtual descriptor and returns its slot for allocation on
// success. The caller must finish all other uses of the slot before closing it.
func VCloseOp(vfd uint32) Op {
	return Op{operation: operation{opcode: OpClose | opVirtual, vfd: vfd}}
}

// DrainOp waits for previously accepted operations on f, including the
// fdatasync synthesized by Durable writes. It does not close f; the caller
// retains ownership and may call f.Close after the returned Ticket completes.
//
// DrainOp holds f strongly until completion, so asynchronous operations cannot
// outlive the os.File and no caller-side runtime.KeepAlive is needed. Callers
// must stop submitting work for f before submitting DrainOp.
func DrainOp(f *os.File) Op {
	return Op{operation: operation{opcode: OpClose, f: f}}
}

// VReadOp constructs a positioned read using an io_uring virtual descriptor.
func VReadOp(vfd uint32, buf []byte, offset int64) Op {
	return Op{operation: operation{opcode: OpRead | opVirtual, vfd: vfd, buf: buf, offset: offset}}
}

// VWriteOp constructs a positioned write using an io_uring virtual descriptor.
func VWriteOp(vfd uint32, buf []byte, offset int64) Op {
	return Op{operation: operation{opcode: OpWrite | opVirtual, vfd: vfd, buf: buf, offset: offset}}
}

// VFsyncOp constructs an fsync operation using an io_uring virtual descriptor.
func VFsyncOp(vfd uint32) Op {
	return Op{operation: operation{opcode: OpFsync | opVirtual, vfd: vfd}}
}

// VFdatasyncOp constructs an fdatasync operation using an io_uring virtual descriptor.
func VFdatasyncOp(vfd uint32) Op {
	return Op{operation: operation{opcode: OpFdatasync | opVirtual, vfd: vfd}}
}

// VReadFixedOp constructs a positioned read using both a virtual descriptor
// and a pre-registered fixed buffer.
func VReadFixedOp(vfd uint32, buf []byte, offset int64) Op {
	return Op{operation: operation{opcode: OpRead | opVirtual | opFixed, vfd: vfd, buf: buf, offset: offset}}
}

// VWriteFixedOp constructs a positioned write using both a virtual
// descriptor and a pre-registered fixed buffer.
func VWriteFixedOp(vfd uint32, buf []byte, offset int64) Op {
	return Op{operation: operation{opcode: OpWrite | opVirtual | opFixed, vfd: vfd, buf: buf, offset: offset}}
}

// FallocateOp constructs an operation that preallocates size bytes of file
// space from offset 0 (fallocate mode 0: reserve blocks and grow the file to
// at least size). It mirrors sys.Fallocate.
func FallocateOp(f *os.File, size int64) Op {
	return Op{operation: operation{opcode: OpFallocate, f: f, length: size}}
}

// VFallocateOp constructs a FallocateOp targeting an io_uring virtual
// descriptor. Compose it into an open→fallocate→write chain with Op.Link to fuse a
// slab's creation and first write into one submission. Wait for that chain to
// complete before independently submitting later operations on the slot.
func VFallocateOp(vfd uint32, size int64) Op {
	return Op{operation: operation{opcode: OpFallocate | opVirtual, vfd: vfd, length: size}}
}

// Link returns a copy of o with next chained after it. A failed operation
// cancels its soft-linked followers. Nested chains are flattened.
func (o Op) Link(next ...Op) Op { return o.chain(next, sqeLink) }

// HardLink continues the next operation even when its predecessor fails.
func (o Op) HardLink(next ...Op) Op { return o.chain(next, sqeHardLink) }

func (o Op) chain(next []Op, flag uint8) Op {
	if len(next) == 0 {
		return o
	}
	count := len(o.followers())
	for i := range next {
		count += next[i].opCount()
	}
	out := Op{operation: o.operation, whenDone: o.whenDone}
	// A slice stored back into a value-type Op would point into its old copy.
	// Keep inline links as values and form the slice only while accessing them.
	var links []operation
	if count <= len(out.linkAlloc) {
		out.linkCount = uint8(count)
		links = out.linkAlloc[:count]
	} else {
		out.linked = make([]operation, count)
		links = out.linked
	}
	n := copy(links, o.followers())
	for i := range next {
		tail := &out.operation
		if n != 0 {
			tail = &links[n-1]
		}
		tail.sqeFlags = flag
		links[n] = next[i].operation
		n++
		n += copy(links[n:], next[i].followers())
	}
	return out
}

func (o *Op) followers() []operation {
	if o.linked != nil {
		return o.linked
	}
	return o.linkAlloc[:o.linkCount]
}

func (o *Op) opCount() int          { return 1 + len(o.followers()) }
func (o *operation) isLinked() bool { return o.sqeFlags&(sqeLink|sqeHardLink) != 0 }

// isVirtual reports whether o addresses its file by virtual-table index (vfd)
// rather than *os.File — i.e. the opVirtual bit is set.
func (o *operation) isVirtual() bool { return o.opcode&opVirtual != 0 }

// isFixed reports whether o uses a pre-registered fixed buffer — the opFixed bit.
func (o *operation) isFixed() bool { return o.opcode&opFixed != 0 }

// kind returns the operation with the opVirtual/opFixed flags masked off, so the
// scheduler switches on the operation regardless of addressing or buffer mode.
func (o *operation) kind() uint8 { return o.opcode &^ (opVirtual | opFixed) }

// Result is one operation's byte count (or opened descriptor) and error.
// A linked operation canceled by an earlier failure has its own cancellation error.
type Result struct {
	N   int
	Err error
}

// completion is a submission's result and its completion signal. The
// scheduler records n and err, then calls finish exactly once. The done
// channel is allocated only by Done. Wait uses a WaitGroup directly; completion
// publishes a shared closed channel if no caller needed selectable completion.
type completion struct {
	results []Result // root and followers for linked submissions
	n       int
	err     error
	started time.Time // submission preparation; monotonic clock

	Slat time.Duration // Preparation to ring placement (POSIX: execution starts).
	Clat time.Duration // Ring placement to ticket completion, including any Durable sync.

	wait sync.WaitGroup                // Wait does not allocate a channel
	done atomic.Pointer[chan struct{}] // made only when Done is requested
}

// closedChan is the done channel of a ticket that completed before anyone
// asked for one.
var closedChan = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// finish publishes the result and releases ticket waiters.
func (c *completion) finish() {
	if c.Slat > 0 {
		c.Clat = time.Since(c.started) - c.Slat
	}
	// Exactly one finish races with any number of Done calls. Either finish
	// publishes the shared closed channel, or a Done caller wins publication
	// and finish closes that channel. No mutex or channel allocation on Wait.
	if !c.done.CompareAndSwap(nil, &closedChan) {
		close(*c.done.Load())
	}
	c.wait.Done()
}

// Ticket is the completion handle returned by a successful Scheduler.Submit.
// Slat and Clat may be read after Wait returns or Done is closed. Slat includes
// preparation and scheduler queueing. Clat covers all linked operations and
// any Durable sync, ending before notification or callback execution. Ring
// placement precedes kernel execution, so Clat includes ring submission and
// completion reaping. Both are zero if the submission failed before placement.
type Ticket struct {
	*completion
}

func (c *completion) markIssued() {
	c.Slat = time.Since(c.started)
}

// SubmitNotify submits op to s, like s.Submit, and has whenDone called with
// the result after completion. If Submit fails, whenDone is not called. A forced
// shutdown that cannot reap an operation also leaves its callback uncalled:
// its operands may still be in use. Ticket.Wait and Close report the shutdown
// error. Because whenDone travels with op, it survives any wrapper that forwards
// op to the scheduler it wraps.
//
// whenDone runs on the goroutine that completes the Ticket, after Done is
// closed. For URingScheduler that is the coordinator, which issues and reaps
// all of the scheduler's I/O, so whenDone must be brief and must not block: no
// Wait, no blocking channel operation, no lock held across blocking work.
// Nothing enforces this. It may call Submit or SubmitNotify, which only stage
// work. For POSIXScheduler, whenDone runs inside Submit, before it returns.
func SubmitNotify(s Scheduler, op Op, whenDone func(n int, err error)) (Ticket, error) {
	op.whenDone = whenDone
	return s.Submit(op)
}

// Done returns a channel that is closed when the Ticket completes, for use in
// a select. Like context.Context's Done, the channel is made on first use.
func (t Ticket) Done() <-chan struct{} {
	c := t.completion
	if d := c.done.Load(); d != nil {
		return *d
	}
	d := make(chan struct{})
	if c.done.CompareAndSwap(nil, &d) {
		return d
	}
	return *c.done.Load()
}

// Wait blocks until the Ticket completes and returns the root operation's byte
// count (or opened file descriptor) and the first operation or scheduler error.
func (t Ticket) Wait() (int, error) {
	t.wait.Wait()
	return t.n, t.err
}

// AllResults returns the individual outcomes in submission order, including
// the root. Call after Wait returns or Done closes. For a linked submission it
// exposes the ticket's retained results without allocating; a standalone result
// slice is allocated only when this method is called. Treat the slice as read-only.
// Wait returns the root count and first chain error; each AllResults entry
// contains only that operation's error.
func (t Ticket) AllResults() []Result {
	if t.results != nil {
		return t.results
	}
	return []Result{{N: t.n, Err: t.err}}
}

func opBytes(op *operation) int {
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

func writeResultError(op *operation, n int, err error) error {
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
