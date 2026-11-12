//go:build linux

package ringo

//go:generate go run ./internal/uapigen

import (
	"fmt"
	"math"
	"sync"
)

const (
	maxIovecs        = 1024
	inlineIovecCount = 4
)

// Op is an opaque io_uring operation. Every implementation owns its typed Go
// arguments and knows how to write its kernel submission queue entry.
//
// Drain configures an operation before submission. A successful Push or
// PushLinked transfers the Op to the Ring permanently; afterward the caller must
// discard every interface copy and use only its Handle. A push that returns an
// error leaves ownership with the caller.
//
// Go cannot express that transfer, and Ringo does not police it. An operation
// pooled with WithAlloc returns to that OpAlloc once the Ring releases its final
// completion, so a retained alias can come to refer to an unrelated operation.
// Reusing a pushed Op is undefined, not diagnosed.
type Op interface {
	op()
	release()
	opcode() rawOpcode
	validate(*Ring) error
	prepare(*rawSQE)
	Drain()
}

// Discard relinquishes a caller-owned, unsubmitted operation, returning pooled
// state to its OpAlloc. Discard all aliases afterward. Use it when abandoning a
// request before Push, or after Push returns an error and leaves ownership with
// the caller. A successful Push transfers that responsibility to the Ring.
func Discard(op Op) {
	if op != nil {
		op.release()
	}
}

// OpAlloc recycles reads, writes, vectored I/O, device commands, fsyncs, opens,
// and closes. Pass it with WithAlloc; the Ring returns each pooled operation
// after reaping its final completion. Other operation kinds use ordinary Go
// allocation. Each pooled operation returns to the OpAlloc that supplied it.
//
// An OpAlloc is safe to use from any goroutine. That matters because the
// goroutine returning an operation is not necessarily the one that built it:
// Reap is what releases an operation back to its OpAlloc, and Ring calls only
// have to be serialized, not confined to one goroutine.
//
// Each pool grows to its peak concurrent use. The zero value is ready to use.
type OpAlloc struct {
	// mu guards the free lists shared by constructors and completion reaping.
	mu sync.Mutex
	freeListAlloc
}

// WithAlloc selects alloc for the operation kinds pooled by OpAlloc.
func WithAlloc(alloc *OpAlloc) OpOption {
	return OpOption{alloc: alloc}
}

// LinkType selects the failure semantics for a chain queued by PushLinked.
type LinkType uint8

const (
	// LinkSoft cancels the remaining operations when one operation fails.
	LinkSoft LinkType = LinkType(rawSqeIOLink)

	// LinkHard continues the remaining operations when one operation fails.
	LinkHard LinkType = LinkType(rawSqeIOHardlink)
)

// Link is one edge in a PushLinked chain. Type controls the edge from the
// preceding operation to Next.
type Link struct {
	Type LinkType
	Next Op
}

// Then constructs a link from the preceding operation to next.
func Then(linkType LinkType, next Op) Link {
	return Link{Type: linkType, Next: next}
}

// opBase is the only state common to all operations. It carries the SQE drain
// flag, a constructor error, and the OpAlloc release must return the object to;
// operation arguments remain in their concrete operation type.
type opBase struct {
	sqeFlags rawSQEFlags
	invalid  error
	alloc    *OpAlloc
}

func (opBase) op() {
}

func (base opBase) validate() error {
	if base.invalid == nil {
		return nil
	}
	return fmt.Errorf("ringo: invalid operation: %w", base.invalid)
}

func (base *opBase) fail(err error) {
	if err != nil && base.invalid == nil {
		base.invalid = err
	}
}

// Drain marks the operation with IOSQE_IO_DRAIN. It applies to an Op the caller
// still owns; a pushed Op has already written its SQE.
func (base *opBase) Drain() {
	base.sqeFlags |= rawSqeIODrain
}

func validateHandle(ring *Ring, handle Handle) error {
	if handle.ring != ring.id {
		return ErrWrongRing
	}
	return nil
}

func fitsInt32(value int) bool {
	return int64(value) >= math.MinInt32 && int64(value) <= math.MaxInt32
}

// nopOp has no arguments beyond its operation flags.
type nopOp struct {
	opBase
}

func (op *nopOp) release() {}

func (op *nopOp) opcode() rawOpcode {
	return rawOpNop
}

func (op *nopOp) validate(*Ring) error {
	return op.opBase.validate()
}

func (op *nopOp) prepare(sqe *rawSQE) {
	*sqe = rawSQE{Opcode: uint8(rawOpNop), Fd: -1}
	sqe.Flags |= uint8(op.sqeFlags)
}

// Nop constructs an operation that performs no I/O.
// liburing: io_uring_prep_nop - https://man7.org/linux/man-pages/man3/io_uring_prep_nop.3.html
func Nop(options ...OpOption) Op {
	return new(nopOp)
}

// freeList recycles the objects of one operation type. Its zero value is an
// empty list whose get allocates.
//
// The owning OpAlloc supplies whatever mutual exclusion applies, so a freeList
// has none of its own and is never used without one.
type freeList[T any] struct {
	free []*T
}

// get returns a zeroed operation, either recycled or newly allocated.
// Constructors rely on that: they assign only the fields they care about
// rather than writing a whole struct literal, so anything a previous operation
// left behind -- a stale buffer, a Drain flag, a constructor error -- must
// already be gone. release guarantees it by resetting before parking.
func (list *freeList[T]) get(alloc *OpAlloc) *T {
	var op *T
	alloc.mu.Lock()
	if count := len(list.free); count != 0 {
		op = list.free[count-1]
		list.free[count-1] = nil
		list.free = list.free[:count-1]
	}
	alloc.mu.Unlock()
	if op == nil {
		return new(T)
	}
	return op
}

func (list *freeList[T]) put(op *T, alloc *OpAlloc) {
	alloc.mu.Lock()
	list.free = append(list.free, op)
	alloc.mu.Unlock()
}

type freeListAlloc struct {
	commands     freeList[uringCmdOp]
	reads        freeList[readOp]
	writes       freeList[writeOp]
	readvs       freeList[readvOp]
	writevs      freeList[writevOp]
	fsyncs       freeList[fsyncOp]
	openAts      freeList[openAtOp]
	openAt2s     freeList[openAt2Op]
	closeDirects freeList[closeDirectOp]
	closeFDs     freeList[closeFDOp]
}

// The constructors below draw one operation from alloc, or allocate when the
// caller supplied none. A nil receiver is that no-allocator case: the operation
// records no OpAlloc, so its release leaves it to the garbage collector.

func (alloc *OpAlloc) newUringCmdOp() *uringCmdOp {
	if alloc == nil {
		return new(uringCmdOp)
	}
	op := alloc.commands.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newReadOp() *readOp {
	if alloc == nil {
		return new(readOp)
	}
	op := alloc.reads.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newWriteOp() *writeOp {
	if alloc == nil {
		return new(writeOp)
	}
	op := alloc.writes.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newReadvOp() *readvOp {
	if alloc == nil {
		return new(readvOp)
	}
	op := alloc.readvs.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newWritevOp() *writevOp {
	if alloc == nil {
		return new(writevOp)
	}
	op := alloc.writevs.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newFsyncOp() *fsyncOp {
	if alloc == nil {
		return new(fsyncOp)
	}
	op := alloc.fsyncs.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newOpenAtOp() *openAtOp {
	if alloc == nil {
		return new(openAtOp)
	}
	op := alloc.openAts.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newOpenAt2Op() *openAt2Op {
	if alloc == nil {
		return new(openAt2Op)
	}
	op := alloc.openAt2s.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newCloseDirectOp() *closeDirectOp {
	if alloc == nil {
		return new(closeDirectOp)
	}
	op := alloc.closeDirects.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newCloseFDOp() *closeFDOp {
	if alloc == nil {
		return new(closeFDOp)
	}
	op := alloc.closeFDs.get(alloc)
	op.alloc = alloc
	return op
}

// OpOption configures an operation at construction. It is a plain value, so
// passing one allocates nothing.
type OpOption struct {
	alloc *OpAlloc
}

func optionAlloc(options []OpOption) *OpAlloc {
	var alloc *OpAlloc
	for _, option := range options {
		if option.alloc != nil {
			alloc = option.alloc
		}
	}
	return alloc
}
