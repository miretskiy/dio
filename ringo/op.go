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
// constructed WithAlloc returns to that OpAlloc once the Ring releases its final
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

// OpAlloc recycles operation objects. Pass one to a constructor with WithAlloc
// and the object returns to it once the Ring reaps that operation's final
// completion, ready for the next constructor to reuse. Leave it out and the
// operation is allocated and left to the garbage collector, so a caller
// recycles the operations worth recycling and ignores the rest. An operation
// returns only to the OpAlloc it came from.
//
// An OpAlloc is safe to use from any goroutine. That matters because the
// goroutine returning an operation is not necessarily the one that built it:
// Reap is what releases an operation back to its OpAlloc, and Ring calls only
// have to be serialized, not confined to one goroutine.
//
// It holds at most as many objects of each kind as the Ring can have operations
// in flight, so it needs no sizing. The zero value is ready to use.
type OpAlloc struct {
	// mu guards every free list. One lock covers all of them rather than one
	// per operation type: the only contention available is between a reaping
	// goroutine and whoever is building the next operation, and a chain long
	// enough to recycle many types at once does not occur.
	mu sync.Mutex
	freeListAlloc
}

// WithAlloc draws the operation from alloc rather than allocating it.
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

func (op *nopOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.nops.put(op, alloc)
}

func (op *nopOp) reset() { *op = nopOp{} }

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
	alloc := optionAlloc(options)
	return alloc.newNopOp()
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
	nops           freeList[nopOp]
	reads          freeList[readOp]
	writes         freeList[writeOp]
	readvs         freeList[readvOp]
	writevs        freeList[writevOp]
	fsyncs         freeList[fsyncOp]
	fallocates     freeList[fallocateOp]
	openAts        freeList[openAtOp]
	openAt2s       freeList[openAt2Op]
	statxes        freeList[statxOp]
	ftruncates     freeList[ftruncateOp]
	closeDirects   freeList[closeDirectOp]
	closeFDs       freeList[closeFDOp]
	timeouts       freeList[timeoutOp]
	linkTimeouts   freeList[linkTimeoutOp]
	timeoutRemoves freeList[timeoutRemoveOp]
	timeoutUpdates freeList[timeoutUpdateOp]
	cancels        freeList[cancelOp]
	cancelFDs      freeList[cancelFDOp]
	pollAdds       freeList[pollAddOp]
	pollRemoves    freeList[pollRemoveOp]
}

// The constructors below draw one operation from alloc, or allocate when the
// caller supplied none. A nil receiver is that no-allocator case: the operation
// records no OpAlloc, so its release leaves it to the garbage collector.

func (alloc *OpAlloc) newNopOp() *nopOp {
	if alloc == nil {
		return new(nopOp)
	}
	op := alloc.nops.get(alloc)
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

func (alloc *OpAlloc) newFallocateOp() *fallocateOp {
	if alloc == nil {
		return new(fallocateOp)
	}
	op := alloc.fallocates.get(alloc)
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

func (alloc *OpAlloc) newStatxOp() *statxOp {
	if alloc == nil {
		return new(statxOp)
	}
	op := alloc.statxes.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newFtruncateOp() *ftruncateOp {
	if alloc == nil {
		return new(ftruncateOp)
	}
	op := alloc.ftruncates.get(alloc)
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

func (alloc *OpAlloc) newTimeoutOp() *timeoutOp {
	if alloc == nil {
		return new(timeoutOp)
	}
	op := alloc.timeouts.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newLinkTimeoutOp() *linkTimeoutOp {
	if alloc == nil {
		return new(linkTimeoutOp)
	}
	op := alloc.linkTimeouts.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newTimeoutRemoveOp() *timeoutRemoveOp {
	if alloc == nil {
		return new(timeoutRemoveOp)
	}
	op := alloc.timeoutRemoves.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newTimeoutUpdateOp() *timeoutUpdateOp {
	if alloc == nil {
		return new(timeoutUpdateOp)
	}
	op := alloc.timeoutUpdates.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newCancelOp() *cancelOp {
	if alloc == nil {
		return new(cancelOp)
	}
	op := alloc.cancels.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newCancelFDOp() *cancelFDOp {
	if alloc == nil {
		return new(cancelFDOp)
	}
	op := alloc.cancelFDs.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newPollAddOp() *pollAddOp {
	if alloc == nil {
		return new(pollAddOp)
	}
	op := alloc.pollAdds.get(alloc)
	op.alloc = alloc
	return op
}

func (alloc *OpAlloc) newPollRemoveOp() *pollRemoveOp {
	if alloc == nil {
		return new(pollRemoveOp)
	}
	op := alloc.pollRemoves.get(alloc)
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
