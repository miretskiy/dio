//go:build linux

package ringo

//go:generate go run ./internal/uapigen

import (
	"fmt"
	"math"
)

const (
	maxIovecs        = 1024
	inlineIovecCount = 4
)

// Op is an opaque io_uring operation. Every implementation owns its typed Go
// arguments and knows how to write its kernel submission queue entry.
//
// Drain configures an operation before submission. A successful Push or
// PushLinked consumes the Op permanently; afterward the caller must discard
// every interface copy and use only its Handle. A push that returns an error
// leaves ownership with the caller. Ringo panics on detected reuse, but callers
// must not rely on detection because internal storage may be recycled.
type Op interface {
	op()
	markConsumed()
	release()
	opcode() rawOpcode
	validate(*Ring) error
	prepare(*rawSQE)
	Drain()
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

// opBase is the only state common to all operations. It carries ownership
// state, the SQE drain flag, and a constructor error; operation arguments
// remain in their concrete operation type.
type opBase struct {
	sqeFlags rawSQEFlags
	consumed bool
	invalid  error
}

func (opBase) op() {
}

func (*opBase) release() {
}

func (base opBase) validate() error {
	if base.consumed {
		panic("ringo: use of consumed operation")
	}
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

func (base *opBase) markConsumed() {
	if base.consumed {
		panic("ringo: use of consumed operation")
	}
	base.consumed = true
}

// Drain marks the operation with IOSQE_IO_DRAIN.
func (base *opBase) Drain() {
	if base.consumed {
		panic("ringo: use of consumed operation")
	}
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
func Nop() Op {
	return &nopOp{}
}
