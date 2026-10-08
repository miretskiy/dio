// Package iosched provides a common I/O scheduler over synchronous POSIX and
// Linux io_uring backends.
//
// The primary abstraction is Scheduler. On Linux, URingScheduler submits ops
// through io_uring. POSIXScheduler implements the same Submit/Ticket lifecycle
// with blocking POSIX file operations.
package iosched

import (
	"errors"
	"io"
)

var errSchedulerClosed = errors.New("iosched: scheduler closed")

// Scheduler is the common I/O submission interface.
type Scheduler interface {
	// Submit schedules op and returns its completion Ticket. URingScheduler is a
	// non-blocking handoff; POSIXScheduler executes the operation before returning.
	// Neither provides application-level backpressure.
	//
	// After an accepted submission, Ticket.Wait returns its result and
	// Ticket.Done offers completion to a select; SubmitNotify also runs a brief
	// function on completion, without a waiting goroutine. Waiting is optional
	// when the caller does not need the result. A Submit error means the
	// operation was not accepted and the returned Ticket is invalid; execution and
	// asynchronous admission errors are returned by a valid Ticket's Wait.
	// Reads absorb EINTR and may complete with a short count. Writes are not
	// retried; a short write reports io.ErrShortWrite together with its count.
	//
	// Operations in a Link or HardLink chain execute in order. Link cancels the
	// remaining chain after a failure; HardLink continues it. URingScheduler
	// rejects a chain of more than eight operations, or, on a smaller ring, of
	// more than its depth less the one entry it keeps for its doorbell. Durable
	// applies only to standalone writes; put an explicit FdatasyncOp in a linked
	// chain.
	//
	// Separate submissions are unordered except for file lifecycle:
	//   - A submission containing VOpenatOp holds subsequently accepted operations
	//     on the same virtual slot until its entire linked chain completes. This is
	//     an ordering barrier, not error propagation; use Link when followers must
	//     be canceled if open fails.
	//   - A submission containing DrainOp or VCloseOp is held whole, every
	//     operation of its chain, until the operations previously accepted on
	//     that file have completed and their completion functions (SubmitNotify)
	//     have returned. Earlier operations in the same linked chain are ordered
	//     before the barrier by the chain and are not part of that drain. So a
	//     completion function may still change a buffer that the held chain
	//     will write. A standalone Durable write remains active through its
	//     synthesized fdatasync, so a following barrier waits for the flush too.
	//   - Operations later in the same linked chain are ordered after a VCloseOp.
	//     A chain that closes a virtual slot and then opens it again replaces
	//     the slot's file: it is accepted while earlier work on the slot remains
	//     (its close drains that work), and plain operations submitted for the
	//     slot while it is pending wait for it, as they wait for an open.
	//     Otherwise, separate work submitted for a file after its lifecycle
	//     barrier has been accepted but before it completes is unsupported. Callers must stop using
	//     a regular file before DrainOp and may close it after the Ticket completes.
	//
	// These rules use the coordinator's acceptance order. Submit calls racing from
	// different goroutines do not establish an application-visible order.
	Submit(op Op) (Ticket, error)

	// Close shuts down the scheduler and waits for accepted tickets to finish.
	// Work that has not completed may finish with a scheduler-closed error.
	// Close must be called exactly once. Callers must stop submitting first;
	// racing Close with Submit is not supported.
	io.Closer
}
