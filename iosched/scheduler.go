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
	// URingScheduler can reject excess outstanding I/O with ErrRejected when
	// its shared DiskPolicy is full. Byte/count limits are disabled by default;
	// the default service-time allowance is 1.5 ms per class.
	//
	// After an accepted submission, Ticket.Wait returns its result and
	// Ticket.Done offers completion to a select; SubmitNotify also runs a brief
	// function on completion, without a waiting goroutine. Waiting is optional
	// when the caller does not need the result. A Submit error means the
	// operation was not accepted and the returned Ticket is invalid; execution
	// errors are returned by a valid Ticket's Wait.
	// Reads absorb EINTR and may complete with a short count. Writes are not
	// retried; a short write reports io.ErrShortWrite together with its count.
	//
	// Operations in a Link or HardLink chain execute in order. Link cancels the
	// remaining chain after a failure; HardLink continues it. A chain holds at
	// most eight operations. A final Durable write's sync may be shared with
	// other durable writes on its file; a Durable write followed by other
	// operations gets its own fdatasync before them, which counts as one of
	// the eight.
	//
	// Separate submissions are unordered except for DrainOp:
	//   - A submission containing DrainOp is held whole, every
	//     operation of its chain, until the operations previously accepted on
	//     that file have completed and their completion functions (SubmitNotify)
	//     have returned. Earlier operations in the same linked chain are ordered
	//     before the barrier by the chain and are not part of that drain. So a
	//     completion function may still change a buffer that the held chain
	//     will write. A Durable write remains active through its
	//     synthesized fdatasync, so a following barrier waits for the flush too.
	//     Callers must stop using a regular file before DrainOp and may close it
	//     after the Ticket completes.
	//
	// These rules use the coordinator's acceptance order. Submit calls racing from
	// different goroutines do not establish an application-visible order.
	Submit(op Op) (Ticket, error)

	// Close gives accepted work a bounded drain, then cancels and closes.
	// Work that has not completed may finish with a scheduler-closed error.
	// Close must be called exactly once. Callers must stop submitting first;
	// racing Close with Submit is not supported.
	io.Closer
}
