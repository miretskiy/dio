// Package iosched provides pluggable I/O scheduling for positioned reads and writes.
//
// The primary abstraction is Scheduler. On Linux, URingScheduler submits ops
// through io_uring. POSIXScheduler implements the same Submit/Ticket lifecycle
// with blocking POSIX file operations.
package iosched

import (
	"errors"
	"io"

	"github.com/miretskiy/dio/mempool"
)

var errSchedulerClosed = errors.New("iosched: scheduler closed")

// Scheduler is the common I/O submission interface.
type Scheduler interface {
	// Submit schedules op and returns its completion Ticket. URingScheduler is a
	// non-blocking handoff; POSIXScheduler executes the operation before returning.
	// Neither provides application-level backpressure.
	//
	// After an accepted submission, call Ticket.Wait before Error or N.
	// Waiting is optional when the caller does not need the result. A Submit error
	// means the operation was not accepted and the returned Ticket is invalid;
	// execution and asynchronous admission errors are reported by a valid Ticket.
	// Reads and writes absorb EINTR but may complete with a short count.
	//
	// Operations in a Link or HardLink chain execute in order. Link cancels the
	// remaining chain after a failure; HardLink continues it. URingScheduler
	// rejects a chain longer than its configured ring depth. Durable applies only
	// to standalone writes; put an explicit FdatasyncOp in a linked chain.
	//
	// Separate submissions are unordered except for file lifecycle:
	//   - A submission containing VOpenatOp holds subsequently accepted operations
	//     on the same virtual slot until its entire linked chain completes. This is
	//     an ordering barrier, not error propagation; use Link when followers must
	//     be canceled if open fails.
	//   - CloseOp and VCloseOp wait for previously accepted operations on their
	//     file to complete. Earlier operations in the same linked chain are ordered
	//     before close by the chain and are not part of that drain. A standalone
	//     Durable write remains active through its synthesized fdatasync, so a
	//     following close waits for the flush too.
	//   - Operations later in the same linked chain are ordered after its close.
	//     Separate work submitted for a file after its close has been accepted but
	//     before that close completes is unsupported. Wait for the close-containing
	//     Ticket before submitting more work for that virtual slot.
	//
	// These rules use the coordinator's acceptance order. Submit calls racing from
	// different goroutines do not establish an application-visible order.
	Submit(op Op) (Ticket, error)

	// Close shuts down the scheduler. Callers must stop submitting before
	// Close; racing Close with Submit is not supported.
	io.Closer
}

// pooler is implemented by schedulers that support pre-registered DMA buffers.
type pooler interface {
	usePool(*mempool.SlabPool) error
}

// RegisterDMASlab registers pool as a fixed buffer with scheduler s via
// io_uring_register_buffers. If s does not support registered buffers, this is
// a no-op.
func RegisterDMASlab(s Scheduler, pool *mempool.SlabPool) error {
	if p, ok := s.(pooler); ok {
		return p.usePool(pool)
	}
	return nil
}

// URingConfig configures the io_uring scheduler.
type URingConfig struct {
	// RingDepth is the number of SQ/CQ entries. Zero uses the default.
	RingDepth uint32

	// SQPOLL enables IORING_SETUP_SQPOLL.
	SQPOLL bool

	// VFiles registers a sparse virtual-file table of this size. Virtual-file
	// ops require a non-zero table.
	VFiles uint32

	// DisableCoalescing turns off write coalescing (contiguous same-file plain
	// writes in a batch merged into one writev). Coalescing is on by default.
	DisableCoalescing bool
}
