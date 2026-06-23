// Package iosched provides pluggable I/O scheduling for positioned reads and writes.
//
// # Architecture
//
// The primary abstraction is Scheduler. On Linux, URingScheduler submits ops
// through io_uring. POSIXScheduler implements the same Submit/Ticket lifecycle
// with blocking POSIX file operations.
//
// # Signals (EINTR)
//
// Schedulers absorb EINTR internally: a completed op's Result.Err is never
// syscall.EINTR, so callers handle only terminal errors and short transfers.
//
// Go's runtime preempts goroutines with SIGURG (since 1.14), which interrupts
// in-flight blocking syscalls, so EINTR is routine for any code entering the
// kernel. The standard library retries it for callers (internal/poll); this
// package does the same. How each backend retries is documented at its
// implementation.
package iosched

import (
	"errors"
	"io"

	"github.com/miretskiy/dio/mempool"
)

var errSchedulerClosed = errors.New("iosched: scheduler closed")

// Scheduler is the async I/O submission interface.
type Scheduler interface {
	// Submit enqueues op for asynchronous execution and returns a Ticket.
	// The caller must call Ticket.Wait before reading results or releasing
	// the Ticket. Linked operations are attached with Op.Link.
	//
	// Submit does not implement backpressure; callers that need admission
	// control wrap the scheduler with their own semaphore or queue. It does,
	// however, handle EINTR internally (see the package doc), so callers never
	// retry interrupted syscalls themselves — Result.Err is never EINTR.
	Submit(op Op) (*Ticket, error)

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
}
