// Package iosched provides pluggable I/O scheduling for positioned reads and writes.
//
// # Architecture
//
// The primary user-facing type is BlockingIO, which provides synchronous
// ReadAt/WriteAt/Fsync methods on every platform. When io_uring is available
// on Linux, it wraps a URingScheduler and routes each call through the async
// Submit path. When io_uring is unavailable, it invokes POSIX syscalls directly.
package iosched

import (
	"io"

	"github.com/miretskiy/dio/mempool"
)

// Scheduler is the async I/O submission interface.
type Scheduler interface {
	// Submit enqueues op for asynchronous execution and returns a Ticket.
	// The caller must call Ticket.Wait before reading results or releasing
	// the Ticket. Linked operations are attached with Op.Link. Submit does
	// not implement backpressure; callers that need admission control should
	// wrap the scheduler with their own semaphore, queue, or retry policy.
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
