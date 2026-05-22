// Package iosched provides pluggable I/O scheduling for positioned reads and writes.
//
// # Architecture
//
// The primary user-facing type is [BlockingIO], which provides synchronous
// ReadAt/WriteAt/Fsync methods on every platform. When io_uring is available
// (Linux), it wraps a [URingScheduler] and routes each call through the async
// Submit path. When io_uring is unavailable, it invokes POSIX syscalls
// directly — no indirection, no scheduler.
//
// The [Scheduler] interface exposes the async Submit API for group-commit
// scenarios: prepare a [Submission] of N ops, call Submit, then wait on
// `<-sub.Done()`. The [URingScheduler] batches all enqueued SQEs into a
// single io_uring_enter. Only [URingScheduler] implements Scheduler; it is
// Linux-only.
//
// Use [NewDefaultIO] to get the best available implementation for the host.
package iosched

import (
	"errors"
	"io"

	"github.com/miretskiy/dio/mempool"
)

// ErrTooBusy is returned by [Scheduler.Submit] when accepting the submission
// would exceed [URingConfig.MaxInflightOps]. Callers decide their own
// backpressure policy: retry, backoff, drop the work, or surface the error.
// The scheduler never blocks Submit waiting for budget.
var ErrTooBusy = errors.New("iosched: too busy")

// Scheduler is the async I/O submission interface.
//
// Callers build [Op] values, acquire a [Ticket] via [NewTicket], populate
// Ticket.Ops, and call Submit. After Submit, the caller waits on
// [Ticket.Wait] and then reads per-op results via [Op.Result]. Once
// finished reading, the caller releases the Ticket back to the pool via
// [Ticket.Release] (typically `defer t.Release()` immediately after
// NewTicket).
//
// Ops with [Op.Linked] or [Op.HardLinked] form SQE chains executed in order
// by the kernel. Independent ops may complete in any order.
//
// Implementations must be safe for concurrent use by multiple goroutines.
type Scheduler interface {
	// Submit enqueues t.Ops for asynchronous execution. Does not block on
	// I/O completion. The scheduler retains *t internally for the duration
	// of the request; the caller MUST call t.Wait before reading results
	// or releasing the Ticket.
	Submit(t *Ticket) error

	io.Closer
}

// pooler is implemented by schedulers that support pre-registered DMA buffers.
type pooler interface {
	usePool(*mempool.SlabPool) error
}

// RegisterDMASlab registers pool as a fixed buffer with scheduler s via
// io_uring_register_buffers. The kernel pins the buffer for the ring lifetime,
// eliminating per-I/O DMA setup overhead for [ReadFixedOp] / [WriteFixedOp].
//
// Must be called after [NewURingScheduler] and before any fixed-buffer ops are
// submitted. Calling it more than once replaces the previous registration.
//
// pool must outlive s — call pool.Close() only after s.Close() returns.
//
// If s does not support registered buffers, RegisterDMASlab is a silent no-op.
// Registered buffers are an optional performance optimisation, not required
// for correctness.
func RegisterDMASlab(s Scheduler, pool *mempool.SlabPool) error {
	if p, ok := s.(pooler); ok {
		return p.usePool(pool)
	}
	return nil // scheduler doesn't support fixed buffers; silently skip
}

// URingConfig configures the io_uring scheduler.
type URingConfig struct {
	// RingDepth is the number of SQ/CQ entries. Must be a power of two.
	// Zero uses the default (256). Caps the number of ops in flight in the
	// kernel at any moment.
	RingDepth uint32

	// MaxInflightOps caps the total number of ops currently in flight in
	// the scheduler (queued + placed in the ring + awaiting reap). Submit
	// returns [ErrTooBusy] when accepting a Submission would push the
	// in-flight count above this cap. Zero (the default) means unlimited.
	//
	// Independent of RingDepth: a single Submission can carry many ops; the
	// budget is per-op since each op holds resources for its lifetime in
	// flight.
	MaxInflightOps int64

	// SQPOLL enables IORING_SETUP_SQPOLL. The kernel spawns a polling thread
	// that submits SQEs without a syscall on the submission path, at the cost
	// of one CPU core. The kernel thread sleeps after ~1 s of idle.
	SQPOLL bool
}
