// Package iosched provides pluggable I/O scheduling for positioned reads and writes.
//
// # Architecture
//
// The primary user-facing type is [BlockingIO], which provides synchronous
// ReadAt/WriteAt/Fsync methods on every platform. When io_uring is available
// (Linux), it wraps a [URingScheduler] and routes each call through the async
// Submit/Wait path. When io_uring is unavailable, it invokes POSIX syscalls
// directly — no indirection, no group management.
//
// The [Scheduler] interface exposes the async Submit/Wait API for group-commit
// scenarios: submit N writes from N goroutines, then Wait on all tickets. The
// [URingScheduler] batches all enqueued SQEs into a single io_uring_enter.
// Only [URingScheduler] implements Scheduler; it is Linux-only.
//
// Use [NewDefaultIO] to get the best available implementation for the host.
package iosched

import (
	"errors"
	"io"

	"github.com/miretskiy/dio/mempool"
)

// Scheduler is the async I/O submission interface.
//
// Callers build [Op] values, call Submit, and call Wait to collect results.
// Ops with [Op.Linked] or [Op.HardLinked] form SQE chains executed in order
// by the kernel. Independent ops may complete in any order.
//
// Each [Ticket] must be passed to Wait exactly once; a second call returns
// [ErrTicketConsumed].
//
// Buffers referenced by Ops must remain valid until the corresponding Wait
// returns.
//
// Implementations must be safe for concurrent use by multiple goroutines.
type Scheduler interface {
	// Submit enqueues ops and returns a completion Ticket. Does not block on
	// I/O completion.
	Submit(ops []Op) (Ticket, error)

	// Wait blocks until all ops for the ticket complete, then writes results
	// into the provided slice (len >= number of ops). Returns count written.
	Wait(ticket Ticket, results []Result) (int, error)

	io.Closer
}

// ErrTicketConsumed is returned by Wait when called more than once with the
// same Ticket.
var ErrTicketConsumed = errors.New("iosched: ticket already consumed")

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

var errSchedulerClosed = errors.New("iosched: scheduler closed")

// URingConfig configures the io_uring scheduler.
type URingConfig struct {
	// RingDepth is the number of SQ/CQ entries. Must be a power of two.
	// Zero uses the default (256).
	RingDepth uint32

	// SQPOLL enables IORING_SETUP_SQPOLL. The kernel spawns a polling thread
	// that submits SQEs without a syscall on the submission path, at the cost
	// of one CPU core. The kernel thread sleeps after ~1 s of idle.
	SQPOLL bool
}

// resultStore holds result slots for one Submit call.
// Inline covers ≤4 ops (the common case) with no heap allocation.
type resultStore struct {
	nOps   uint16
	inline [4]Result
	extra  []Result // allocated lazily for >4 ops
}

func (r *resultStore) init(nOps int) {
	r.nOps = uint16(nOps)
	if nOps > len(r.inline) {
		need := nOps - len(r.inline)
		if cap(r.extra) >= need {
			r.extra = r.extra[:need]
		} else {
			r.extra = make([]Result, need)
		}
	}
}

func (r *resultStore) at(i int) *Result {
	if i < len(r.inline) {
		return &r.inline[i]
	}
	return &r.extra[i-len(r.inline)]
}

func (r *resultStore) copyTo(dst []Result) int {
	n := int(r.nOps)
	for i := 0; i < n && i < len(dst); i++ {
		dst[i] = *r.at(i)
	}
	return min(n, len(dst))
}
