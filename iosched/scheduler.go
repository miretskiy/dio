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
// The [Scheduler] interface exposes a blocking Submit API for group-commit
// scenarios: prepare N ops, call Submit, then read each op's [Result]. The
// [URingScheduler] batches concurrent Submit calls into shared io_uring_enter
// cycles. Only [URingScheduler] implements Scheduler; it is Linux-only.
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

// Scheduler is the synchronous I/O submission interface.
//
// Callers build [Op] values and call Submit, which blocks until every op
// has completed (or the scheduler shuts down). After Submit returns,
// per-op outcomes are readable via [Op.Result].
//
// Parallelism comes from goroutines: many goroutines may call Submit
// concurrently. The scheduler batches concurrent submissions into shared
// io_uring_enter calls via a drain-leader pattern — one caller does the
// kernel work for itself and any other callers whose ops happen to be
// queued at the same time.
//
// Ops with [Op.Linked] or [Op.HardLinked] form SQE chains executed in order
// by the kernel. Independent ops may complete in any order.
//
// Implementations must be safe for concurrent use by multiple goroutines.
type Scheduler interface {
	// Submit executes ops and blocks until every op has completed. The
	// caller's ops slice must remain valid for the duration of the call;
	// since Submit blocks until completion, this is the caller's natural
	// stack frame lifetime.
	//
	// After Submit returns nil, each op's result is readable via
	// op.Result(). If Submit returns a non-nil error (scheduler closed,
	// backpressure, etc.), the per-op results are not guaranteed.
	Submit(ops []Op) error

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
	// returns [ErrTooBusy] when accepting a Submit call would push the
	// in-flight count above this cap. Zero (the default) means unlimited.
	//
	// Independent of RingDepth: a single Submit call can carry many ops; the
	// budget is per-op since each op holds resources for its lifetime in
	// flight.
	MaxInflightOps int64

	// SQPOLL enables IORING_SETUP_SQPOLL. The kernel spawns a polling thread
	// that submits SQEs without a syscall on the submission path, at the cost
	// of one CPU core. The kernel thread sleeps after ~1 s of idle.
	SQPOLL bool
}
