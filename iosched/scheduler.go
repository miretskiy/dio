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

// Scheduler is the async I/O submission interface.
type Scheduler interface {
	// Submit hands op to the scheduler for execution, possibly asynchronously,
	// and returns a Ticket to await it. The scheduler is a lean pipe: it moves
	// bytes between the caller and the kernel and reports completions. It does not
	// model files, order your operations, or manage their lifecycle — those are
	// the caller's.
	//
	// Guaranteed:
	//   - Every op eventually completes exactly once. The Ticket becomes ready
	//     after Ticket.Wait; then Ticket.Result, Ticket.N, and Ticket.Error hold
	//     the outcome. A linked chain completes as one unit.
	//   - EINTR is absorbed internally, so Result.Err is never EINTR (see the
	//     package doc); callers handle only terminal errors and short transfers.
	//   - A linked chain (Op.Link) executes in order — each op starts only after
	//     the previous one completed — and a failure short-circuits the rest with
	//     ECANCELED.
	//   - use-before-close: a close does not run until its file's in-flight data
	//     ops complete. The scheduler holds the close (both virtual slots and
	//     regular descriptors) until the file quiesces, so a close submitted right
	//     behind writes will not clear the file out from under them. You need not
	//     await the writes before closing.
	//
	// NOT guaranteed:
	//   - Any ordering between separate Submits. Independent ops — even to the
	//     same file or offset — may execute and complete in any order and
	//     concurrently. Submit order is not execution order.
	//   - Open-before-use, and reopen-after-close, are not yet sequenced by the
	//     scheduler: an open does not hold back later writes to its slot, and a
	//     reopen is not ordered after a prior close. Sequence those with Wait or
	//     Op.Link (see below).
	//
	// If you need ordering the scheduler does not give, encode it:
	//   - Wait: submit the prerequisite, Ticket.Wait for it, then submit the
	//     dependents. Simplest, and right when the prerequisite is rare (open a
	//     file once, then stream many writes to it).
	//   - Link: chain dependents after the prerequisite with Op.Link so the
	//     kernel orders them with no round-trip. Right for a linear dependency
	//     (write→fdatasync, or close→reopen of a recycled slot). A link is
	//     strictly linear — no fan-out or fan-in — so N independent writes cannot
	//     be "ordered before" one op without serializing them; use Wait (or a
	//     caller-side join) for that.
	//
	// On virtual files specifically: the kernel resolves an async op's fixed file
	// when it issues the op, not at submission, so a write must not be submitted
	// before its open has completed, and a reopen must follow the prior close.
	// (Closing is handled for you by use-before-close above.) Order the open and
	// reopen with Wait or Op.Link.
	//
	// Submit implements no backpressure: it is a non-blocking handoff onto a
	// lock-free staging queue. Callers that need admission control wrap the
	// scheduler with their own semaphore, token bucket, or queue.
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

	// VFiles registers a sparse virtual-file table of this size. Virtual-file
	// ops require a non-zero table.
	VFiles uint32

	// DisableCoalescing turns off write coalescing (contiguous same-file plain
	// writes in a batch merged into one writev). Coalescing is on by default.
	DisableCoalescing bool
}
