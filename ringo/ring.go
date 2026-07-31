//go:build linux

package ringo

import (
	"errors"
	"fmt"
	"iter"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/miretskiy/dio/internal/buildutil"
	"github.com/miretskiy/dio/internal/intrusive"
)

var (
	// ErrFull reports that the ring cannot own another operation until
	// completions are reaped.
	ErrFull = errors.New("ringo: ring is full")
	// ErrClosed reports use of a closed Ring.
	ErrClosed = errors.New("ringo: ring is closed")
	// ErrPending reports that Close released a Ring that still owned operations,
	// so the Ring and their operands are retained for the life of the process.
	// The Ring is closed either way.
	ErrPending = errors.New("ringo: ring closed with pending operations")
	// ErrWrongRing reports a ring-scoped handle or resource used with another
	// Ring.
	ErrWrongRing = errors.New("ringo: resource belongs to another ring")
)

// abandoned roots every Ring that was closed while it still owned operations,
// and never releases one. The kernel dereferences a pending operation's
// operands while its request runs, and closing the descriptor only starts an
// asynchronous teardown whose completion nothing can observe, so there is no
// point at which releasing them becomes provably safe. A Ring cannot keep them
// alive on its own: once Close returns, an unreferenced Ring and everything
// under it are collectable immediately. Only a root outside the caller's
// reachability works, so the leak is made explicit here instead.
var abandoned sync.Map // *Ring -> struct{}

// Handle is an opaque, comparable, ring-local operation identity.
type Handle struct {
	ring uint64
	slot intrusive.Handle
}

// Index returns a dense index for the operation, in [0, Ring.Capacity()). It
// lets a caller keep per-operation state in a slice instead of a map, which is
// the difference between an array index and a hash on every completion.
//
// The index names the Ring slot the operation occupies, not the operation, so a
// later operation reuses it once this one's final completion is reaped. A side
// table must therefore be written on every push, and should store the Handle
// alongside its entry so its owner can tell the slot's current occupant from a
// stale one. Index is meaningless for the zero Handle.
func (handle Handle) Index() int {
	return int(uint32(handle.slot)) - 1
}

// CompletionFlags are the flags supplied with a completion queue entry.
type CompletionFlags uint32

// More reports whether the kernel kept a multishot operation active after
// this completion.
func (flags CompletionFlags) More() bool {
	return rawCQEFlags(flags)&rawCQEMore != 0
}

// Completion is an independent Go value decoded from a kernel completion
// queue entry. Result is nonnegative when Err is nil. Kernel errors are
// returned in Err as syscall.Errno values.
type Completion struct {
	// Handle identifies the operation that produced this completion.
	Handle Handle
	// Result is the nonnegative CQE result. It is zero when Err is non-nil.
	Result int
	// Flags contains the kernel CQE flags.
	Flags CompletionFlags
	// Err reports that the operation failed. It is the syscall.Errno encoded by
	// a negative CQE result.
	Err error
}

type pendingSlot struct {
	op     Op
	handle Handle
}

// Ring owns an io_uring instance and every operation pushed into it until the
// operation's final completion is reaped. A Ring must be constructed by New;
// its zero value is not usable.
type Ring struct {
	backend *rawRing
	id      uint64
	pending intrusive.FixedList[pendingSlot]

	files   *FixedFiles
	buffers *FixedBuffers

	eventFD *os.File

	closed bool
}

var nextRingID atomic.Uint64

// New creates a Ring. The running kernel must support IORING_FEAT_NODROP,
// IORING_FEAT_LINKED_FILE, and IORING_SETUP_SUBMIT_ALL. NODROP is required
// because losing a CQE would make safe ownership release impossible.
// LINKED_FILE and SUBMIT_ALL give every Ring the same safe linked-submission
// semantics.
// liburing: io_uring_queue_init_params - https://man7.org/linux/man-pages/man3/io_uring_queue_init_params.3.html
func New(options ...Option) (*Ring, error) {
	config := defaultConfig()
	for _, option := range options {
		if option == nil {
			return nil, errors.New("ringo: nil option")
		}
		if err := option.apply(&config); err != nil {
			return nil, err
		}
	}

	params := rawParams{
		Cq_entries:     config.cqEntries,
		Flags:          uint32(config.flags),
		Sq_thread_cpu:  config.sqThreadCPU,
		Sq_thread_idle: config.sqThreadIdle,
	}
	raw := newRaw()
	if err := raw.queueInit(config.depth, params); err != nil {
		return nil, fmt.Errorf("ringo: io_uring_setup: %w", err)
	}
	if !raw.hasFeature(rawFeatNoDrop) {
		_ = raw.queueExit()
		return nil, fmt.Errorf(
			"ringo: kernel does not support IORING_FEAT_NODROP: %w",
			syscall.EOPNOTSUPP,
		)
	}
	if !raw.hasFeature(rawFeatLinkedFile) {
		_ = raw.queueExit()
		return nil, fmt.Errorf(
			"ringo: kernel does not support IORING_FEAT_LINKED_FILE: %w",
			syscall.EOPNOTSUPP,
		)
	}

	ring := &Ring{
		backend: raw,
		id:      nextRingID.Add(1),
		pending: intrusive.MakeFixedList[pendingSlot](int(raw.sqCapacity())),
	}
	if config.fixedFiles != 0 {
		if _, err := raw.registerFilesSparse(config.fixedFiles); err != nil {
			_ = raw.queueExit()
			return nil, fmt.Errorf("ringo: register sparse files: %w", err)
		}
		ring.files = &FixedFiles{
			ring:   ring,
			count:  config.fixedFiles,
			owners: make([]*os.File, config.fixedFiles),
		}
	}
	return ring, nil
}

// Available reports whether a lifetime-safe Ring can be created on this
// kernel.
func Available() bool {
	ring, err := New(WithDepth(1))
	if err != nil {
		return false
	}
	_ = ring.Close()
	return true
}

// Capacity returns the maximum number of operations the Ring can own at once.
// It reports the actual SQ depth selected by the kernel, which may differ from
// the value requested with WithDepth because Linux may round or clamp it.
func (ring *Ring) Capacity() int {
	return ring.pending.Cap()
}

// FixedFiles returns the Ring's registered-file table, or nil when no table is
// registered.
func (ring *Ring) FixedFiles() *FixedFiles {
	return ring.files
}

// Push queues one SQE without entering the kernel. On success it takes
// ownership of op permanently; the caller must discard every interface copy and
// use only the returned Handle. If Push returns an error, the caller retains
// ownership of op. Reusing a pushed Op is forbidden and is not diagnosed.
// liburing: io_uring_get_sqe - https://man7.org/linux/man-pages/man3/io_uring_get_sqe.3.html
func (ring *Ring) Push(op Op) (Handle, error) {
	if op == nil {
		return Handle{}, errors.New("ringo: operation 0: nil operation")
	}
	if err := ring.ready(); err != nil {
		return Handle{}, err
	}
	if !ring.hasCapacity(1) {
		return Handle{}, ErrFull
	}
	if err := op.validate(ring); err != nil {
		return Handle{}, fmt.Errorf("ringo: operation 0: %w", err)
	}
	return ring.pushValidated(op, 0), nil
}

func (ring *Ring) pushValidated(op Op, link rawSQEFlags) Handle {
	slot := ring.pending.PushBack()
	handle := Handle{ring: ring.id, slot: slot}
	pending := ring.pending.Value(slot)
	pending.op = op
	pending.handle = handle

	sqe := ring.backend.getSQE()
	ring.prepareSQE(sqe, pending)
	sqe.Flags |= uint8(link)
	return handle
}

func (ring *Ring) hasCapacity(count int) bool {
	return count <= ring.pending.Cap()-ring.pending.Len() &&
		uint32(count) <= ring.backend.sqSpaceLeft()
}

// PushLinked atomically queues one linked sequence. Each Link describes the
// edge from the preceding operation to Link.Next. At least one link is
// required. Every Ring uses IORING_SETUP_SUBMIT_ALL because a linked chain
// cannot continue across a short submission boundary.
//
// On success PushLinked takes ownership of every Op permanently, but it does
// not retain the variadic Link slice. The caller must discard every Op copy and
// use only the returned Handles. A returned error queues and takes nothing:
// every element is validated before any of them is transferred.
// liburing: io_uring_get_sqe - https://man7.org/linux/man-pages/man3/io_uring_get_sqe.3.html
func (ring *Ring) PushLinked(
	first Op,
	link Link,
	other ...Link,
) ([]Handle, error) {
	if first == nil {
		return nil, errors.New("ringo: operation 0: nil operation")
	}
	if err := ring.ready(); err != nil {
		return nil, err
	}
	count := len(other) + 2
	if !ring.hasCapacity(count) {
		return nil, ErrFull
	}
	if err := first.validate(ring); err != nil {
		return nil, fmt.Errorf("ringo: operation 0: %w", err)
	}
	current := link
	for index := range len(other) + 1 {
		link := current
		if link.Type != LinkSoft && link.Type != LinkHard {
			return nil, fmt.Errorf("ringo: link %d: invalid link type", index)
		}
		if link.Next == nil {
			return nil, fmt.Errorf("ringo: operation %d: nil operation", index+1)
		}
		if err := link.Next.validate(ring); err != nil {
			return nil, fmt.Errorf("ringo: operation %d: %w", index+1, err)
		}
		if index < len(other) {
			current = other[index]
		}
	}

	handles := make([]Handle, count)
	handles[0] = ring.pushValidated(first, rawSQEFlags(link.Type))
	current = link
	for index := range len(other) + 1 {
		var outgoing rawSQEFlags
		if index < len(other) {
			outgoing = rawSQEFlags(other[index].Type)
		}
		handles[index+1] = ring.pushValidated(current.Next, outgoing)
		if index < len(other) {
			current = other[index]
		}
	}
	return handles, nil
}

func (ring *Ring) prepareSQE(sqe *rawSQE, pending *pendingSlot) {
	pending.op.prepare(sqe)
	sqe.User_data = uint64(pending.handle.slot)
}

// Submit enters the kernel without waiting for completions. It reports the same
// retryable conditions as SubmitAndWait.
// liburing: io_uring_submit - https://man7.org/linux/man-pages/man3/io_uring_submit.3.html
func (ring *Ring) Submit() (submitted int, err error) {
	return ring.submitAndWait(0)
}

// SubmitAndWait submits queued work and asks the kernel to wait until at least
// minComplete completions are available. It does not reap them.
//
// minComplete must not exceed the number of operations currently in flight,
// meaning pushed and not yet finally completed. Asking the kernel to wait for
// more completions than can ever arrive blocks inside io_uring_enter until the
// wait is otherwise satisfied: by those operations completing, by a queued
// Timeout, by CancelAll, or by a signal. Passing 0 never waits.
//
// A submit call may report both progress and an error; that error never returns
// ownership of a pushed operation to the caller. EAGAIN and EBUSY are the
// kernel's temporary resource conditions and are passed through: reap the
// available completions and submit again. Ringo retries EINTR internally,
// because an interrupted io_uring_enter consumes no SQE and loses no
// completion, so it carries no information for the caller; interruption of an
// operation is reported in that operation's completion instead.
// liburing: io_uring_submit_and_wait - https://man7.org/linux/man-pages/man3/io_uring_submit_and_wait.3.html
func (ring *Ring) SubmitAndWait(minComplete int) (submitted int, err error) {
	if minComplete < 0 || uint64(minComplete) > math.MaxUint32 {
		return 0, errors.New("ringo: invalid minimum completion count")
	}
	return ring.submitAndWait(uint32(minComplete))
}

func (ring *Ring) submitAndWait(minComplete uint32) (int, error) {
	if err := ring.ready(); err != nil {
		return 0, err
	}
	submitted, err := ring.backend.submitAndWait(minComplete)
	return int(submitted), err
}

// Reap returns a nonblocking iterator over a bounded snapshot of currently
// available completions. The Ring is exclusively borrowed while the iterator
// is active. The yielded completion is advanced and its final pending state is
// released after each iterator step, including break and panic. Reap yields
// nothing for a closed Ring.
//
// Every yielded Completion describes one kernel completion queue entry, so
// Handle always identifies an operation the Ring owns and Err is always that
// operation's own error. A CQE whose identity does not resolve is an internal
// invariant violation: test builds fail loudly, while production discards it
// rather than inventing a Completion for an operation the Ring does not own.
//
// Reap does not enter the kernel, so it cannot move entries off the kernel's
// overflow list. Only a multishot operation can produce more completions than
// the Ring has pending slots, because Linux never sizes the completion queue
// below the submission queue. A caller using multishot operations must
// therefore keep calling Submit or SubmitAndWait, which enter the kernel on
// IORING_SQ_CQ_OVERFLOW, rather than reap alone.
// liburing: io_uring_peek_cqe - https://man7.org/linux/man-pages/man3/io_uring_peek_cqe.3.html
func (ring *Ring) Reap() iter.Seq[Completion] {
	return func(yield func(Completion) bool) {
		if ring.closed {
			return
		}
		available := ring.backend.cqReady()
		for range available {
			cqe := ring.backend.peekCQE()
			// Only Reap advances the completion head and the kernel only grows
			// the tail, so the snapshot cannot shrink underneath this loop.
			if err := buildutil.Assert(cqe != nil); err != nil {
				return
			}
			slotHandle := intrusive.Handle(cqe.Data)
			pending, resolved := ring.pending.TryValue(slotHandle)
			// Every SQE carries a generation-tagged identity, so one Ringo
			// cannot resolve means Ringo released a slot before the kernel's
			// last CQE for it: its own finality bookkeeping is wrong. Fail
			// loudly in a test build. In production drop the entry rather than
			// take a hard dependency on the kernel never producing one; it names
			// no operation the caller holds, and reporting its raw user_data as
			// a Handle would resurrect the stale identity the generation tag
			// exists to reject.
			if err := buildutil.Assert(resolved); err != nil {
				ring.backend.advanceCQ(1)
				continue
			}

			completion := Completion{
				Handle: pending.handle,
				Flags:  CompletionFlags(cqe.Flags),
			}
			if cqe.Res < 0 {
				completion.Err = syscall.Errno(-cqe.Res)
			} else {
				completion.Result = int(cqe.Res)
			}
			final := !completion.Flags.More()
			if !ring.yieldCompletion(yield, completion, slotHandle, final) {
				return
			}
		}
	}
}

func (ring *Ring) yieldCompletion(
	yield func(Completion) bool,
	completion Completion,
	slot intrusive.Handle,
	final bool,
) (more bool) {
	defer func() {
		ring.backend.advanceCQ(1)
		if final {
			ring.release(slot)
		}
	}()
	return yield(completion)
}

func (ring *Ring) release(handle intrusive.Handle) {
	pending, ok := ring.pending.TryValue(handle)
	if !ok {
		return
	}
	operation := pending.op
	pending.op = nil
	pending.handle = Handle{}
	ring.pending.Remove(handle)
	operation.release()
}

func (ring *Ring) ready() error {
	if ring.closed {
		return ErrClosed
	}
	return nil
}

// CancelAll synchronously requests cancellation of every in-flight operation,
// waiting up to timeout for the kernel to finish. Cancellation is best effort:
// a request the kernel cannot interrupt keeps running, and the call reports
// ETIME rather than success. Operations that are cancelled report ECANCELED in
// their own completions, which the caller must still reap.
//
// CancelAll is the one Ring method that may overlap another call on the same
// Ring except Close, which is what lets one goroutine break another out of a
// blocking SubmitAndWait during shutdown. Close must not begin until CancelAll
// and every other Ring call have returned.
// liburing: io_uring_register_sync_cancel - https://man7.org/linux/man-pages/man3/io_uring_register_sync_cancel.3.html
func (ring *Ring) CancelAll(timeout time.Duration) error {
	if ring.closed {
		return ErrClosed
	}
	if timeout < 0 {
		return errors.New("ringo: cancellation timeout must be nonnegative")
	}
	// struct io_uring_sync_cancel_reg carries a __kernel_timespec, which is
	// 64-bit on every architecture, unlike syscall.Timespec.
	spec := syscall.NsecToTimespec(timeout.Nanoseconds())
	_, err := ring.backend.registerSyncCancel(&rawSyncCancelReg{
		Flags: uint32(rawAsyncCancelAny | rawAsyncCancelAll),
		Timeout: rawTimespec{
			Sec:  int64(spec.Sec),
			Nsec: int64(spec.Nsec),
		},
	})
	return err
}

// Close releases the Ring. It always closes the io_uring descriptor, which asks
// the kernel to release the context, and never waits. The Ring is unusable
// afterwards, and Close is idempotent. Close must not overlap another Ring
// call, including CancelAll.
//
// Reaping every pushed operation to its final completion first is the orderly
// close: the Ring then also unmaps its ring memory and drops every reference it
// held, and Close returns nil.
//
// Closing with operations still pending is possible but not free. Their
// buffers, iovecs, and output structures stay visible to the kernel while the
// requests run, and closing the descriptor does not stop that: io_uring_setup(2)
// documents that the context's resources are freed asynchronously, and nothing
// reports when that has finished. So Close cannot release those operands, and
// it cannot ask the caller to hold them either, because an unreferenced Ring
// becomes collectable the moment Close returns. Instead the Ring is retained
// permanently, along with its ring mappings, and Close reports ErrPending with
// the number of operations involved. That leak is bounded by the Ring's
// capacity and lasts for the life of the process.
//
// Close drops the Ring's own references to its registered tables but does not
// modify the FixedFiles and FixedBuffers values the caller holds. Those stay
// immutable for their whole lifetime, so their lookups remain safe to call from
// any goroutine, and they become collectable once the caller drops them too.
// liburing: io_uring_queue_exit - https://man7.org/linux/man-pages/man3/io_uring_queue_exit.3.html
func (ring *Ring) Close() error {
	if ring.closed {
		return nil
	}
	ring.closed = true
	err := ring.backend.closeDescriptor()
	ring.buffers = nil
	ring.files = nil
	ring.eventFD = nil

	if pending := ring.pending.Len(); pending != 0 {
		// Retain the Ring, its pending operands, and its still-mapped ring
		// memory. Unmapping is very likely safe, since the kernel owns those
		// pages and posts through its own reference, but nothing documents it
		// for a ring with requests outstanding, and this branch is already
		// leaking.
		abandoned.Store(ring, struct{}{})
		return errors.Join(err, fmt.Errorf(
			"%w: %d operations retained", ErrPending, pending,
		))
	}

	ring.backend.releaseMappings()
	ring.backend = nil
	return err
}
