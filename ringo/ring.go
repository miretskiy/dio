//go:build linux

package ringo

import (
	"errors"
	"fmt"
	"iter"
	"math"
	"os"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/miretskiy/dio/internal/intrusive"
)

var (
	// ErrFull reports that the ring cannot own another operation until
	// completions are reaped.
	ErrFull = errors.New("ringo: ring is full")
	// ErrClosed reports use of a closed Ring.
	ErrClosed = errors.New("ringo: ring is closed")
	// ErrPending reports that Close cannot release a Ring that still owns
	// operations awaiting final completion reaping.
	ErrPending = errors.New("ringo: ring has pending operations")
	// ErrWrongRing reports a ring-scoped handle or resource used with another
	// Ring.
	ErrWrongRing = errors.New("ringo: resource belongs to another ring")
	// ErrCorruptCompletion reports a CQE whose private lifecycle identity is
	// unknown or stale.
	ErrCorruptCompletion = errors.New("ringo: corrupt completion identity")
)

type ringBackend interface {
	sqCapacity() uint32
	sqSpaceLeft() uint32
	getSQE() *rawSQE
	submitAndWait(uint32) (uint, error)
	cqReady() uint32
	peekCQE() *rawCQE
	advanceCQ(uint32)
	registerBuffers([]syscall.Iovec) (uint, error)
	registerFilesSparse(uint32) (uint, error)
	registerSyncCancel(*rawSyncCancelReg) (uint, error)
	register(rawRegisterOpcode, unsafe.Pointer, uint32) (uint, syscall.Errno)
	queueExit() error
}

// Handle is an opaque, comparable, ring-local operation identity.
type Handle struct {
	ring uint64
	slot intrusive.Handle
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
	// Err is the syscall.Errno encoded by a negative CQE result.
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
	backend ringBackend
	id      uint64
	pending intrusive.FixedList[pendingSlot]

	files   *FixedFiles
	buffers *FixedBuffers

	eventFD *os.File

	closed bool
	fatal  error
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

// Push queues one SQE without entering the kernel. On success it permanently
// consumes op; the caller must discard every interface copy and use only the
// returned Handle. If Push returns an error, the caller retains ownership of
// op. Detected use of an already-consumed Op panics.
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
	op.markConsumed()
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
// On success PushLinked permanently consumes every Op, but it does not retain
// the variadic Link slice. The caller must discard every Op copy and use only
// the returned Handles. A returned error queues and consumes nothing.
// Duplicate or previously consumed operations panic when detected; a misuse
// panic provides no ownership or rollback guarantee.
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

// Submit enters the kernel without waiting for completions.
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
// released after each iterator step, including break and panic.
// liburing: io_uring_peek_cqe - https://man7.org/linux/man-pages/man3/io_uring_peek_cqe.3.html
func (ring *Ring) Reap() iter.Seq[Completion] {
	return func(yield func(Completion) bool) {
		if ring.closed {
			yield(Completion{Err: ErrClosed})
			return
		}
		available := ring.backend.cqReady()
		for range available {
			cqe := ring.backend.peekCQE()
			if cqe == nil {
				ring.setFatal(errors.New("ringo: completion queue shrank while reaping"))
				return
			}
			slotHandle := intrusive.Handle(cqe.Data)
			pending, ok := ring.pending.TryValue(slotHandle)
			if !ok {
				err := fmt.Errorf("%w: user_data=%#x", ErrCorruptCompletion, cqe.Data)
				ring.setFatal(err)
				if !ring.yieldCompletion(yield, Completion{
					Flags: CompletionFlags(cqe.Flags),
					Err:   err,
				}, 0, false) {
					return
				}
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

func (ring *Ring) setFatal(err error) {
	if ring.fatal == nil {
		ring.fatal = err
	}
}

func (ring *Ring) ready() error {
	if ring.closed {
		return ErrClosed
	}
	if ring.fatal != nil {
		return ring.fatal
	}
	return nil
}

// CancelAll synchronously requests cancellation of every in-flight operation.
// It may be called concurrently with SubmitAndWait during shutdown.
// liburing: io_uring_register_sync_cancel - https://man7.org/linux/man-pages/man3/io_uring_register_sync_cancel.3.html
func (ring *Ring) CancelAll(timeout time.Duration) error {
	if ring.closed {
		return ErrClosed
	}
	if timeout < 0 {
		return errors.New("ringo: cancellation timeout must be nonnegative")
	}
	spec := syscall.NsecToTimespec(timeout.Nanoseconds())
	_, err := ring.backend.registerSyncCancel(&rawSyncCancelReg{
		Flags: uint32(rawAsyncCancelAny | rawAsyncCancelAll),
		Timeout: rawTimespec{
			Sec:  spec.Sec,
			Nsec: spec.Nsec,
		},
	})
	return err
}

// Close releases an idle Ring. Every pushed operation must have reached a
// final completion and been reaped first. If operations remain, Close returns
// ErrPending without changing the Ring. Close is idempotent after success.
// liburing: io_uring_queue_exit - https://man7.org/linux/man-pages/man3/io_uring_queue_exit.3.html
func (ring *Ring) Close() error {
	if ring.closed {
		return nil
	}
	if ring.pending.Len() != 0 {
		return ErrPending
	}
	ring.closed = true
	err := ring.backend.queueExit()
	if ring.buffers != nil {
		clear(ring.buffers.buffers)
		ring.buffers.buffers = nil
		ring.buffers.ring = nil
		ring.buffers = nil
	}
	if ring.files != nil {
		ring.files.ring = nil
		clear(ring.files.owners)
		ring.files.owners = nil
		ring.files.count = 0
	}
	ring.files = nil
	ring.eventFD = nil
	ring.backend = nil
	return err
}
