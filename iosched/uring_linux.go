package iosched

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/v2/internal/buildutil"
	"github.com/miretskiy/dio/v2/mempool"
	"github.com/miretskiy/dio/v2/ringo"
)

// IOUringAvailable reports whether the running kernel provides the io_uring
// features required by URingScheduler.
var IOUringAvailable = probeIOUring()

func probeIOUring() bool {
	return ringo.Available()
}

// slotKind says what an operation's completion means to the coordinator.
type slotKind uint8

const (
	// slotOperation is one operation of a caller's chain, placed as submitted.
	slotOperation slotKind = iota
	// slotCoalescedWrite is the one write placed for a coalesced run of
	// writes, split back into each member's result.
	slotCoalescedWrite
	// slotSync is an fdatasync shared by the durable writes in batch: writes
	// on one file that completed before it was placed.
	slotSync
	// slotDoorbell is the coordinator's doorbell read; it has no work.
	slotDoorbell
)

// ringSlot holds state that must remain live until an SQE completes. It is
// stored at its handle's Index. handle identifies the operation occupying this
// table entry, telling it from a stale completion for an earlier occupant; a
// zero handle marks the entry free.
type ringSlot struct {
	handle ringo.Handle
	kind   slotKind
	work   *submission     // the work the operation belongs to, or a coalesced run's leader
	op     *Op             // the operation, for slotOperation
	batch  submissionQueue // the durable writes an fdatasync covers, for slotSync
}

var stagingClosed submission

// doorbellSlots is the ring entry the coordinator keeps for its doorbell read.
const doorbellSlots = 1

// doorbellMaxInFlight is how many operations may be in flight while the
// coordinator still asks Submit to ring the doorbell. With more, the next
// completion arrives about as soon as a doorbell wakeup would, so new work
// waits for it rather than costing a submitter an eventfd write and the
// coordinator a wakeup. Measured with bench_coord_linux_test.go on an m7gd
// instance store: any value from 16 to 32 cut CPU per 128 KiB read at
// saturation from 30.6 to 25 µs with no measurable latency cost, while 8
// slowed 16 concurrent 4 KiB reads by 7%.
const doorbellMaxInFlight = 16

// How Submit wakes the coordinator; see URingScheduler.wake.
const (
	wakeNone uint32 = iota
	wakeDoorbell
	wakeChannel
)

// doorbellIncrement is what ringing adds to the doorbell's eventfd counter, an
// unsigned 64-bit integer in native byte order. A read completes once the
// counter is nonzero and resets it to zero; adding 1 keeps the counter far from
// the overflow at which an eventfd write would block.
var doorbellIncrement = func() (value [8]byte) {
	binary.NativeEndian.PutUint64(value[:], 1)
	return value
}()

// maxChainLength bounds a linked chain. A chain is placed whole, so a long one
// at the head of the ready queue holds back everything behind it until that
// many ring entries are free. Eight covers an open, fallocate, writes, a sync
// and a close in one chain. A ring with fewer entries limits chains to what
// fits beside the doorbell.
const maxChainLength = 8

// URingScheduler is an asynchronous Scheduler backed by io_uring.
//
// A single coordinator goroutine owns the SQ and CQ. Submitters publish Ops to
// a lock-free MPSC stack. The coordinator sleeps only inside
// io_uring_enter, waiting for a completion. To let new work end that sleep, it
// keeps one read of an eventfd, the doorbell, in the ring: a submitter that
// finds it asleep with room to place work writes the eventfd, which completes
// the read. Close owns the ring lifetime and may issue synchronous cancellation
// through its fd before tearing it down.
type URingScheduler struct {
	config schedulerConfig

	ring              *ringo.Ring
	fixedFiles        []ringo.FixedFile
	registeredPool    *mempool.SlabPool
	registeredBuffers *ringo.FixedBuffers
	stagingHead       atomic.Pointer[submission]
	done              chan struct{}

	// doorbellFD is a blocking eventfd, not registered with the ring, so the
	// read the coordinator keeps queued on it completes only when Submit or
	// Close rings it. Close sets it to -1 once closed.
	doorbellFD int
	// wake says how the next Submit must wake the coordinator: not at all while
	// it is busy, by ringing the doorbell while it waits in io_uring_enter with
	// I/O in flight and room for new work, or through wakeup while it is parked
	// with nothing in flight. The Submit that finds it set clears it.
	wake atomic.Uint32
	// wakeup carries one wakeup to a parked coordinator. Its buffer keeps a
	// wakeup sent before the coordinator receives, so none is lost.
	wakeup chan struct{}

	stop atomic.Pointer[error]

	// drainErr records that the coordinator gave up waiting for placed
	// operations. It is written before done closes and read after Close observes
	// that close, so the channel supplies the ordering.
	drainErr error
}

// NewURingScheduler creates an io_uring-backed scheduler. The kernel must
// provide the setup and feature guarantees required by ringo.New.
func NewURingScheduler(opts ...Option) (*URingScheduler, error) {
	if !IOUringAvailable {
		return nil, errors.New("iosched: io_uring not available on this kernel")
	}

	cfg := makeSchedulerConfig(opts)
	if err := cfg.budget.validate(); err != nil {
		return nil, err
	}
	if cfg.dmaPoolSet && cfg.dmaPool == nil {
		return nil, errors.New("iosched: cannot register a nil DMA slab")
	}
	dmaPool := cfg.dmaPool
	cfg.dmaPool = nil
	cfg.dmaPoolSet = false

	ringOptions := []ringo.Option{ringo.WithDepth(cfg.ringDepth)}
	if cfg.sqPoll {
		ringOptions = append(ringOptions, ringo.WithSQPoll())
	}
	if cfg.vfiles > 0 {
		ringOptions = append(ringOptions, ringo.WithFixedFiles(cfg.vfiles))
	}
	doorbellFD, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("iosched: doorbell eventfd: %w", err)
	}
	ring, err := ringo.New(ringOptions...)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("iosched: io_uring_setup: %w", err), unix.Close(doorbellFD))
	}
	cfg.ringDepth = uint32(ring.Capacity())
	// The setup failures below have their own error to report; a failure to
	// release the half-built ring or doorbell is joined to it.
	abandon := func(err error) error {
		return errors.Join(err, ring.Close(), unix.Close(doorbellFD))
	}
	if cfg.ringDepth <= doorbellSlots {
		return nil, abandon(fmt.Errorf(
			"iosched: ring depth %d leaves no entries beside the coordinator's doorbell", cfg.ringDepth,
		))
	}

	fixedFiles := make([]ringo.FixedFile, cfg.vfiles)
	for index := range fixedFiles {
		fixedFiles[index], err = ring.FixedFiles().File(uint32(index))
		if err != nil {
			return nil, abandon(fmt.Errorf("iosched: fixed-file slot %d: %w", index, err))
		}
	}

	s := &URingScheduler{
		config:     cfg,
		ring:       ring,
		fixedFiles: fixedFiles,
		done:       make(chan struct{}),
		wakeup:     make(chan struct{}, 1),
		doorbellFD: doorbellFD,
	}
	if dmaPool != nil {
		buffers, err := ring.RegisterBuffers(dmaPool.RawData())
		if err != nil {
			return nil, abandon(fmt.Errorf("iosched: io_uring_register_buffers: %w", err))
		}
		s.registeredPool = dmaPool
		s.registeredBuffers = buffers
	}
	go s.loop()
	return s, nil
}

// Submit enqueues op for asynchronous execution.
func (s *URingScheduler) Submit(op Op) (Ticket, error) {
	if err := s.stopCause(); err != nil {
		return Ticket{}, err
	}

	n, err := countAndValidateOps(&op, &s.config)
	if err != nil {
		return Ticket{}, err
	}
	if limit := min(maxChainLength, int(s.config.ringDepth)-doorbellSlots); int(n) > limit {
		return Ticket{}, fmt.Errorf(
			"iosched: linked chain of %d operations exceeds the limit of %d", n, limit,
		)
	}
	if err := s.validateFixedBuffers(&op); err != nil {
		return Ticket{}, err
	}

	request, ticket := newSubmission(op, n)
	if !s.tryPush(request) {
		return Ticket{}, s.stopCause()
	}
	// Wake the coordinator only if it asked to be woken. Clearing the request
	// makes this the one Submit that wakes it from that wait. The push came
	// first and the coordinator sets the request before it rechecks the staging
	// stack; Go atomics behave as though executed in one sequentially
	// consistent order, so either that recheck sees this push or this swap sees
	// the request.
	switch s.wake.Swap(wakeNone) {
	case wakeChannel:
		s.wakeChannel()
	case wakeDoorbell:
		if err := s.ringDoorbell(); err != nil {
			// The coordinator may be asleep with only the doorbell read in
			// flight, so the work just staged would never be placed. Stop the
			// scheduler, and cancel the ring's operations so that the
			// coordinator's wait ends and it sees the stop; cancellation is best
			// effort, exactly as in Close.
			s.signalShutdown(err)
			_ = s.ring.CancelAll(shutdownCancelTimeout)
		}
	}
	return ticket, nil
}

// wakeChannel wakes a parked coordinator, or leaves the wakeup buffered for
// the next time it parks.
func (s *URingScheduler) wakeChannel() {
	select {
	case s.wakeup <- struct{}{}:
	default:
	}
}

// ringDoorbell adds doorbellIncrement to the doorbell counter, completing the
// queued read. It cannot block or be interrupted: an eventfd write waits only
// when the counter would overflow, and each read resets it.
func (s *URingScheduler) ringDoorbell() error {
	if _, err := unix.Write(s.doorbellFD, doorbellIncrement[:]); err != nil {
		return fmt.Errorf("iosched: doorbell write: %w", err)
	}
	return nil
}

func (s *URingScheduler) validateFixedBuffers(root *Op) error {
	for op := root; op != nil; op = op.linked {
		if !op.isFixed() {
			continue
		}
		if s.registeredBuffers == nil {
			return errors.New("iosched: fixed-buffer operation requires WithDMASlab")
		}
		if _, err := s.registeredBuffers.Bind(op.buf); err != nil {
			return fmt.Errorf("iosched: fixed buffer is outside the registered DMA slab: %w", err)
		}
	}
	return nil
}

func (s *URingScheduler) tryPush(request *submission) bool {
	for {
		head := s.stagingHead.Load()
		if head == &stagingClosed {
			return false
		}
		request.staged = head
		if s.stagingHead.CompareAndSwap(head, request) {
			return true
		}
	}
}

func (s *URingScheduler) closeStaging() *submission {
	head := s.stagingHead.Swap(&stagingClosed)
	if head == &stagingClosed {
		return nil
	}
	return head
}

const (
	// shutdownCancelTimeout bounds each synchronous cancellation attempt during
	// shutdown. Cancellation is best effort, so exceeding it is not an error.
	shutdownCancelTimeout = time.Second
	// A cancellation attempt can race a batch being placed. Retry promptly while
	// the coordinator can still enter a blocking submission, without spinning if
	// the kernel reports that there is currently nothing to cancel.
	shutdownCancelRetryDelay = time.Millisecond
)

// Close stops the coordinator and releases ring resources. Unplaced work is
// failed with the scheduler-closed error. Placed work retains its resources
// until its final completion; Close requests cancellation but may wait for
// noncancelable in-flight file I/O. Cancellation is retried until the
// coordinator exits, so work placed after an earlier attempt is not missed.
// Close must be called exactly once. Callers must stop submitting first; racing
// Close with Submit is not supported.
//
// Close returns an error if the ring stopped reporting completions while
// operations were still placed. Tickets for that work report the same error,
// and Ringo permanently retains those operands because the kernel may still be
// using them. A caller that registered a DMA slab must not close it in that
// case, and must not reuse the buffers involved.
func (s *URingScheduler) Close() error {
	if s.signalShutdown(errSchedulerClosed) {
		// The coordinator may be parked or asleep in io_uring_enter; wake it
		// either way. Ringing is best effort: if the write fails, the
		// cancellation below completes the doorbell read instead, and Close
		// returns only once the coordinator has drained either way.
		s.wakeChannel()
		_ = s.ringDoorbell()
	}
	cancelUntilCoordinatorDone(s.done, s.ring.CancelAll)

	err := s.drainErr
	// Ringo's Close always releases the ring. If operations are still pending,
	// because the drain gave up, it retains their operands and reports
	// ringo.ErrPending. The kernel may still use the registered DMA slab and the
	// doorbell then, so they must outlive this call too.
	ringErr := s.ring.Close()
	err = errors.Join(err, ringErr)
	if ringErr != nil {
		return err
	}
	s.registeredPool = nil
	s.registeredBuffers = nil
	err = errors.Join(err, unix.Close(s.doorbellFD))
	s.doorbellFD = -1
	return err
}

// cancelUntilCoordinatorDone closes the window in which one cancellation can
// run just before the coordinator places and submits a new batch. It is a
// shutdown-only path and does not add synchronization to normal I/O.
func cancelUntilCoordinatorDone(
	done <-chan struct{},
	cancel func(time.Duration) error,
) {
	for {
		select {
		case <-done:
			return
		default:
		}

		// ANY ignores the match key; ALL cancels every request. The operation is
		// best effort: a noncancelable request keeps Close waiting for the
		// coordinator, exactly as documented.
		_ = cancel(shutdownCancelTimeout)
		select {
		case <-done:
			return
		case <-time.After(shutdownCancelRetryDelay):
		}
	}
}

func (s *URingScheduler) signalShutdown(err error) bool {
	return s.stop.CompareAndSwap(nil, &err)
}

func (s *URingScheduler) stopCause() error {
	state := s.stop.Load()
	if state == nil {
		return nil
	}
	return *state
}

type coordinator struct {
	sched *URingScheduler
	ring  *ringo.Ring

	// The fields below are state carried from one pass to the next. Only the
	// coordinator goroutine touches them; what Submit and Close share with it
	// lives in URingScheduler.

	// slots holds scheduler-owned completion state indexed by ringo.Handle.Index,
	// one entry per ring entry. Entries are addressed, not hashed, so a
	// completion costs an array access.
	slots []ringSlot
	// occupied counts occupied entries in slots: operations queued for or
	// handed to the kernel whose final completion has not been reaped yet, the
	// doorbell read included. It is what limits placement.
	occupied int

	// accepted holds every accepted submission until its ticket completes.
	accepted submissionQueue
	// ready holds the accepted work eligible for placement, oldest first. Work
	// waiting for a barrier stays out of it until the barrier releases it.
	ready submissionQueue

	files        fileTable
	nextSequence uint64

	// inFlightCost is the cost of each class's operations in flight, in
	// nanoseconds of device time; see ioBudget.
	inFlightCost [budgetedClasses]int64

	// syncPending holds the files with durable writes waiting for an fdatasync
	// that is not placed yet, in the order their first write completed.
	syncPending []*fileState

	// alloc recycles Ringo operation objects. Only this goroutine builds them
	// and only this goroutine reaps them, so its lock is never contended.
	alloc ringo.OpAlloc
}

func newCoordinator(s *URingScheduler) *coordinator {
	return &coordinator{
		sched:    s,
		ring:     s.ring,
		slots:    make([]ringSlot, s.config.ringDepth),
		accepted: submissionQueue{links: acceptedLinks},
		ready:    submissionQueue{links: placementLinks},
		files:    newFileTable(s.config.vfiles),
	}
}

func (s *URingScheduler) loop() {
	defer close(s.done)
	c := newCoordinator(s)
	s.drainErr = c.shutdown(c.run())
}

// shutdown retires the coordinator once run has returned cause. It returns an
// error only when the drain gave up on placed operations.
func (c *coordinator) shutdown(cause error) error {
	s := c.sched
	// Fail every later Submit. Close may have stopped the scheduler already.
	s.signalShutdown(cause)
	// The drain below waits for every placed operation, the doorbell read
	// included. Ringing the doorbell completes that read even if the
	// cancellation below does not reach it, and the cancellation completes it if
	// this write fails, so the write is best effort like the cancellation.
	_ = s.ringDoorbell()
	// Cancellation is best effort and it races submission: Close cancels before
	// it waits for the coordinator, so a batch placed in between was never
	// offered to that cancellation, and a coordinator-initiated shutdown has not
	// cancelled at all. Ask once more now that placement has stopped. Draining
	// final completions below remains the ownership barrier.
	_ = s.ring.CancelAll(shutdownCancelTimeout)
	// Closing the stack makes any Submit still racing this shutdown fail rather
	// than stage work no one will take. Failing staged work is order-free.
	staged := s.closeStaging()
	// A drain that gives up leaves placed work that never reported; the kernel
	// may still be using its operands, so its tickets report an unknown outcome.
	drainErr := c.waitForInflight()
	placedErr := cause
	if drainErr != nil {
		placedErr = drainErr
	}
	c.failRemaining(staged, cause, placedErr)
	return drainErr
}

// How work moves through the coordinator, with its workState in brackets:
//
//	Submit ──► staging stack ──takeStaged──► accept ──check rejects──► ticket fails
//	                                           │
//	                     every accepted submission is on the accepted queue
//	                     until its ticket completes
//	                     ┌─────────────────────┴───────────────────┐
//	                     ▼ held by a lifecycle barrier             ▼ no barrier
//	                [waiting] ───────barrier released──────► ready queue [ready]
//	                                                               │ placeReady: in
//	                                                               │ order, while it fits
//	                                                               ▼
//	                                        doorbell read ──► ring [issued]
//	                                                               │ reap
//	                     ┌─────────────────────────────────────────┤
//	                     ▼ a durable write's write completed       ▼ nothing more owed
//	          file's sync batch, file on syncPending         ticket completes [done]
//	                     │ placeReady: fdatasyncs first            ▲
//	                     ▼                                         │
//	                   ring ──────────────── reap ─────────────────┘
//
// run is the coordinator's loop. Each pass reaps what has completed, takes new
// submissions and places what fits. If the pass placed anything, it is handed
// to the kernel without waiting and the next pass begins. Otherwise nothing
// more can happen until a completion arrives or, if new work would fit, until
// new work is submitted, so the coordinator sleeps until one of them does.
func (c *coordinator) run() error {
	// doorbell is the doorbell read, while it is in the ring. Once it has
	// completed, the next pass queues a new one before placing caller work: reap
	// cannot, because nothing may push while Ringo lends the ring to it.
	var doorbell ringo.Handle
	for {
		c.reap()
		// A completion can stop the scheduler: a failed doorbell read does.
		if stop := c.sched.stopCause(); stop != nil {
			return stop
		}
		// Reaping the doorbell read's completion clears its slot.
		if doorbell == (ringo.Handle{}) || c.slots[doorbell.Index()].handle != doorbell {
			doorbell = c.armDoorbell()
		}
		c.accept(c.sched.takeStaged())
		placed, room := c.placeReady(c.sched.config.coalescing)

		// After placement, accepted work requires at least one occupied ring slot
		// besides the doorbell.
		if err := buildutil.Assert(c.occupied > doorbellSlots || c.accepted.len == 0); err != nil {
			return fmt.Errorf("iosched: accepted work has no runnable or issued operation: %w", err)
		}

		if err := c.submitAndWait(placed, room); err != nil {
			return fmt.Errorf("iosched: ring error: %w", err)
		}
	}
}

// submitAndWait hands the operations this pass placed to the kernel and waits
// for something to do: in the same io_uring_enter, for at least one
// completion, or parked on the wakeup channel when nothing is in flight. It
// skips the wait only when more work is already staged, so the next pass can
// take it.
//
// Without room for new work (see placeReady), only a completion can help.
// With doorbellMaxInFlight operations in flight, a completion is due about as
// soon as a wakeup would be. Otherwise it first asks Submit to wake it: through
// the doorbell, whose read is a completion that ends the wait, or, with only
// the doorbell in the ring, through the channel, which costs a submitter less
// than a write and a wakeup from io_uring_enter.
func (c *coordinator) submitAndWait(placed int, room bool) error {
	s := c.sched
	inFlight := c.occupied - doorbellSlots
	if !room || inFlight >= doorbellMaxInFlight {
		return c.enter(1)
	}
	idle := inFlight == 0
	mode := wakeDoorbell
	if idle {
		mode = wakeChannel
	}
	// Ask before looking at the staging stack. A Submit that pushes after this
	// load sees the request and wakes the coordinator; one that pushed before
	// it is seen here (see Submit for why one side always sees the other). A
	// closed stack is also non-nil and sends the loop to shutdown.
	s.wake.Store(mode)
	defer s.wake.Store(wakeNone)
	if s.stagingHead.Load() != nil {
		if placed == 0 {
			return nil
		}
		return c.enter(0)
	}
	if idle {
		// Nothing is in flight but the doorbell read, which only a Submit could
		// complete, so there is nothing to submit or reap until one arrives.
		<-s.wakeup
		return nil
	}
	return c.enter(1)
}

// armDoorbell queues the doorbell read. The read completes when Submit or Close
// rings the eventfd. It is not caller work: it carries no ticket and holds the
// entry reserved for it. The pass that arms it does so before placing caller
// work, and the read's own completion freed the entry it needs, so the push
// cannot find the ring full. Ringo retains the buffer until the read
// completes, so each read gets its own.
func (c *coordinator) armDoorbell() ringo.Handle {
	read := ringo.Read(ringo.BorrowedFD(c.sched.doorbellFD), make([]byte, 8), 0, ringo.WithAlloc(&c.alloc))
	handle, err := c.ring.Push(read)
	if err != nil {
		panic(fmt.Sprintf("iosched: Ringo rejected the doorbell read: %v", err))
	}
	c.place(handle, ringSlot{kind: slotDoorbell})
	return handle
}

func (s *URingScheduler) takeStaged() *submission {
	var out *submission
	for head := s.stagingHead.Swap(nil); head != nil; {
		next := head.staged
		head.staged = out
		out = head
		head = next
	}
	return out
}

func (c *coordinator) accept(head *submission) {
	for head != nil {
		work := head
		head = head.staged
		work.staged = nil
		root := &work.root
		work.durable = root.durable() && (root.kind() == OpWrite || root.kind() == OpWritev)

		var inline [4]fileUse
		uses := fileUses(work, inline[:0])
		if err := c.files.check(uses); err != nil {
			completeFailed(root, err)
			continue
		}
		work.class, work.cost = c.sched.config.budget.cost(root)
		work.remaining = work.count
		if work.durable {
			work.remaining++ // the fdatasync that covers the write
		}
		work.sequence = c.nextSequence
		c.nextSequence++
		c.accepted.push(work)
		c.admit(work, uses)
		if work.waitCount == 0 {
			c.makeReady(work)
		}
	}
}

func (c *coordinator) releaseWait(work *submission) {
	work.waitCount--
	if err := buildutil.Assert(work.waitCount >= 0); err != nil {
		panic(err)
	}
	if work.waitCount == 0 {
		c.makeReady(work)
	}
}

// placeReady places what waits for the ring and returns how many entries it
// placed, and whether new work could still be placed now. Pending fdatasyncs
// go first: the writes they cover have completed, so each one only completes
// tickets. Ready work follows, oldest first, while it fits in the ring and in
// its class's in-flight budget. Once one item of a class does not fit its
// budget, later items of that class wait too, so a large operation is not
// overtaken indefinitely, but other classes go on: a read never waits for the
// write budget. A run of adjacent contiguous writes becomes one write; anything
// else is placed as its own linked chain, one entry per operation.
func (c *coordinator) placeReady(coalesce bool) (placed int, room bool) {
	start := c.occupied
	free := func() int { return int(c.sched.config.ringDepth) - c.occupied }

	syncs := 0
	for _, state := range c.syncPending {
		if free() == 0 {
			break
		}
		c.placeSync(state)
		syncs++
	}
	c.syncPending = c.syncPending[:copy(c.syncPending, c.syncPending[syncs:])]
	if len(c.syncPending) != 0 {
		return c.occupied - start, false
	}

	var overBudget [budgetedClasses]bool
	for work := c.ready.head; work != nil; {
		if work.class != classOther && overBudget[work.class] {
			work = c.ready.next(work)
			continue
		}
		run := 1
		if coalesce {
			run = c.coalescedRun(work)
		}
		need, cost := work.root.opCount(), work.cost
		if run > 1 {
			need = 1
			for member, i := c.ready.next(work), 1; i < run; member, i = c.ready.next(member), i+1 {
				cost += member.cost
			}
		}
		if need > free() {
			return c.occupied - start, false
		}
		if !c.fitsBudget(work.class, cost) {
			overBudget[work.class] = true
			if overBudget[classRead] && overBudget[classWrite] {
				break
			}
			work = c.ready.next(work)
			continue
		}
		if run > 1 {
			work = c.placeCoalescedRun(work, run)
		} else {
			work = c.placeChain(work)
		}
	}
	return c.occupied - start, free() > 0 && !(overBudget[classRead] && overBudget[classWrite])
}

// fitsBudget reports whether cost more of class may be placed. A class with
// nothing in flight takes any one operation, however costly, so none waits
// forever.
func (c *coordinator) fitsBudget(class ioClass, cost int64) bool {
	if class == classOther {
		return true
	}
	inFlight := c.inFlightCost[class]
	return inFlight == 0 || inFlight+cost <= int64(c.sched.config.budget.goal)
}

// take removes work from the ready queue, marks it issued and charges its
// cost to its class's budget.
func (c *coordinator) take(work *submission) {
	c.ready.remove(work)
	work.state = workIssued
	if work.class != classOther && work.cost != 0 {
		c.inFlightCost[work.class] += work.cost
		work.charged = true
	}
}

// release returns work's cost to its class's budget once the operation it was
// charged for has completed.
func (c *coordinator) release(work *submission) {
	if work.charged {
		c.inFlightCost[work.class] -= work.cost
		work.charged = false
	}
}

// coalescedRun returns how many ready entries, starting at first, form one run
// of writes that can be coalesced into a writev: adjacent submissions writing
// contiguous ranges of the same file. A head that cannot be coalesced is a run
// of one.
func (c *coordinator) coalescedRun(first *submission) int {
	firstOp := &first.root
	if firstOp.linked != nil || !firstOp.coalescibleWrite() {
		return 1
	}
	run := 1
	sequence := first.sequence
	runEnd := firstOp.offset + int64(len(firstOp.buf))
	// Only adjacent submissions are coalesced. Searching past unrelated work
	// would require proving that every skipped operation is independent of this
	// file; sorting by offset would invent ordering that separate Submit calls do
	// not provide. Keep the placement rule local and predictable instead.
	for work := c.ready.next(first); work != nil && run < maxCoalescedWrites; work = c.ready.next(work) {
		op := &work.root
		sequence++
		if work.sequence != sequence || op.linked != nil || !op.coalescibleWrite() ||
			!sameFile(firstOp, op) || op.offset != runEnd {
			break
		}
		runEnd += int64(len(op.buf))
		run++
	}
	return run
}

func ringoLinkType(op *Op) ringo.LinkType {
	if op.sqeFlags&sqeHardLink != 0 {
		return ringo.LinkHard
	}
	return ringo.LinkSoft
}

// placeChain places work as one linked chain, one ring entry per operation,
// linked as the caller linked them, and returns the ready work after it.
func (c *coordinator) placeChain(work *submission) (next *submission) {
	next = c.ready.next(work)
	c.take(work)
	root := &work.root
	first := c.translateOp(root)
	if root.linked == nil {
		ringHandle, err := c.ring.Push(first)
		if err != nil {
			panic(fmt.Sprintf("iosched: Ringo rejected a validated operation: %v", err))
		}
		c.place(ringHandle, ringSlot{kind: slotOperation, work: work, op: root})
		return next
	}

	// PushLinked does not retain links.
	links := make([]ringo.Link, 0, root.opCount()-1)
	for op := root; op.linked != nil; op = op.linked {
		links = append(links, ringo.Then(ringoLinkType(op), c.translateOp(op.linked)))
	}
	ringHandles, err := c.ring.PushLinked(first, links[0], links[1:]...)
	if err != nil {
		panic(fmt.Sprintf("iosched: Ringo rejected a validated chain: %v", err))
	}
	op := root
	for _, ringHandle := range ringHandles {
		c.place(ringHandle, ringSlot{kind: slotOperation, work: work, op: op})
		op = op.linked
	}
	return next
}

// placeCoalescedRun places the run ready entries starting at first, adjacent
// writes to contiguous ranges of one file, as one writev of their buffers, and
// returns the ready work after them. The leader records every member, so the
// one completion can be split back into each member's result.
func (c *coordinator) placeCoalescedRun(first *submission, run int) (next *submission) {
	members := make([]*submission, run)
	// Ringo snapshots the buffer list into the Op it builds.
	bufs := make([][]byte, run)
	next = first
	for i := range members {
		members[i] = next
		next = c.ready.next(next)
		c.take(members[i])
		bufs[i] = members[i].root.buf
	}
	leader := members[0]
	leader.coalesced = members
	writev := leader.root
	writev.opcode = OpWritev | (leader.root.opcode & opVirtual)
	writev.bufs = bufs
	ringHandle, err := c.ring.Push(c.translateOp(&writev))
	if err != nil {
		panic(fmt.Sprintf("iosched: Ringo rejected a validated write: %v", err))
	}
	c.place(ringHandle, ringSlot{kind: slotCoalescedWrite, work: leader})
	return next
}

// placeSync places one fdatasync for the durable writes waiting on state's
// file. Their writes have all completed, so it covers each of them.
func (c *coordinator) placeSync(state *fileState) {
	batch := state.syncBatch
	state.syncBatch = submissionQueue{}
	sync := batch.head.root.syncOp()
	ringHandle, err := c.ring.Push(c.translateOp(&sync))
	if err != nil {
		panic(fmt.Sprintf("iosched: Ringo rejected a validated fdatasync: %v", err))
	}
	c.place(ringHandle, ringSlot{kind: slotSync, batch: batch})
}

func (c *coordinator) translateOp(op *Op) ringo.Op {
	recycle := ringo.WithAlloc(&c.alloc)
	var direct ringo.FixedFile
	fd := ringo.FileFD(op.f)
	if op.isVirtual() {
		direct = c.sched.fixedFiles[op.vfd]
		fd = ringo.FixedFD(direct)
	}

	switch op.kind() {
	case OpRead:
		if op.isFixed() {
			buffer, err := c.sched.registeredBuffers.Bind(op.buf)
			if err != nil {
				panic(fmt.Sprintf("iosched: validated fixed buffer became invalid: %v", err))
			}
			return ringo.ReadFixed(fd, buffer, op.offset, recycle)
		}
		return ringo.Read(fd, op.buf, op.offset, recycle)
	case OpWrite:
		if op.isFixed() {
			buffer, err := c.sched.registeredBuffers.Bind(op.buf)
			if err != nil {
				panic(fmt.Sprintf("iosched: validated fixed buffer became invalid: %v", err))
			}
			return ringo.WriteFixed(fd, buffer, op.offset, recycle)
		}
		return ringo.Write(fd, op.buf, op.offset, recycle)
	case OpReadv:
		return ringo.Readv(fd, op.bufs, op.offset, 0, recycle)
	case OpWritev:
		return ringo.Writev(fd, op.bufs, op.offset, 0, recycle)
	case OpFsync:
		return ringo.Fsync(fd, recycle)
	case OpFdatasync:
		return ringo.Fdatasync(fd, recycle)
	case OpFallocate:
		return ringo.Fallocate(fd, op.offset, op.length, recycle)
	case OpOpenat:
		path := string(op.path[:len(op.path)-1])
		if op.isVirtual() {
			return ringo.OpenAtDirect(
				ringo.BorrowedFD(op.dfd),
				path,
				op.openFlag,
				op.mode,
				direct,
				recycle,
			)
		}
		return ringo.OpenAt(
			ringo.BorrowedFD(op.dfd),
			path,
			op.openFlag,
			op.mode,
			recycle,
		)
	case OpClose:
		if op.isVirtual() {
			return ringo.CloseDirect(direct, recycle)
		}
		return ringo.Nop(recycle)
	default:
		panic(fmt.Sprintf("iosched: invalid opcode %d", op.opcode))
	}
}

// enter submits queued SQEs and asks io_uring to wait for at least minComplete
// completions; zero submits without waiting. Operation results arrive through
// CQEs, not as enter errors, and enter does not reap them: every caller reaps
// next. Ringo retries EINTR itself.
//
// EAGAIN and EBUSY are not failures: io_uring_enter(2) documents them as
// temporary resource conditions to resolve by reaping and retrying, which the
// caller's next reap and pass do. enter pauses briefly first so that a
// condition that persists does not busy-loop, and returns nil.
func (c *coordinator) enter(minComplete int) error {
	_, err := c.ring.SubmitAndWait(minComplete)
	if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EBUSY) {
		time.Sleep(time.Microsecond)
		return nil
	}
	return err
}

// reap applies every available completion. It only records results: Ringo
// lends the ring to the iterator, so nothing reached from reapOne may push.
func (c *coordinator) reap() {
	for completion := range c.ring.Reap() {
		c.reapOne(completion)
	}
}

// drainStallLimit bounds how many consecutive drain rounds may reap nothing
// before the coordinator stops waiting. On a usable ring SubmitAndWait blocks
// until at least one completion is available, so a round that reaps nothing
// means the ring can no longer report: it has recorded a fault, or entering it
// keeps failing. Neither clears on its own. Tests lower these.
//
// TODO: a stall round only occurs when entering the ring fails, so this is a
// busy retry against a failing syscall that gives up after about a second.
// Replace the fixed count and delay with a backoff against a deadline.
var (
	drainStallLimit = 1024
	drainStallDelay = time.Millisecond
)

// waitForInflight keeps every placed operation and its ticket alive until Ringo
// yields the operation's final completion. Cancellation only shortens this
// wait; an unsupported, timed-out, or otherwise failed cancellation does not
// relax the ownership boundary.
//
// It returns an error only when the ring stops reporting completions
// altogether, leaving c.slots populated. That is not recoverable: the
// coordinator cannot tell an operation whose completion was lost from one still
// running, so it can prove nothing about the operands either still holds.
func (c *coordinator) waitForInflight() error {
	stalls := 0
	for c.occupied != 0 {
		// Nothing is placed during the drain, so the ring is still reporting
		// exactly when a reap lowers the occupied count.
		before := c.occupied
		if c.reap(); c.occupied < before {
			stalls = 0
			continue
		}
		err := c.enter(1)
		if c.reap(); c.occupied < before {
			stalls = 0
			continue
		}
		stalls++
		if stalls >= drainStallLimit {
			if err == nil {
				// Entering kept succeeding and reaping kept finding nothing,
				// which SubmitAndWait's minimum completion count should make
				// impossible. Name it rather than wrap a nil.
				err = errors.New("ring reported no completions")
			}
			return fmt.Errorf(
				"iosched: %d io_uring operations stopped reporting completions "+
					"after %d attempts: %w",
				c.occupied, stalls, err,
			)
		}
		// An enter error does not prove that previously submitted I/O has
		// stopped, so keep the state and retry rather than completing tickets
		// whose buffers may still be in use.
		time.Sleep(drainStallDelay)
	}
	return nil
}

// place records scheduler state for a queued operation in the entry its handle
// names. Ringo bounds live operations by the ring depth, which sizes c.slots.
func (c *coordinator) place(handle ringo.Handle, slot ringSlot) {
	slot.handle = handle
	c.slots[handle.Index()] = slot
	c.occupied++
}

func (c *coordinator) reapOne(completion ringo.Completion) {
	index := completion.Handle.Index()
	slot := c.slots[index]
	if slot.handle != completion.Handle {
		panic(fmt.Sprintf(
			"iosched: Ringo returned unknown completion handle with error %v",
			completion.Err,
		))
	}
	c.slots[index] = ringSlot{}
	c.occupied--

	n, err := completion.Result, completion.Err
	if errors.Is(err, syscall.ECANCELED) && c.sched.stopCause() == errSchedulerClosed {
		err = errSchedulerClosed
	}
	switch slot.kind {
	case slotOperation:
		c.finishOperation(slot.work, slot.op, n, err)
	case slotCoalescedWrite:
		c.finishWrite(slot.work, n, err)
	case slotSync:
		c.finishSync(slot.batch, err)
	case slotDoorbell:
		// The read's result is the counter, which says nothing beyond the
		// wakeup, and cancellation only happens at shutdown. Any other failure
		// means the doorbell no longer works: every new read would fail at once,
		// and the coordinator would spin arming them. Stop instead.
		if completion.Err != nil && !errors.Is(completion.Err, syscall.ECANCELED) {
			c.sched.signalShutdown(fmt.Errorf("iosched: doorbell read: %w", completion.Err))
		}
	}
}

// finishWrite splits a coalesced run's result into each member's, in writev
// order: each member gets the bytes written within its own buffer.
func (c *coordinator) finishWrite(leader *submission, n int, err error) {
	members := leader.coalesced
	leader.coalesced = nil
	for _, member := range members {
		written := 0
		if err == nil {
			written = min(len(member.root.buf), n)
			n -= written
		}
		c.finishOperation(member, &member.root, written, err)
	}
}

// finishOperation records the result of one of the work's operations. A
// durable write that succeeded then waits for its file's next fdatasync; one
// that failed has nothing to make durable, so its fdatasync is retired with it.
func (c *coordinator) finishOperation(work *submission, op *Op, n int, err error) {
	if op == &work.root {
		c.release(work) // the cost was the first operation's
	}
	err = writeResultError(op, n, err)
	recordResult(&work.root, op, n, err)
	c.operationDone(work, op)
	switch {
	case work.durable && err == nil:
		c.awaitSync(work)
	case work.durable:
		c.operationDone(work, op)
	}
}

// awaitSync adds a durable write whose write has completed to the batch for
// its file's next fdatasync. Every durable write on the file that completes
// before that fdatasync is placed shares it.
func (c *coordinator) awaitSync(work *submission) {
	state := c.files.lookup(&work.root)
	if state.syncBatch.len == 0 {
		c.syncPending = append(c.syncPending, state)
	}
	state.syncBatch.push(work)
}

// finishSync completes every durable write in the batch an fdatasync covered
// with the fdatasync's error.
func (c *coordinator) finishSync(batch submissionQueue, err error) {
	for batch.len != 0 {
		work := batch.pop()
		recordError(&work.root, err)
		c.operationDone(work, &work.root)
	}
}

// operationDone retires one completion the work owed: op's file bookkeeping,
// then the work itself once nothing more is owed. The fdatasync covering a
// durable write is retired as its write, the operation it makes durable.
func (c *coordinator) operationDone(work *submission, op *Op) {
	if err := buildutil.Assert(work.state == workIssued); err != nil {
		panic(err)
	}
	c.completedOperation(work, op)
	work.remaining--
	if work.remaining != 0 {
		return
	}
	c.completedWork(work)
	c.accepted.remove(work)
	work.state = workDone
	work.root.done.Done()
}

// failRemaining completes every ticket the coordinator still owns, staged
// included. Work that never reached the ring fails with unplaced; work whose
// SQEs were placed but never reported fails with placed, which the caller must
// treat as an unknown outcome rather than a clean failure. Only a drain that
// gave up leaves placed work behind.
func (c *coordinator) failRemaining(staged *submission, unplaced, placed error) {
	for c.ready.len != 0 {
		c.ready.pop()
	}
	for c.accepted.len != 0 {
		work := c.accepted.pop()
		// Work in a file's sync batch is still linked there; the table goes
		// away with the coordinator, but the Ticket keeps work reachable.
		work.links = [2]queueLinks{}
		if work.state == workIssued {
			completeUnknown(&work.root, placed)
		} else {
			completeFailed(&work.root, unplaced)
		}
		work.state = workDone
	}
	for staged != nil {
		next := staged.staged
		staged.staged = nil
		completeFailed(&staged.root, unplaced)
		staged = next
	}
}

func completeFailed(root *Op, err error) {
	if root.err == nil {
		root.err = err
	}
	root.done.Done()
}

// completeUnknown reports that one of root's operations never told the
// coordinator what happened to it. Only a drain that gave up reaches this. It
// replaces any result another operation in the same work recorded, because that
// result reads as a settled failure, and a settled failure is what tells a
// caller its buffers are its own again.
func completeUnknown(root *Op, err error) {
	root.err = err
	root.done.Done()
}
