package iosched

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/v2/internal/buildutil"
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
	registeredBuffers *ringo.FixedBuffers
	stagingHead       atomic.Pointer[submission]
	done              chan struct{}
	stagingPeak       atomic.Int64
	waitingDepth      queueDepth
	readyDepth        queueDepth
	inFlightDepth     queueDepth

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

	// drainErr records a coordinator failure or shutdown timeout. It is written
	// before done closes and read after Close observes that close.
	drainErr error
}

// QueueStats reports current submission depths and lifetime high-water marks.
// It is safe to call concurrently with Submit or completion. Staged depth is
// read from the immutable published head, without traversing the stack or
// synchronizing with its reversal. A detached batch being accepted is between
// Staged and the coordinator queues, so these fields are not a total-work count.
func (s *URingScheduler) QueueStats() QueueStats {
	var staged int64
	if head := s.stagingHead.Load(); head != nil && head != &stagingClosed {
		staged = head.stagedDepth
	}
	return QueueStats{
		Staged:   QueueDepth{Current: staged, Peak: max(staged, s.stagingPeak.Load())},
		Waiting:  s.waitingDepth.snapshot(),
		Ready:    s.readyDepth.snapshot(),
		InFlight: s.inFlightDepth.snapshot(),
	}
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
	if cfg.coordinatorCPU < -1 {
		return nil, fmt.Errorf("iosched: invalid coordinator CPU %d", cfg.coordinatorCPU)
	}
	data := make([][]byte, len(cfg.dmaBuffers))
	for i, buffer := range cfg.dmaBuffers {
		chunks := buffer.Chunks()
		if len(chunks) > 1 {
			return nil, fmt.Errorf("iosched: DMA buffer %d has %d chunks; expected one", i, len(chunks))
		}
		data[i] = chunks[0]
	}
	cfg.dmaBuffers = nil

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
	if len(data) != 0 {
		buffers, err := ring.RegisterBuffers(data...)
		if err != nil {
			return nil, abandon(fmt.Errorf("iosched: io_uring_register_buffers: %w", err))
		}
		s.registeredBuffers = buffers
	}
	go s.loop()
	return s, nil
}

// Submit enqueues op for asynchronous execution.
func (s *URingScheduler) Submit(op Op) (Ticket, error) {
	started := time.Now()
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

	request, ticket := newSubmission(op, n, started)
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
			return errors.New("iosched: fixed-buffer operation requires WithDMABuffers")
		}
		if _, err := s.registeredBuffers.Bind(op.buf); err != nil {
			return fmt.Errorf("iosched: fixed buffer is outside the registered DMA buffers: %w", err)
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
		request.stagedDepth = 1
		if head != nil {
			request.stagedDepth += head.stagedDepth
		}
		if s.stagingHead.CompareAndSwap(head, request) {
			observeQueuePeak(&s.stagingPeak, request.stagedDepth)
			return true
		}
	}
}

func (s *URingScheduler) closeStaging() *submission {
	head := s.stagingHead.Swap(&stagingClosed)
	if head == &stagingClosed {
		return nil
	}
	return reverseStaged(head)
}

// shutdownTimeout covers the graceful drain, cancellation, and final reap.
// Tests shorten it; production has one deadline for the entire shutdown.
var shutdownTimeout = 5 * time.Second

const shutdownCancelTimeout = time.Second

// Close stops admission and asks the coordinator to finish accepted work.
// The coordinator owns the bounded drain and cancellation; Close then releases
// the ring. Callers must stop submitting first and call Close exactly once.
func (s *URingScheduler) Close() error {
	if s.signalShutdown(errSchedulerClosed) {
		s.wakeChannel()
		if err := s.ringDoorbell(); err != nil {
			_ = s.ring.CancelAll(shutdownCancelTimeout)
		}
	}
	<-s.done
	ringErr := s.ring.Close()
	err := errors.Join(s.drainErr, ringErr)
	if ringErr != nil {
		return err // Ringo retains resources still held by outstanding operations.
	}
	s.registeredBuffers = nil
	err = errors.Join(err, unix.Close(s.doorbellFD))
	s.doorbellFD = -1
	return err
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
	runtime.LockOSThread()
	// Exit locked so the runtime cannot reuse a thread with restricted affinity.
	if cpu := s.config.coordinatorCPU; cpu >= 0 {
		var set unix.CPUSet
		set.Set(cpu)
		if err := unix.SchedSetaffinity(0, &set); err != nil {
			slog.Warn("iosched: cannot pin coordinator", "cpu", cpu, "error", err)
		}
	}
	defer close(s.done)
	c := newCoordinator(s)
	s.drainErr = c.shutdown(c.run())
}

// shutdown is the sole owner of draining accepted work and cancellation.
func (c *coordinator) shutdown(cause error) error {
	c.sched.signalShutdown(cause)
	_ = c.sched.ringDoorbell()
	staged := c.sched.closeStaging()
	graceful := cause == errSchedulerClosed
	if graceful {
		c.accept(staged)
		staged = nil
	}
	err := c.drain(graceful)
	c.failRemaining(staged, cause, errors.Join(cause, err))
	if !graceful {
		return errors.Join(cause, err)
	}
	return err
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
	return reverseStaged(s.stagingHead.Swap(nil))
}

func reverseStaged(head *submission) *submission {
	var out *submission
	for head != nil {
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
		last := root
		for last.linked != nil {
			last = last.linked
		}
		if last.durable() && (last.kind() == OpWrite || last.kind() == OpWritev) {
			work.durable = last
		}

		uses := fileUses(work, work.inlineFile[:0])
		if err := c.files.check(uses); err != nil {
			completeFailed(root, err)
			continue
		}
		work.cost = c.sched.config.budget.costs(root)
		work.remaining = work.count
		if work.durable != nil {
			work.remaining++ // the fdatasync that covers the write
		}
		work.sequence = c.nextSequence
		c.nextSequence++
		c.accepted.push(work)
		c.admit(work, uses)
		if work.waitCount == 0 {
			c.makeReady(work)
		} else {
			c.sched.waitingDepth.add(1)
		}
	}
}

func (c *coordinator) releaseWait(work *submission) {
	work.waitCount--
	if err := buildutil.Assert(work.waitCount >= 0); err != nil {
		panic(err)
	}
	if work.waitCount == 0 {
		c.sched.waitingDepth.add(-1)
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
		blocked := false
		for class, cost := range work.cost {
			blocked = blocked || (cost != 0 && overBudget[class])
		}
		if blocked {
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
				cost[classWrite] += member.cost[classWrite]
			}
		}
		if need > free() {
			return c.occupied - start, false
		}
		for class, amount := range cost {
			if amount != 0 && !c.fitsBudget(ioClass(class), amount) {
				overBudget[class], blocked = true, true
			}
		}
		if blocked {
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
	c.sched.readyDepth.add(-1)
	work.state = workIssued
	c.sched.inFlightDepth.add(1)
	for class, cost := range work.cost {
		c.inFlightCost[class] += cost
	}
	work.charged = true
}

// release returns each operation's cost at its own completion, including
// canceled followers. Metadata operations have no charge.
func (c *coordinator) release(work *submission, op *Op) {
	if work.charged {
		class, cost := c.sched.config.budget.cost(op)
		if class != classOther {
			c.inFlightCost[class] -= cost
		}
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
	cost := first.cost[classWrite]
	goal := int64(c.sched.config.budget.goal)
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
		// The oversized-operation escape is for one caller operation, never an
		// oversized batch manufactured by coalescing. Use the remaining budget.
		nextCost := cost + work.cost[classWrite]
		if goal > 0 && c.inFlightCost[classWrite]+nextCost > goal {
			break
		}
		cost = nextCost
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
		work.completion.markIssued()
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
	work.completion.markIssued()
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
	for _, member := range members {
		member.completion.markIssued()
	}
	c.place(ringHandle, ringSlot{kind: slotCoalescedWrite, work: leader})
	return next
}

// placeSync places one fdatasync for the durable writes waiting on state's
// file. Their writes have all completed, so it covers each of them.
func (c *coordinator) placeSync(state *fileState) {
	batch := state.syncBatch
	state.syncBatch = submissionQueue{}
	sync := batch.head.durable.syncOp()
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
	case OpUnlinkat:
		return ringo.UnlinkAt(ringo.BorrowedFD(op.dfd), string(op.path[:len(op.path)-1]), recycle)
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

// drain gives normal work half the shutdown period, then cancels once and
// reaps until the same deadline. No blocking completion wait can hide that
// deadline. An I/O error takes the cancellation path immediately.
func (c *coordinator) drain(graceful bool) error {
	deadline := time.Now().Add(shutdownTimeout)
	cancelAt := deadline.Add(-shutdownTimeout / 2)
	cancelled := false
	var lastErr error
	for {
		c.reap()
		if c.occupied == 0 && (cancelled || !graceful || c.accepted.len == 0) {
			return lastErr
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("iosched: shutdown timed out with %d operations pending: %w",
				c.occupied, errors.Join(os.ErrDeadlineExceeded, lastErr))
		}
		if !cancelled && graceful && time.Now().Before(cancelAt) {
			c.placeReady(c.sched.config.coalescing)
		}
		// Flush placed SQEs before cancellation so they are visible to the kernel.
		if err := c.enter(0); err != nil {
			lastErr = err
			graceful = false
		}
		if !cancelled && (!graceful || !time.Now().Before(cancelAt)) {
			// Stop placement and request cancellation once. Leave time to reap.
			if err := c.ring.CancelAll(min(shutdownCancelTimeout, remaining/2)); err != nil && !errors.Is(err, syscall.ENOENT) {
				lastErr = err
			}
			cancelled = true
		}
		time.Sleep(time.Millisecond)
	}
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
	c.release(work, op)
	err = writeResultError(op, n, err)
	recordResult(&work.root, op, n, err)
	c.operationDone(work, op)
	if op != work.durable {
		return
	}
	if err != nil {
		c.operationDone(work, op)
		return
	}
	c.awaitSync(work)
}

// awaitSync adds a durable write whose write has completed to the batch for
// its file's next fdatasync. Every durable write on the file that completes
// before that fdatasync is placed shares it.
func (c *coordinator) awaitSync(work *submission) {
	state := work.fileState(work.durable)
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
		c.operationDone(work, work.durable)
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
	c.sched.inFlightDepth.add(-1)
	work.root.finish()
}

// failRemaining completes every ticket the coordinator still owns, staged
// included. Work that never reached the ring fails with unplaced; work whose
// SQEs were placed but never reported fails with placed, which the caller must
// treat as an unknown outcome rather than a clean failure. Only a drain that
// gave up leaves placed work behind.
func (c *coordinator) failRemaining(staged *submission, unplaced, placed error) {
	c.sched.readyDepth.add(-int64(c.ready.len))
	for c.ready.len != 0 {
		c.ready.pop()
	}
	for c.accepted.len != 0 {
		work := c.accepted.pop()
		// Work in a file's sync batch is still linked there; the table goes
		// away with the coordinator, but the Ticket keeps work reachable.
		work.links = [2]queueLinks{}
		switch work.state {
		case workWaiting:
			c.sched.waitingDepth.add(-1)
		case workIssued:
			c.sched.inFlightDepth.add(-1)
		}
		if work.state == workIssued && c.occupied != 0 {
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
	root.finish()
}

// completeUnknown reports a forced shutdown without releasing operands whose
// kernel completions have not been reaped.
func completeUnknown(root *Op, err error) {
	root.err = err
	// A forced close cannot release resources still borrowed by kernel I/O.
	// Report the ticket error, but leave its resource-release callback alone.
	root.completion.whenDone = nil
	root.finish()
}
