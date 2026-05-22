//go:build linux

package iosched

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/miretskiy/dio/giouring"
	"github.com/miretskiy/dio/internal/buildutil"
	"github.com/miretskiy/dio/mempool"
)

// IOUringAvailable reports whether io_uring is supported on the running kernel.
// Probed once at package init time by attempting to create and immediately
// tear down a minimal ring.
var IOUringAvailable = probeIOUring()

func probeIOUring() bool {
	ring := giouring.NewRing()
	if err := ring.QueueInit(1, 0); err != nil {
		return false
	}
	ring.QueueExit()
	return true
}

const defaultRingDepth uint32 = 256

var errSchedulerClosed = errors.New("iosched: scheduler closed")

// ── Submit state ──────────────────────────────────────────────────────────────

// inflightEntry maps a ring slot to the Ticket and op that owns it.
type inflightEntry struct {
	t     *Ticket
	opIdx uint16
	valid bool
}

// ── URingScheduler ────────────────────────────────────────────────────────────

// URingScheduler is an asynchronous [Scheduler] backed by io_uring.
//
// Architecture: a single coordinator goroutine owns the ring exclusively.
// Callers deliver requests to the coordinator via a buffered(1) signal channel
// (fast path, lock-free for single producer) or a mutex-protected pending
// slice (slow path when the channel is full). The coordinator runs a
// sliding-window loop:
//
//	collect pending → fill free SQ slots → SubmitAndWait(1) → reap CQEs → repeat
//
// This keeps the ring as full as possible, maximising NVMe queue depth.
type URingScheduler struct {
	// inflightOps counts ops currently in flight in the scheduler (queued +
	// placed + awaiting reap). Incremented by Submit, decremented by
	// coordinator.complete. Compared against maxInflight to apply
	// non-blocking backpressure via ErrTooBusy.
	inflightOps atomic.Int64
	// maxInflight is the cached cap from URingConfig.MaxInflightOps.
	// 0 means unlimited; Submit never returns ErrTooBusy in that case.
	maxInflight int64

	// overflow is the slow-path queue for tickets that lost the signal-
	// channel fast path. Embeds the mutex so the protection domain is
	// visible at the type level.
	overflow struct {
		sync.Mutex
		queue []*Ticket
	}

	// signal carries Tickets to the coordinator.
	// Fast path: single non-blocking send; slow path: overflow queue.
	signal chan *Ticket // buffered(1)

	// shutdown groups the shutdown state machine. Once serializes the close
	// and err write; close(stop) is the publish edge for err — readers that
	// observe stop closed safely see err.
	shutdown struct {
		sync.Once
		stop chan struct{}
		err  error
	}

	wg sync.WaitGroup

	// stats tracks coordinator-internal counters useful for diagnosing
	// batching behavior. Written only by the coordinator goroutine; read
	// safely by anyone via [URingScheduler.Stats].
	stats struct {
		// syscalls counts io_uring_enter calls that actually placed new
		// SQEs (i.e. cycles where fillSlots produced at least one SQE).
		// Reap-only cycles (waiting on prior-cycle CQEs without new work)
		// are excluded so the ratio opsPlaced/syscalls is the true
		// average batch size.
		syscalls atomic.Uint64
		// opsPlaced counts SQEs prepared and submitted to the kernel.
		opsPlaced atomic.Uint64
	}

	// ring and depth are set in NewURingScheduler before the loop goroutine
	// starts, so they are safe to read from any goroutine without locks.
	ring  *giouring.Ring
	depth uint32
}

// Stats is a point-in-time snapshot of coordinator counters.
type Stats struct {
	// Syscalls is the number of io_uring_enter calls that submitted new SQEs.
	Syscalls uint64
	// OpsPlaced is the total number of SQEs placed into the ring.
	// OpsPlaced / Syscalls gives the average batch size.
	OpsPlaced uint64
}

// Stats returns a snapshot of the coordinator counters.
func (s *URingScheduler) Stats() Stats {
	return Stats{
		Syscalls:  s.stats.syscalls.Load(),
		OpsPlaced: s.stats.opsPlaced.Load(),
	}
}

// usePool implements the unexported pooler interface defined in scheduler.go.
// Calls io_uring_register_buffers; safe from any goroutine because ring.ringFd
// is immutable after QueueInit and io_uring_register does not race with
// io_uring_enter in the kernel.
func (s *URingScheduler) usePool(pool *mempool.SlabPool) error {
	data := pool.RawData()
	iovs := []syscall.Iovec{{Base: &data[0], Len: uint64(len(data))}}
	if _, err := s.ring.RegisterBuffers(iovs); err != nil {
		return fmt.Errorf("iosched: io_uring_register_buffers: %w", err)
	}
	return nil
}

// NewURingScheduler creates an io_uring-backed scheduler.
// The ring is initialised here before the event-loop goroutine starts,
// so callers can call RegisterDMASlab immediately after construction.
func NewURingScheduler(cfg URingConfig) (*URingScheduler, error) {
	if !IOUringAvailable {
		return nil, errors.New("iosched: io_uring not available on this kernel")
	}

	depth := max(cfg.RingDepth, defaultRingDepth)

	var flags uint32
	if cfg.SQPOLL {
		flags |= giouring.SetupSQPoll
	}

	ring := giouring.NewRing()
	if err := ring.QueueInit(depth, flags); err != nil {
		return nil, fmt.Errorf("iosched: io_uring_setup: %w", err)
	}

	s := &URingScheduler{
		ring:        ring,
		depth:       depth,
		maxInflight: cfg.MaxInflightOps,
		signal:      make(chan *Ticket, 1),
	}
	s.shutdown.stop = make(chan struct{})

	s.wg.Add(1)
	go s.loop()
	return s, nil
}

// Submit enqueues t.Ops for asynchronous execution. The caller MUST call
// t.Wait before reading any [Op.Result] or releasing the Ticket.
func (s *URingScheduler) Submit(t *Ticket) error {
	if err := s.fatalError(); err != nil {
		return err
	}
	if t == nil || len(t.Ops) == 0 {
		return errors.New("iosched: nil ticket or empty ops")
	}

	n := int64(len(t.Ops))
	if s.maxInflight > 0 {
		if s.inflightOps.Add(n) > s.maxInflight {
			s.inflightOps.Add(-n)
			return ErrTooBusy
		}
	} else {
		s.inflightOps.Add(n)
	}

	t.pending = int32(len(t.Ops))
	t.nextOp = 0
	t.wg.Add(1) // released by coordinator.complete when pending hits 0

	s.enqueue(t)
	return nil
}

// Close shuts down the coordinator goroutine and releases ring resources.
func (s *URingScheduler) Close() error {
	s.stop(nil)
	s.wg.Wait()
	return nil
}

func (s *URingScheduler) enqueue(t *Ticket) {
	// Fast path: deliver directly to coordinator without locking.
	select {
	case s.signal <- t:
		return
	default:
	}
	// Slow path: channel full — append under mutex.
	s.overflow.Lock()
	s.overflow.queue = append(s.overflow.queue, t)
	s.overflow.Unlock()
}

// stop shuts the scheduler down. If err is nil the scheduler reports
// errSchedulerClosed for subsequent Submits; otherwise err is recorded as
// the fatal error. Idempotent — first call wins.
//
// The write to shutdown.err inside shutdown.Once.Do happens-before
// close(shutdown.stop), which happens-before any receive from shutdown.stop,
// so readers that observe shutdown.stop closed (see [fatalError]) safely see
// the latest shutdown.err value.
func (s *URingScheduler) stop(err error) {
	s.shutdown.Do(func() {
		if err == nil {
			err = errSchedulerClosed
		}
		s.shutdown.err = err
		close(s.shutdown.stop)
	})
}

// fatalError returns the recorded shutdown / ring error, or nil if the
// scheduler is still running. Reads shutdown.err only after observing
// shutdown.stop closed; otherwise the field may be uninitialized.
func (s *URingScheduler) fatalError() error {
	select {
	case <-s.shutdown.stop:
		return s.shutdown.err
	default:
		return nil
	}
}

// ── Coordinator ───────────────────────────────────────────────────────────────

// coordinator encapsulates the io_uring sliding-window event loop.
// All fields are owned exclusively by the coordinator goroutine.
type coordinator struct {
	sched *URingScheduler
	ring  *giouring.Ring

	// inflight maps ring slot → in-flight entry (valid=false means free).
	inflight  []inflightEntry
	freeSlots []int
	nInflight int

	// batch holds Tickets collected from the pending queue whose ops are
	// not yet fully placed into the ring.
	batch []*Ticket
}

func (s *URingScheduler) loop() {
	defer s.wg.Done()
	defer s.ring.QueueExit()

	c := coordinator{
		sched:     s,
		ring:      s.ring,
		inflight:  make([]inflightEntry, s.depth),
		freeSlots: make([]int, s.depth),
		batch:     make([]*Ticket, 0, s.depth),
	}
	for i := range s.depth {
		c.freeSlots[i] = int(i)
	}
	defer c.drainAll()

	for {
		if !c.collect() {
			return
		}
		before := c.nInflight
		c.fillSlots()
		if c.nInflight == 0 {
			continue
		}
		// Stats: only count syscalls that actually placed new SQEs.
		// Reap-only cycles (waiting on prior-cycle CQEs with no new work)
		// would otherwise skew the avg-batch-size ratio.
		if placed := c.nInflight - before; placed > 0 {
			s.stats.syscalls.Add(1)
			s.stats.opsPlaced.Add(uint64(placed))
		}
		if err := c.submit(); err != nil {
			c.failAllInflight(err)
			s.stop(fmt.Errorf("iosched: ring error: %w", err))
			return
		}
		c.reap()
	}
}

// collect drains newly-submitted Tickets from the signal channel and
// overflow queue. Blocks when idle; non-blocking when work is in-flight.
// Returns false on shutdown.
func (c *coordinator) collect() bool {
	prevLen := len(c.batch)

	if c.nInflight == 0 && prevLen == 0 {
		// Idle: block until new work or shutdown.
		select {
		case t := <-c.sched.signal:
			c.batch = append(c.batch, t)
		case <-c.sched.shutdown.stop:
			return false
		}
	} else {
		// Busy: non-blocking check for more work.
		select {
		case t := <-c.sched.signal:
			c.batch = append(c.batch, t)
		case <-c.sched.shutdown.stop:
			return false
		default:
		}
	}

	// Drain overflow queue (Tickets that lost the fast-path race).
	c.sched.overflow.Lock()
	c.batch = append(c.batch, c.sched.overflow.queue...)
	c.sched.overflow.queue = c.sched.overflow.queue[:0]
	c.sched.overflow.Unlock()

	return true
}

// fillSlots places ops from the batch into free SQ slots.
// Linked chains are placed atomically: if a chain won't fit, it stays at the
// front of the batch. Unlinked ops fill one slot each and may be partial.
func (c *coordinator) fillSlots() {
	for len(c.batch) > 0 {
		t := c.batch[0]
		placed := c.fillSlotsFromTicket(t)
		if placed == 0 {
			break // not enough slots even for the next chain
		}
		if t.nextOp >= uint16(len(t.Ops)) {
			// All ops from this ticket are in the ring; drop from batch.
			c.batch = c.batch[1:]
		}
	}
}

// fillSlotsFromTicket places as many ops as possible from t into free SQ
// slots. Returns the number of ops placed. Linked chains are placed
// atomically; advances t.nextOp as it goes.
func (c *coordinator) fillSlotsFromTicket(t *Ticket) int {
	placed := 0
	for t.nextOp < uint16(len(t.Ops)) {
		// Find the end of the current linked chain starting at t.nextOp.
		chainEnd := t.nextOp
		for chainEnd < uint16(len(t.Ops))-1 && t.Ops[chainEnd].isLinked() {
			chainEnd++
		}
		chainLen := int(chainEnd-t.nextOp) + 1

		if len(c.freeSlots) < chainLen {
			break // not enough room for this chain
		}

		for i := t.nextOp; i <= chainEnd; i++ {
			op := t.Ops[i]
			slot := c.freeSlots[len(c.freeSlots)-1]
			c.freeSlots = c.freeSlots[:len(c.freeSlots)-1]

			sqe := c.ring.GetSQE()
			// We just popped a free slot, so the SQ must have matching room.
			// If GetSQE returns nil, inflight+free no longer sums to depth —
			// loud in tests; in production, wrap the assertion error with
			// context, fail the current op, mark fatal, and shut down so
			// subsequent Submits return rather than hang.
			if err := buildutil.Assert(sqe != nil); err != nil {
				err = fmt.Errorf(
					"iosched: GetSQE returned nil; slot=%d nInflight=%d freeSlots=%d depth=%d: %w",
					slot, c.nInflight, len(c.freeSlots)+1, c.sched.depth, err,
				)
				c.freeSlots = append(c.freeSlots, slot)
				t.Ops[i].result.Err = err
				t.nextOp = i + 1
				placed++
				c.complete(t, 1)
				c.sched.stop(err)
				return placed
			}

			prepareSQE(sqe, op)
			sqe.SetData64(uint64(slot))

			c.inflight[slot] = inflightEntry{
				t:     t,
				opIdx: i,
				valid: true,
			}
			c.nInflight++
			placed++
		}
		t.nextOp = chainEnd + 1
	}
	return placed
}

// prepareSQE configures sqe for op.
// op.f is alive for the duration of this call (held by the caller's Submission).
func prepareSQE(sqe *giouring.SubmissionQueueEntry, op Op) {
	fd := int(op.f.Fd())
	switch op.opcode {
	case OpRead:
		sqe.PrepareRead(fd, uintptr(unsafe.Pointer(&op.buf[0])), uint32(len(op.buf)), uint64(op.offset))
	case OpWrite:
		sqe.PrepareWrite(fd, uintptr(unsafe.Pointer(&op.buf[0])), uint32(len(op.buf)), uint64(op.offset))
	case OpReadFixed:
		sqe.PrepareReadFixed(fd, uintptr(unsafe.Pointer(&op.buf[0])), uint32(len(op.buf)), uint64(op.offset), 0)
	case OpWriteFixed:
		sqe.PrepareWriteFixed(fd, uintptr(unsafe.Pointer(&op.buf[0])), uint32(len(op.buf)), uint64(op.offset), 0)
	case OpFsync:
		sqe.PrepareFsync(fd, 0)
	case OpFdatasync:
		sqe.PrepareFsync(fd, giouring.FsyncDatasync)
	case OpFallocate:
		sqe.PrepareFallocate(fd, int(op.opFlags), uint64(op.offset), uint64(op.length))
	default:
		panic(fmt.Sprintf("iosched: invalid opcode %d", op.opcode))
	}
	// Propagate SQE linking flags.
	if op.sqeFlags&sqeLink != 0 {
		sqe.SetFlags(uint32(giouring.SqeIOLink))
	} else if op.sqeFlags&sqeHardLink != 0 {
		sqe.SetFlags(uint32(giouring.SqeIOHardlink))
	}
}

// releaseSlot returns slot to the free pool and clears its inflight entry.
func (c *coordinator) releaseSlot(slot int) {
	c.inflight[slot].valid = false
	c.freeSlots = append(c.freeSlots, slot)
	c.nInflight--
}

// complete acks n ops on t: decrements t.pending, decrements the scheduler's
// global inflightOps counter, and — when t.pending reaches zero — calls
// t.wg.Done to release the caller's Wait.
func (c *coordinator) complete(t *Ticket, n int32) {
	t.pending -= n
	c.sched.inflightOps.Add(-int64(n))
	if t.pending == 0 {
		t.wg.Done()
	}
}

// submit submits all prepared SQEs and blocks for ≥1 completion when ops are
// in flight. Retries on EINTR. On non-recoverable error, the caller is
// responsible for failing in-flight ops (see failAllInflight).
func (c *coordinator) submit() error {
	waitNr := uint32(1)
	if c.nInflight == 0 {
		waitNr = 0
	}
	for {
		_, err := c.ring.SubmitAndWait(waitNr)
		if err == nil {
			return nil
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return err
	}
}

// reap drains every completed CQE and acks its owning Ticket.
func (c *coordinator) reap() {
	var count uint32
	c.ring.ForEachCQE(func(cqe *giouring.CompletionQueueEvent) {
		count++
		slot := int(cqe.GetData64())
		e := c.inflight[slot]
		c.releaseSlot(slot)

		op := &e.t.Ops[e.opIdx]
		if cqe.Res < 0 {
			op.result.Err = syscall.Errno(-cqe.Res)
		} else {
			op.result.N = int(cqe.Res)
		}
		c.complete(e.t, 1)
	})
	if count > 0 {
		c.ring.CQAdvance(count)
	}
}

// failAllInflight fails every valid inflight entry with the given ring error.
// Called when submit returns a non-recoverable error.
func (c *coordinator) failAllInflight(err error) {
	ringErr := fmt.Errorf("iosched: ring error: %w", err)
	for i, e := range c.inflight {
		if !e.valid {
			continue
		}
		op := &e.t.Ops[e.opIdx]
		op.result.Err = ringErr
		c.releaseSlot(i)
		c.complete(e.t, 1)
	}
}

// drainAll fails all in-flight, batched, signal-queued, and pending ops on
// shutdown. complete() handles per-op decrement of pending and inflightOps;
// when pending reaches zero for a Ticket it calls wg.Done to release the
// caller's Wait. A Ticket with both placed (in c.inflight) AND unplaced
// (in c.batch) ops is handled correctly across phases 1 and 2 — pending
// hits zero across the two passes, and complete fires once.
func (c *coordinator) drainAll() {
	// 1. In-flight ops: kernel may or may not finish them.
	for i, e := range c.inflight {
		if !e.valid {
			continue
		}
		op := &e.t.Ops[e.opIdx]
		if op.result.Err == nil {
			op.result.Err = errSchedulerClosed
		}
		c.releaseSlot(i)
		c.complete(e.t, 1)
	}

	// 2. Batched Tickets — ops at indices >= t.nextOp are unplaced.
	for _, t := range c.batch {
		unplaced := int32(len(t.Ops) - int(t.nextOp))
		if unplaced > 0 {
			failUnplaced(t)
			c.complete(t, unplaced)
		}
	}
	c.batch = c.batch[:0]

	// 3. signal channel: at most one Ticket, all ops unplaced.
	select {
	case t := <-c.sched.signal:
		n := int32(len(t.Ops))
		failUnplaced(t)
		c.complete(t, n)
	default:
	}

	// 4. pending overflow queue.
	c.sched.overflow.Lock()
	overflow := c.sched.overflow.queue
	c.sched.overflow.queue = c.sched.overflow.queue[:0]
	c.sched.overflow.Unlock()
	for _, t := range overflow {
		n := int32(len(t.Ops))
		failUnplaced(t)
		c.complete(t, n)
	}
}

// failUnplaced marks every op at or beyond t.nextOp as errSchedulerClosed
// (only if it doesn't already carry an error). Ops at indices < t.nextOp
// were placed and are tracked separately via c.inflight.
func failUnplaced(t *Ticket) {
	for i := int(t.nextOp); i < len(t.Ops); i++ {
		if t.Ops[i].result.Err == nil {
			t.Ops[i].result.Err = errSchedulerClosed
		}
	}
}
