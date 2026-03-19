//go:build linux

package iosched

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/miretskiy/dio/giouring"
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

// ── Request types ─────────────────────────────────────────────────────────────

// submissionMsg carries ops from Submit to the coordinator.
// Pooled via sync.Pool to avoid per-call allocation.
type submissionMsg struct {
	ops      []Op   // borrowed from caller; valid until coordinator calls fillSlots
	groupIdx uint16 // index into URingScheduler.groups
	gen      uint32 // snapshot of group.gen at Submit time
	nextOp   uint16 // index of first op not yet placed into the ring
}

// uringGroup is a pre-allocated completion cell for one Submit call.
//
// Concurrency model:
//   - done (buffered chan struct{}) carries exactly one signal: coordinator→Wait.
//   - gen and consumed together form the double-Wait guard (see claimWait).
//   - pending is written only by the coordinator goroutine (ForEachCQE,
//     drainPending). No synchronisation needed: single writer.
//   - ops is set by Submit and cleared by Wait. The done channel receive
//     provides the happens-before between coordinator writing results and
//     Wait reading them.
type uringGroup struct {
	done     chan struct{} // buffered(1); allocated once at init, never freed
	gen      uint32       // incremented on recycle; encoded into Ticket
	consumed atomic.Uint32 // CAS: 0=claimable, 1=claimed; prevents double-Wait
	pending  int32        // coordinator-only: ops not yet completed
	nOps     uint16       // total ops in this submission
	ops      []Op         // retained to keep caller buffers alive until Wait
	resultStore
}

// claimWait validates the ticket generation and atomically claims the Wait.
// Returns ErrTicketConsumed if the ticket is stale or already claimed.
func (g *uringGroup) claimWait(ticketGen uint32) error {
	if g.gen != ticketGen {
		return ErrTicketConsumed
	}
	if !g.consumed.CompareAndSwap(0, 1) {
		return ErrTicketConsumed
	}
	return nil
}

// inflightEntry maps a ring slot to the group and op that owns it.
type inflightEntry struct {
	groupIdx uint16
	opIdx    uint16
	valid    bool
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
	// groups is the pre-allocated completion slab.
	// One slot per potentially concurrent Submit call.
	groups    []uringGroup
	groupSlab indexSlab

	mu      sync.Mutex
	pending []*submissionMsg

	// signal carries submissionMsgs to the coordinator.
	// Fast path: single non-blocking send; slow path: mutex+pending+no signal needed.
	signal  chan *submissionMsg // buffered(1)
	msgPool sync.Pool

	stopCh   chan struct{}
	stopOnce sync.Once

	// fatalErr stores the first non-recoverable ring error.
	fatalErr atomic.Pointer[error]

	wg sync.WaitGroup

	// ring and depth are set in NewURingScheduler before the loop goroutine
	// starts, so they are safe to read from any goroutine without locks.
	ring  *giouring.Ring
	depth uint32
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
		ring:      ring,
		depth:     depth,
		groups:    make([]uringGroup, depth),
		groupSlab: newIndexSlab(int(depth)),
		signal:    make(chan *submissionMsg, 1),
		stopCh:    make(chan struct{}),
	}
	for i := range s.groups {
		s.groups[i].done = make(chan struct{}, 1)
	}
	s.msgPool.New = func() any { return &submissionMsg{} }

	s.wg.Add(1)
	go s.loop()
	return s, nil
}

// Submit enqueues ops and returns a Ticket. The caller must call Wait to
// collect results. Buffers in ops must remain valid until Wait returns.
func (s *URingScheduler) Submit(ops []Op) (Ticket, error) {
	if err := s.fatalError(); err != nil {
		return 0, err
	}
	if len(ops) == 0 {
		return 0, errors.New("iosched: Submit called with empty op slice")
	}

	groupIdx, err := s.groupSlab.acquire(s.stopCh)
	if err != nil {
		return 0, err
	}
	g := &s.groups[groupIdx]
	ticket := makeTicket(groupIdx, g.gen)

	g.nOps = uint16(len(ops))
	g.pending = int32(len(ops))
	g.ops = ops
	g.consumed.Store(0)
	g.resultStore.init(len(ops))

	// Drain any stale signal on the done channel (guard against prior cycle).
	select {
	case <-g.done:
	default:
	}

	msg := s.msgPool.Get().(*submissionMsg)
	msg.ops = ops
	msg.groupIdx = groupIdx
	msg.gen = g.gen
	msg.nextOp = 0

	s.enqueue(msg)
	return ticket, nil
}

// Wait blocks until all ops for the ticket complete, then copies results into
// the provided slice. Each Ticket must be waited exactly once.
func (s *URingScheduler) Wait(ticket Ticket, results []Result) (int, error) {
	groupIdx := ticket.groupIdx()
	g := &s.groups[groupIdx]

	if err := g.claimWait(ticket.gen()); err != nil {
		return 0, err
	}

	select {
	case <-g.done:
	case <-s.stopCh:
		select {
		case <-g.done:
		default:
			return 0, errSchedulerClosed
		}
	}

	n := g.copyTo(results)
	runtime.KeepAlive(g.ops)

	g.ops = nil
	g.gen++
	s.groupSlab.release(groupIdx)
	return n, nil
}

// Close shuts down the coordinator goroutine and releases ring resources.
func (s *URingScheduler) Close() error {
	s.stop()
	s.wg.Wait()
	return nil
}

func (s *URingScheduler) enqueue(msg *submissionMsg) {
	// Fast path: deliver directly to coordinator without locking.
	select {
	case s.signal <- msg:
		return
	default:
	}
	// Slow path: channel full — append under mutex.
	s.mu.Lock()
	s.pending = append(s.pending, msg)
	s.mu.Unlock()
}

func (s *URingScheduler) stop() {
	s.stopOnce.Do(func() {
		s.setFatalErr(errSchedulerClosed)
		close(s.stopCh)
	})
}

func (s *URingScheduler) fatalError() error {
	if p := s.fatalErr.Load(); p != nil {
		return *p
	}
	return nil
}

func (s *URingScheduler) setFatalErr(err error) {
	s.fatalErr.CompareAndSwap(nil, &err)
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

	// batch holds messages collected from the pending queue, not yet fully submitted.
	batch      []*submissionMsg
	nSubmitted int // SQEs prepared since last SubmitAndWait
}

func (s *URingScheduler) loop() {
	defer s.wg.Done()
	defer s.ring.QueueExit()

	c := coordinator{
		sched:     s,
		ring:      s.ring,
		inflight:  make([]inflightEntry, s.depth),
		freeSlots: make([]int, s.depth),
		batch:     make([]*submissionMsg, 0, s.depth),
	}
	for i := range s.depth {
		c.freeSlots[i] = int(i)
	}
	defer c.drainAll()

	for {
		if !c.collect() {
			return
		}
		c.fillSlots()

		if c.nSubmitted == 0 && c.nInflight == 0 {
			continue // spurious wakeup
		}

		if err := c.submitAndReap(); err != nil {
			s.setFatalErr(fmt.Errorf("iosched: ring error: %w", err))
			s.stop()
			return
		}
	}
}

// collect drains new messages from the signal channel and pending queue.
// Blocks when idle; non-blocking when work is in-flight.
// Returns false on shutdown.
func (c *coordinator) collect() bool {
	prevLen := len(c.batch)

	if c.nInflight == 0 && prevLen == 0 {
		// Idle: block until new work or shutdown.
		select {
		case msg := <-c.sched.signal:
			c.batch = append(c.batch, msg)
		case <-c.sched.stopCh:
			return false
		}
	} else {
		// Busy: non-blocking check for more work.
		select {
		case msg := <-c.sched.signal:
			c.batch = append(c.batch, msg)
		case <-c.sched.stopCh:
			return false
		default:
		}
	}

	// Drain overflow queue (msgs that lost the fast-path race).
	c.sched.mu.Lock()
	c.batch = append(c.batch, c.sched.pending...)
	c.sched.pending = c.sched.pending[:0]
	c.sched.mu.Unlock()

	return true
}

// fillSlots places ops from the batch into free SQ slots.
// Linked chains are placed atomically: if a chain won't fit, it stays at the
// front of the batch. Unlinked ops fill one slot each and may be partial.
func (c *coordinator) fillSlots() {
	for len(c.batch) > 0 {
		msg := c.batch[0]
		placed := c.fillMsg(msg)
		if placed == 0 {
			break // not enough slots even for the next chain
		}
		if msg.nextOp >= uint16(len(msg.ops)) {
			// All ops from this msg are in the ring; return msg to pool.
			c.batch = c.batch[1:]
			c.sched.msgPool.Put(msg)
		}
	}
}

// fillMsg places as many ops as possible from msg into free SQ slots.
// Returns the number of ops placed. Linked chains are placed atomically.
func (c *coordinator) fillMsg(msg *submissionMsg) int {
	placed := 0
	for msg.nextOp < uint16(len(msg.ops)) {
		// Find the end of the current linked chain starting at msg.nextOp.
		chainEnd := msg.nextOp
		for chainEnd < uint16(len(msg.ops))-1 && msg.ops[chainEnd].isLinked() {
			chainEnd++
		}
		chainLen := int(chainEnd-msg.nextOp) + 1

		if len(c.freeSlots) < chainLen {
			break // not enough room for this chain
		}

		for i := msg.nextOp; i <= chainEnd; i++ {
			op := msg.ops[i]
			slot := c.freeSlots[len(c.freeSlots)-1]
			c.freeSlots = c.freeSlots[:len(c.freeSlots)-1]

			sqe := c.ring.GetSQE()
			if sqe == nil {
				// Ring unexpectedly full; fail this op.
				g := &c.sched.groups[msg.groupIdx]
				g.at(int(i)).Err = errors.New("iosched: SQ ring full")
				g.pending--
				if g.pending == 0 {
					g.done <- struct{}{}
				}
				c.freeSlots = append(c.freeSlots, slot)
				msg.nextOp = i + 1
				placed++
				continue
			}

			prepareSQE(sqe, op)
			sqe.SetData64(uint64(slot))

			c.inflight[slot] = inflightEntry{
				groupIdx: msg.groupIdx,
				opIdx:    i,
				valid:    true,
			}
			c.nInflight++
			c.nSubmitted++
			placed++
		}
		msg.nextOp = chainEnd + 1
	}
	return placed
}

// prepareSQE configures sqe for op.
// op.f is alive for the duration of this call (held by the caller's group.ops).
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

// submitAndReap submits pending SQEs, waits for ≥1 completion, and reaps CQEs.
func (c *coordinator) submitAndReap() error {
	for {
		waitNr := uint32(1)
		if c.nInflight == 0 {
			waitNr = 0
		}

		_, err := c.ring.SubmitAndWait(waitNr)
		c.nSubmitted = 0

		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			// Fatal: fail all in-flight ops.
			for i, e := range c.inflight {
				if e.valid {
					g := &c.sched.groups[e.groupIdx]
					g.at(int(e.opIdx)).Err = fmt.Errorf("iosched: ring error: %w", err)
					g.pending--
					if g.pending == 0 {
						g.done <- struct{}{}
					}
					c.inflight[i].valid = false
					c.freeSlots = append(c.freeSlots, i)
					c.nInflight--
				}
			}
			return err
		}

		var count uint32
		c.ring.ForEachCQE(func(cqe *giouring.CompletionQueueEvent) {
			count++
			slot := int(cqe.GetData64())
			e := c.inflight[slot]
			c.inflight[slot].valid = false
			c.freeSlots = append(c.freeSlots, slot)
			c.nInflight--

			g := &c.sched.groups[e.groupIdx]
			res := g.at(int(e.opIdx))
			if cqe.Res < 0 {
				res.Err = syscall.Errno(-cqe.Res)
			} else {
				res.N = int(cqe.Res)
			}
			g.pending--
			if g.pending == 0 {
				g.done <- struct{}{}
			}
		})
		if count > 0 {
			c.ring.CQAdvance(count)
		}
		return nil
	}
}

// drainAll fails all in-flight, batched, signal-queued, and pending requests
// on shutdown.
func (c *coordinator) drainAll() {
	// Return all unprocessed msgs to pool first.
	for _, msg := range c.batch {
		c.sched.msgPool.Put(msg)
	}
	c.batch = c.batch[:0]

	select {
	case msg := <-c.sched.signal:
		c.sched.msgPool.Put(msg)
	default:
	}

	c.sched.mu.Lock()
	for _, msg := range c.sched.pending {
		c.sched.msgPool.Put(msg)
	}
	c.sched.pending = c.sched.pending[:0]
	c.sched.mu.Unlock()

	// Fail all groups with outstanding pending ops.
	// This covers: (a) in-flight SQEs, (b) msgs whose group was set up
	// but ops never reached the ring.
	for i := range c.sched.groups {
		g := &c.sched.groups[i]
		if g.pending > 0 {
			for j := 0; j < int(g.nOps); j++ {
				g.at(j).Err = errSchedulerClosed
			}
			g.pending = 0
			select {
			case g.done <- struct{}{}:
			default:
			}
		}
	}
}
