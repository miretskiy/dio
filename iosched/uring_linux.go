//go:build linux

package iosched

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/miretskiy/dio/giouring"
	"github.com/miretskiy/dio/mempool"
	"golang.org/x/sys/unix"
)

const defaultRingDepth = 256

// Compile-time assertion that op.go's mirrored SQE flag bits match the
// kernel values giouring exposes (a mismatch makes the index non-zero).
var (
	_ = [1]struct{}{}[giouring.SqeIOLink-sqeLink]
	_ = [1]struct{}{}[giouring.SqeIOHardlink-sqeHardlink]
)

// URingScheduler is the io_uring-backed [Scheduler].
//
// # Design
//
// The scheduler owns no goroutines and no timers; the only resources beyond
// the ring itself are a fixed op-slot table and a few reusable scratch
// buffers. All kernel work is done by the submitting goroutines:
//
//   - Every Submit places its ops into the SQ (under a short mutex hold) and
//     publishes them with one io_uring_enter. Independent Submit calls that
//     race on the same flush window are picked up by whichever enter runs
//     first, so concurrent submissions share enter cycles.
//   - Exactly one submitter at a time is the leader: it parks in
//     io_uring_enter(GETEVENTS, waitNr=1) so every completion is reaped the
//     moment the kernel posts it, then completes ops, wakes finished waiters,
//     and refills the SQ from the overflow queue. When the leader's own ops
//     finish while others remain in flight, it hands leadership to a waiting
//     submitter.
//
// waitNr=1 keeps completion latency minimal: the leader wakes per CQE batch
// available, not after an arbitrary count.
//
// In-kernel concurrency is bounded by the CQ size (2 × RingDepth) via the
// slot table, so the completion queue can never overflow. Ops that do not
// fit wait in an internal FIFO and enter the ring as completions free slots.
type URingScheduler struct {
	ring *giouring.Ring

	// budget is the remaining in-flight op allowance; used only when
	// maxInflight > 0.
	budget      atomic.Int64
	maxInflight int64
	maxChain    int
	sqPoll      bool

	mu        sync.Mutex
	closed    bool
	hasLeader bool
	freeSlots []uint32
	overflow  opDeque    // accepted ops not yet placed in the SQ (FIFO)
	waiters   waiterList // groups parked in Submit, in arrival order

	// slots maps SQE user_data (a slot index) to the in-flight op. Entries
	// are written under mu by submitters and read lock-free by the leader;
	// atomic.Pointer provides the cross-goroutine ordering.
	slots []atomic.Pointer[Op]

	// Leader-only scratch (at most one leader exists at any time).
	cqes       []*giouring.CompletionQueueEvent
	reaped     []uint32
	doneGroups []*submitGroup

	pool *mempool.SlabPool // registered DMA slab, if any
}

var _ Scheduler = (*URingScheduler)(nil)

// NewURingScheduler creates an io_uring scheduler. The returned scheduler
// must be closed with Close, which blocks until all in-flight ops drain.
func NewURingScheduler(cfg URingConfig) (*URingScheduler, error) {
	depth := cfg.RingDepth
	if depth == 0 {
		depth = defaultRingDepth
	}
	if depth&(depth-1) != 0 {
		return nil, fmt.Errorf("iosched: RingDepth %d is not a power of two", depth)
	}

	flags := giouring.SetupClamp
	if cfg.SQPOLL {
		flags |= giouring.SetupSQPoll
	}
	ring := giouring.NewRing()
	if err := ring.QueueInit(depth, flags); err != nil {
		return nil, fmt.Errorf("iosched: io_uring setup: %w", err)
	}

	// The kernel sizes the CQ at 2× the SQ entries; capping in-kernel ops at
	// the CQ size guarantees completions are never dropped to the (slow)
	// overflow path.
	numSlots := int(depth) * 2
	s := &URingScheduler{
		ring:        ring,
		maxInflight: cfg.MaxInflightOps,
		maxChain:    int(depth),
		sqPoll:      cfg.SQPOLL,
		freeSlots:   make([]uint32, numSlots),
		slots:       make([]atomic.Pointer[Op], numSlots),
		cqes:        make([]*giouring.CompletionQueueEvent, numSlots),
		reaped:      make([]uint32, 0, numSlots),
		doneGroups:  make([]*submitGroup, 0, numSlots),
	}
	for i := range s.freeSlots {
		s.freeSlots[i] = uint32(i)
	}
	return s, nil
}

// Submit implements [Scheduler].
func (s *URingScheduler) Submit(ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	if err := validateChains(ops, s.maxChain); err != nil {
		return err
	}
	nOps := int64(len(ops))
	if s.maxInflight > 0 {
		if s.budget.Add(nOps) > s.maxInflight {
			s.budget.Add(-nOps)
			return ErrTooBusy
		}
	}

	g := acquireGroup()
	g.remaining.Store(int32(len(ops)))
	for i := range ops {
		ops[i].g = g
		ops[i].res = 0
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		if s.maxInflight > 0 {
			s.budget.Add(-nOps)
		}
		releaseGroup(g)
		return ErrClosed
	}
	s.enqueueLocked(ops)
	toSubmit := s.flushLocked()
	leader := !s.hasLeader
	pushed := false
	if leader {
		s.hasLeader = true
	} else if g.remaining.Load() > 0 {
		s.waiters.push(g)
		pushed = true
	}
	s.mu.Unlock()

	if leader {
		s.lead(g, toSubmit)
	} else {
		if toSubmit > 0 && g.remaining.Load() > 0 {
			s.enterSubmit(toSubmit)
		}
		s.await(g, pushed)
	}
	releaseGroup(g)
	return nil
}

// lead is the drain-leader loop: park in the kernel until completions arrive,
// reap them, complete ops and wake their submitters, refill the SQ from the
// overflow queue, and repeat until g's own ops are done. On exit, leadership
// is handed to a waiting submitter if work remains. Called without mu held;
// only one goroutine leads at a time.
func (s *URingScheduler) lead(g *submitGroup, toSubmit uint32) {
	for {
		s.enterWait(toSubmit)
		toSubmit = 0

		n := int(s.ring.PeekBatchCQE(s.cqes))
		if n == 0 {
			continue
		}
		for _, cqe := range s.cqes[:n] {
			idx := uint32(cqe.GetData64())
			op := s.slots[idx].Load()
			op.res = cqe.Res
			s.reaped = append(s.reaped, idx)
			if grp := op.g; grp.remaining.Add(-1) == 0 && grp != g {
				s.doneGroups = append(s.doneGroups, grp)
			}
		}
		s.ring.CQAdvance(uint32(n))
		if s.maxInflight > 0 {
			s.budget.Add(-int64(n))
		}

		s.mu.Lock()
		s.freeSlots = append(s.freeSlots, s.reaped...)
		s.reaped = s.reaped[:0]
		// Unlink completed groups before signaling them (below, after
		// unlock): a signaled waiter may recycle its group immediately, so
		// it must already be off the list.
		for _, grp := range s.doneGroups {
			s.waiters.unlink(grp)
		}
		s.refillLocked()
		toSubmit = s.flushLocked()
		done := g.remaining.Load() == 0
		s.mu.Unlock()

		// We still hold leadership here, so the scratch buffers are
		// exclusively ours; they must be clean before leadership is
		// released below, when the next leader takes them over.
		for _, grp := range s.doneGroups {
			grp.signal()
		}
		s.doneGroups = s.doneGroups[:0]

		if !done {
			continue
		}

		s.mu.Lock()
		s.hasLeader = false
		var successor *submitGroup
		if len(s.freeSlots) < len(s.slots) || s.overflow.len() > 0 {
			// Work remains; wake a waiter to take over leadership. The
			// list holds only incomplete groups, and nothing can complete
			// them until a new leader reaps.
			successor = s.waiters.first()
		}
		s.mu.Unlock()

		if toSubmit > 0 {
			s.enterSubmit(toSubmit)
		}
		if successor != nil {
			successor.signal()
		}
		return
	}
}

// await parks until g's ops complete, taking over ring leadership if it is
// handed to us. pushed reports whether g was added to the wait list.
func (s *URingScheduler) await(g *submitGroup, pushed bool) {
	for {
		if g.remaining.Load() == 0 {
			break
		}
		<-g.sema // done- or leadership-signal; recheck either way
		if g.remaining.Load() == 0 {
			break
		}
		s.mu.Lock()
		// Recheck under mu: a leader may have completed our ops and exited
		// between the check above and the Lock. Taking leadership with no
		// remaining ops would park forever on a possibly idle ring. If the
		// lock-holder that completed us has released leadership, its
		// decrements are visible here (mutex ordering), so the check is
		// authoritative.
		if g.remaining.Load() == 0 {
			s.mu.Unlock()
			break
		}
		if !s.hasLeader {
			s.hasLeader = true
			s.waiters.unlink(g)
			s.mu.Unlock()
			s.lead(g, 0)
			return
		}
		s.mu.Unlock()
	}
	if pushed {
		// The leader may have decremented our last op but not yet unlinked
		// us; g must be off the list before it is recycled.
		s.mu.Lock()
		s.waiters.unlink(g)
		s.mu.Unlock()
	}
}

// enqueueLocked places ops into the SQ, falling back to the overflow FIFO
// when slots or SQ space run out. Link chains are kept intact: a chain is
// placed contiguously or not at all (the kernel terminates a link chain at
// the end of a submission batch). Once anything has overflowed, subsequent
// ops queue behind it to preserve FIFO fairness.
func (s *URingScheduler) enqueueLocked(ops []Op) {
	for start := 0; start < len(ops); {
		end := chainEnd(ops, start)
		n := end - start
		if s.overflow.len() > 0 || len(s.freeSlots) < n || s.ring.SQSpaceLeft() < uint32(n) {
			for i := start; i < end; i++ {
				s.overflow.push(&ops[i])
			}
		} else {
			for i := start; i < end; i++ {
				s.placeLocked(&ops[i])
			}
		}
		start = end
	}
}

// refillLocked moves overflowed ops into the SQ while capacity allows,
// whole chains at a time.
func (s *URingScheduler) refillLocked() {
	for s.overflow.len() > 0 {
		n := s.overflow.chainLen()
		if len(s.freeSlots) < n || s.ring.SQSpaceLeft() < uint32(n) {
			return
		}
		for range n {
			s.placeLocked(s.overflow.pop())
		}
	}
}

// placeLocked writes one SQE for op. Caller guarantees a free slot and SQ
// space.
func (s *URingScheduler) placeLocked(op *Op) {
	last := len(s.freeSlots) - 1
	idx := s.freeSlots[last]
	s.freeSlots = s.freeSlots[:last]
	s.slots[idx].Store(op)

	sqe := s.ring.GetSQE()
	var addr uintptr
	if len(op.buf) > 0 {
		addr = uintptr(unsafe.Pointer(&op.buf[0]))
	}
	switch op.code {
	case opRead:
		sqe.PrepareRead(int(op.fd), addr, uint32(len(op.buf)), uint64(op.off))
	case opWrite:
		sqe.PrepareWrite(int(op.fd), addr, uint32(len(op.buf)), uint64(op.off))
	case opReadFixed:
		// The whole DMA slab is registered as buffer index 0.
		sqe.PrepareReadFixed(int(op.fd), addr, uint32(len(op.buf)), uint64(op.off), 0)
	case opWriteFixed:
		sqe.PrepareWriteFixed(int(op.fd), addr, uint32(len(op.buf)), uint64(op.off), 0)
	case opFsync:
		sqe.PrepareFsync(int(op.fd), 0)
	case opFdatasync:
		sqe.PrepareFsync(int(op.fd), giouring.FsyncDatasync)
	default:
		sqe.PrepareNop()
	}
	sqe.Flags = op.sqeFlags
	sqe.UserData = uint64(idx)
}

// flushLocked publishes prepared SQEs and returns the count to pass to
// io_uring_enter (outside mu). Under SQPOLL the kernel-side poller consumes
// SQEs itself; the occasional wakeup syscall is issued inline and 0 is
// returned.
func (s *URingScheduler) flushLocked() uint32 {
	if s.sqPoll {
		_, _ = s.ring.Submit() // flush + wake the SQ poller thread if idle
		return 0
	}
	return s.ring.FlushSQ()
}

// enterWait submits toSubmit pending SQEs (over-counting is harmless — the
// kernel consumes at most what is flushed) and blocks until at least one
// CQE is available.
func (s *URingScheduler) enterWait(toSubmit uint32) {
	for {
		if toSubmit == 0 && s.ring.CQReady() > 0 {
			return
		}
		_, err := s.ring.Enter(toSubmit, 1, giouring.EnterGetEvents, nil)
		switch err {
		case nil:
			return
		case unix.EINTR:
			continue
		case unix.EAGAIN, unix.EBUSY:
			runtime.Gosched()
		default:
			panic(fmt.Sprintf("iosched: io_uring_enter: %v", err))
		}
	}
}

// enterSubmit publishes toSubmit pending SQEs without waiting.
func (s *URingScheduler) enterSubmit(toSubmit uint32) {
	for {
		_, err := s.ring.Enter(toSubmit, 0, 0, nil)
		switch err {
		case nil:
			return
		case unix.EINTR:
			continue
		case unix.EAGAIN, unix.EBUSY:
			runtime.Gosched()
		default:
			panic(fmt.Sprintf("iosched: io_uring_enter: %v", err))
		}
	}
}

// usePool implements pooler: it registers pool's slab as fixed buffer 0 via
// io_uring_register_buffers. See [RegisterDMASlab].
func (s *URingScheduler) usePool(pool *mempool.SlabPool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.pool != nil {
		if _, err := s.ring.UnregisterBuffers(); err != nil {
			return fmt.Errorf("iosched: unregister buffers: %w", err)
		}
		s.pool = nil
	}
	raw := pool.RawData()
	iov := []syscall.Iovec{{Base: &raw[0], Len: uint64(len(raw))}}
	if _, err := s.ring.RegisterBuffers(iov); err != nil {
		return fmt.Errorf("iosched: register buffers: %w", err)
	}
	s.pool = pool
	return nil
}

// Close implements [io.Closer]. It rejects new submissions, waits for all
// in-flight ops to drain (their Submit callers reap them), then tears down
// the ring.
func (s *URingScheduler) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	for {
		s.mu.Lock()
		idle := !s.hasLeader && s.overflow.len() == 0 && len(s.freeSlots) == len(s.slots)
		s.mu.Unlock()
		if idle {
			break
		}
		time.Sleep(50 * time.Microsecond)
	}

	if s.pool != nil {
		_, _ = s.ring.UnregisterBuffers()
		s.pool = nil
	}
	s.ring.QueueExit()
	return nil
}

// groupPool recycles submitGroups so steady-state Submit calls allocate
// nothing.
var groupPool = sync.Pool{
	New: func() any { return &submitGroup{sema: make(chan struct{}, 1)} },
}

func acquireGroup() *submitGroup {
	return groupPool.Get().(*submitGroup)
}

func releaseGroup(g *submitGroup) {
	g.drain() // discard a stale token (e.g. an unconsumed handoff signal)
	groupPool.Put(g)
}

// waiterList is an intrusive doubly-linked FIFO of parked submission groups.
// All operations require the scheduler mutex.
type waiterList struct {
	head, tail *submitGroup
}

func (l *waiterList) push(g *submitGroup) {
	g.inList = true
	g.prev = l.tail
	g.next = nil
	if l.tail != nil {
		l.tail.next = g
	} else {
		l.head = g
	}
	l.tail = g
}

// unlink removes g if present; safe to call on an already-unlinked group.
func (l *waiterList) unlink(g *submitGroup) {
	if !g.inList {
		return
	}
	if g.prev != nil {
		g.prev.next = g.next
	} else {
		l.head = g.next
	}
	if g.next != nil {
		g.next.prev = g.prev
	} else {
		l.tail = g.prev
	}
	g.next, g.prev = nil, nil
	g.inList = false
}

func (l *waiterList) first() *submitGroup { return l.head }

// opDeque is a slice-backed FIFO of ops awaiting ring capacity. Capacity is
// reused across cycles; it grows only to the peak backlog.
type opDeque struct {
	ops  []*Op
	head int
}

func (q *opDeque) len() int { return len(q.ops) - q.head }

func (q *opDeque) push(op *Op) { q.ops = append(q.ops, op) }

func (q *opDeque) pop() *Op {
	op := q.ops[q.head]
	q.ops[q.head] = nil
	q.head++
	if q.head == len(q.ops) {
		q.ops = q.ops[:0]
		q.head = 0
	}
	return op
}

// chainLen returns the length of the link chain at the head of the queue.
// Chains are always enqueued whole, and validateChains guarantees the final
// op of every submission carries no link flag, so the scan terminates.
func (q *opDeque) chainLen() int {
	n := 1
	for i := q.head; q.ops[i].sqeFlags&sqeLinkMask != 0; i++ {
		n++
	}
	return n
}
