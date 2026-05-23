//go:build linux

package iosched

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
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

// ── Per-Submit request ────────────────────────────────────────────────────────

// request is the internal handle for one Submit call. It is allocated per
// Submit and stays referenced by the active batch until that batch completes.
type request struct {
	ops     []Op  // caller's slice; lives across Submit invocation
	pending int32 // unfinished op count
	nextOp  int   // next op index to place into the ring
	done    bool  // protected by URingScheduler.mu
	err     error // protected by URingScheduler.mu once done is published
	trace   *requestTrace
}

type requestTrace struct {
	sink               uringTraceSink
	start              time.Time
	completed          time.Time
	role               uringTraceRole
	queuedBehindLeader bool
	queueLatency       time.Duration
	batchSize          int
	batchCycles        int
}

type uringTraceRole uint8

const (
	uringTraceLeader uringTraceRole = iota + 1
	uringTraceFollower
)

type uringTraceEvent struct {
	role               uringTraceRole
	queuedBehindLeader bool
	submitLatency      time.Duration
	queueLatency       time.Duration
	completeToReturn   time.Duration
	batchSize          int
	batchCycles        int
}

type uringTraceSink interface {
	recordURingTrace(uringTraceEvent)
}

// inflightEntry maps a ring slot to the request and op that owns it.
type inflightEntry struct {
	r     *request
	opIdx int
	valid bool
}

// ── URingScheduler ────────────────────────────────────────────────────────────

// URingScheduler is a synchronous io_uring [Scheduler]. Submit blocks until
// completion. Concurrent Submit calls cooperate via a drain-leader pattern:
// one caller becomes the leader and drives io_uring_enter / CQE reaping for
// itself and any other callers currently in the pending queue; the others
// park on the cond and return when their own request is marked done. There
// is no dedicated coordinator goroutine.
//
// Group commit: the leader swaps the pending queue into a private batch,
// processes that batch to completion, then publishes every completion under
// the mutex. Callers that arrive while the leader is active wait for the next
// batch.
type URingScheduler struct {
	// inflightOps counts ops currently in flight when MaxInflightOps is
	// configured. The default unlimited path leaves this counter untouched.
	inflightOps atomic.Int64
	// maxInflight is the cached cap from URingConfig.MaxInflightOps.
	// 0 means unlimited.
	maxInflight int64

	// mu + cond + pending/flushing/leaderActive form the drain-leader state
	// machine. mu protects pending, flushing, leaderActive, closing.closed,
	// and closing.err. cond is signaled when a leader broadcasts at the end
	// of its run or when Close fires.
	mu           sync.Mutex
	cond         *sync.Cond
	pending      []*request
	flushing     []*request
	leaderActive bool

	// closing is the shutdown state. Once.Do serializes the first close.
	// closed is set under mu; readers check it under mu in the Submit loop.
	closing struct {
		sync.Once
		closed bool
		err    error
	}

	closeRing sync.Once

	// stats tracks coordinator-internal counters useful for diagnosing
	// batching behavior. Protected by mu.
	stats struct {
		// syscalls counts io_uring_enter calls that actually placed new
		// SQEs (i.e. cycles where fillSlots produced at least one SQE).
		// opsPlaced / syscalls = average batch size.
		syscalls  uint64
		opsPlaced uint64
	}

	trace uringTraceSink

	// Ring + ring-owned state. Accessed only by the active leader (the
	// leaderActive flag under mu serializes ownership). Producers that are
	// not the leader never touch any of these fields.
	ring      *giouring.Ring
	depth     uint32
	inflight  []inflightEntry
	freeSlots []int
	nInflight int
}

// Stats is a point-in-time snapshot of leader-thread counters.
type Stats struct {
	// Syscalls is the number of io_uring_enter calls that submitted new SQEs.
	Syscalls uint64
	// OpsPlaced is the total number of SQEs placed into the ring.
	// OpsPlaced / Syscalls gives the average batch size.
	OpsPlaced uint64
}

// Stats returns a snapshot of the leader counters.
func (s *URingScheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		Syscalls:  s.stats.syscalls,
		OpsPlaced: s.stats.opsPlaced,
	}
}

func (s *URingScheduler) tryReserveInflight(n int64) bool {
	if s.maxInflight == 0 {
		return true
	}
	if s.inflightOps.Add(n) > s.maxInflight {
		s.inflightOps.Add(-n)
		return false
	}
	return true
}

func (s *URingScheduler) releaseInflight(n int64) {
	if s.maxInflight > 0 {
		s.inflightOps.Add(-n)
	}
}

// usePool implements the unexported pooler interface defined in scheduler.go.
func (s *URingScheduler) usePool(pool *mempool.SlabPool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing.closed {
		return s.closing.err
	}
	if s.leaderActive || len(s.pending) > 0 || s.nInflight > 0 {
		return errors.New("iosched: cannot register DMA slab while scheduler is active")
	}

	data := pool.RawData()
	iovs := []syscall.Iovec{{Base: &data[0], Len: uint64(len(data))}}
	if _, err := s.ring.RegisterBuffers(iovs); err != nil {
		return fmt.Errorf("iosched: io_uring_register_buffers: %w", err)
	}
	return nil
}

// NewURingScheduler creates an io_uring-backed scheduler. No background
// goroutine is started; the ring is driven by whichever caller currently
// holds the leader role.
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
		pending:     make([]*request, 0, 128),
		flushing:    make([]*request, 0, 128),
		inflight:    make([]inflightEntry, depth),
		freeSlots:   make([]int, depth),
	}
	s.cond = sync.NewCond(&s.mu)
	for i := range s.depth {
		s.freeSlots[i] = int(i)
	}
	return s, nil
}

// Submit executes ops and blocks until every op has completed.
// See [Scheduler] for the full contract.
func (s *URingScheduler) Submit(ops []Op) error {
	if len(ops) == 0 {
		return errors.New("iosched: empty op slice")
	}

	n := int64(len(ops))
	if !s.tryReserveInflight(n) {
		return ErrTooBusy
	}

	r := &request{
		ops:     ops,
		pending: int32(len(ops)),
	}

	s.mu.Lock()
	if s.trace != nil {
		r.trace = &requestTrace{
			sink:               s.trace,
			start:              time.Now(),
			queuedBehindLeader: s.leaderActive,
		}
	}
	if s.closing.closed {
		// Scheduler is already shut down; refund the budget and bail.
		s.releaseInflight(n)
		err := s.closing.err
		s.mu.Unlock()
		return err
	}
	s.pending = append(s.pending, r)

	for {
		if r.done {
			err := r.err
			s.recordTraceReturnLocked(r)
			s.mu.Unlock()
			return err
		}
		if s.leaderActive {
			s.cond.Wait()
			continue
		}
		if s.closing.closed {
			s.failPendingLocked(s.closing.err)
			continue
		}

		// Become the leader. Swap the pending queue into a private batch,
		// process it to completion, then publish all completions under mu.
		s.leaderActive = true
		batch := s.pending
		s.pending = s.flushing[:0]
		s.flushing = nil
		s.recordTraceBatchStartLocked(batch, r)
		s.mu.Unlock()

		stats, err := s.runLeader(batch)

		s.mu.Lock()
		s.stats.syscalls += stats.syscalls
		s.stats.opsPlaced += stats.opsPlaced
		s.recordTraceBatchCyclesLocked(batch, stats.cycles)
		if err != nil {
			s.closeLocked(err)
		}
		for _, br := range batch {
			if br.done {
				continue
			}
			if br.err == nil {
				br.err = err
			}
			br.done = true
		}
		s.leaderActive = false
		wakeCompletedFollowers := len(batch) > 1
		wakeNextLeader := len(s.pending) > 0
		closing := s.closing.closed
		clear(batch)
		s.flushing = batch[:0]
		switch {
		case closing || wakeCompletedFollowers:
			s.cond.Broadcast()
		case wakeNextLeader:
			s.cond.Signal()
		}
		if r.done {
			err := r.err
			s.recordTraceReturnLocked(r)
			s.mu.Unlock()
			return err
		}
		// Should not happen: the leader's request was in the swapped batch.
		// Loop instead of hanging if the invariant is violated.
	}
}

// runLeader drives the io_uring loop on behalf of one swapped batch.
//
// Invariant on entry: leaderActive is true; no one else touches the ring
// state (inflight, freeSlots, nInflight). The batch contains at least one
// request not yet done.
func (s *URingScheduler) runLeader(batch []*request) (leaderStats, error) {
	var stats leaderStats
	for !allComplete(batch) {
		stats.cycles++
		before := s.nInflight
		if err := s.fillSlots(batch); err != nil {
			s.failInflight(err)
			s.failBatch(batch, err)
			return stats, err
		}

		// If we have nothing to wait on, there's a bug — break out to avoid
		// a hard hang. Should never happen if allComplete returned false.
		if s.nInflight == 0 {
			err := errors.New("iosched: leader stalled with no in-flight ops")
			s.failBatch(batch, err)
			return stats, err
		}

		if placed := s.nInflight - before; placed > 0 {
			stats.syscalls++
			stats.opsPlaced += uint64(placed)
		}

		if err := s.kernelSubmit(); err != nil {
			ringErr := fmt.Errorf("iosched: ring error: %w", err)
			s.failInflight(ringErr)
			s.failBatch(batch, ringErr)
			return stats, ringErr
		}
		s.reap()
	}
	return stats, nil
}

type leaderStats struct {
	syscalls  uint64
	opsPlaced uint64
	cycles    int
}

func allComplete(batch []*request) bool {
	for _, r := range batch {
		if r.pending > 0 {
			return false
		}
	}
	return true
}

// fillSlots places as many ops as possible from batch into free SQ slots.
// Skips requests already done. Linked chains are placed atomically: if a
// chain won't fit, we stop trying to place ops from that request this cycle.
func (s *URingScheduler) fillSlots(batch []*request) error {
	for _, r := range batch {
		if r.pending == 0 {
			continue
		}
		if err := s.fillSlotsFromRequest(r); err != nil {
			return err
		}
		if len(s.freeSlots) == 0 {
			return nil
		}
	}
	return nil
}

func (s *URingScheduler) fillSlotsFromRequest(r *request) error {
	for r.nextOp < len(r.ops) {
		// Find the end of the current linked chain.
		chainEnd := r.nextOp
		for chainEnd < len(r.ops)-1 && r.ops[chainEnd].isLinked() {
			chainEnd++
		}
		chainLen := chainEnd - r.nextOp + 1
		if chainLen > int(s.depth) {
			return fmt.Errorf("iosched: linked op chain length %d exceeds ring depth %d", chainLen, s.depth)
		}
		if len(s.freeSlots) < chainLen {
			return nil // not enough room for this chain
		}

		for i := r.nextOp; i <= chainEnd; i++ {
			op := r.ops[i]
			slot := s.freeSlots[len(s.freeSlots)-1]
			s.freeSlots = s.freeSlots[:len(s.freeSlots)-1]

			sqe := s.ring.GetSQE()
			if err := buildutil.Assert(sqe != nil); err != nil {
				err = fmt.Errorf(
					"iosched: GetSQE returned nil; slot=%d nInflight=%d freeSlots=%d depth=%d: %w",
					slot, s.nInflight, len(s.freeSlots)+1, s.depth, err,
				)
				s.freeSlots = append(s.freeSlots, slot)
				r.ops[i].result.Err = err
				r.nextOp = i + 1
				s.complete(r, 1)
				return err
			}

			prepareSQE(sqe, op)
			sqe.SetData64(uint64(slot))

			s.inflight[slot] = inflightEntry{
				r:     r,
				opIdx: i,
				valid: true,
			}
			s.nInflight++
		}
		r.nextOp = chainEnd + 1
	}
	return nil
}

// prepareSQE configures sqe for op.
// op.f is alive for the duration of this call (held by the caller's slice).
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
	if op.sqeFlags&sqeLink != 0 {
		sqe.SetFlags(uint32(giouring.SqeIOLink))
	} else if op.sqeFlags&sqeHardLink != 0 {
		sqe.SetFlags(uint32(giouring.SqeIOHardlink))
	}
}

// kernelSubmit calls io_uring_enter, submitting prepared SQEs and waiting for
// at least one completion. Retries on EINTR.
func (s *URingScheduler) kernelSubmit() error {
	waitNr := uint32(1)
	if s.nInflight == 0 {
		waitNr = 0
	}
	for {
		_, err := s.ring.SubmitAndWait(waitNr)
		if err == nil {
			return nil
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return err
	}
}

// reap drains every completed CQE and acks its owning request.
func (s *URingScheduler) reap() {
	var count uint32
	s.ring.ForEachCQE(func(cqe *giouring.CompletionQueueEvent) {
		count++
		slot := int(cqe.GetData64())
		e := s.inflight[slot]
		s.releaseSlot(slot)

		op := &e.r.ops[e.opIdx]
		if cqe.Res < 0 {
			op.result.Err = syscall.Errno(-cqe.Res)
		} else {
			op.result.N = int(cqe.Res)
		}
		s.complete(e.r, 1)
	})
	if count > 0 {
		s.ring.CQAdvance(count)
	}
}

func (s *URingScheduler) releaseSlot(slot int) {
	s.inflight[slot].valid = false
	s.freeSlots = append(s.freeSlots, slot)
	s.nInflight--
}

// complete acks n ops on r. The active leader publishes r.done only after the
// whole swapped batch has completed, under URingScheduler.mu.
func (s *URingScheduler) complete(r *request, n int32) {
	r.pending -= n
	if r.pending == 0 {
		s.recordTraceComplete(r)
	}
	s.releaseInflight(int64(n))
}

func (s *URingScheduler) recordTraceBatchStartLocked(batch []*request, leader *request) {
	if s.trace == nil {
		return
	}
	hasTrace := false
	for _, r := range batch {
		if r.trace != nil {
			hasTrace = true
			break
		}
	}
	if !hasTrace {
		return
	}
	now := time.Now()
	for _, r := range batch {
		if r.trace == nil {
			continue
		}
		r.trace.role = uringTraceFollower
		if r == leader {
			r.trace.role = uringTraceLeader
		}
		r.trace.queueLatency = now.Sub(r.trace.start)
		r.trace.batchSize = len(batch)
	}
}

func (s *URingScheduler) recordTraceBatchCyclesLocked(batch []*request, cycles int) {
	if s.trace == nil {
		return
	}
	for _, r := range batch {
		if r.trace != nil {
			r.trace.batchCycles = cycles
		}
	}
}

func (s *URingScheduler) recordTraceComplete(r *request) {
	if r.trace == nil || !r.trace.completed.IsZero() {
		return
	}
	r.trace.completed = time.Now()
}

func (s *URingScheduler) recordTraceReturnLocked(r *request) {
	if r.trace == nil {
		return
	}
	now := time.Now()
	completeToReturn := time.Duration(0)
	if !r.trace.completed.IsZero() {
		completeToReturn = now.Sub(r.trace.completed)
	}
	r.trace.sink.recordURingTrace(uringTraceEvent{
		role:               r.trace.role,
		queuedBehindLeader: r.trace.queuedBehindLeader,
		submitLatency:      now.Sub(r.trace.start),
		queueLatency:       r.trace.queueLatency,
		completeToReturn:   completeToReturn,
		batchSize:          r.trace.batchSize,
		batchCycles:        r.trace.batchCycles,
	})
}

// failInflight fails every valid inflight entry with err. Used when
// kernelSubmit returns a fatal error.
func (s *URingScheduler) failInflight(err error) {
	for i, e := range s.inflight {
		if !e.valid {
			continue
		}
		op := &e.r.ops[e.opIdx]
		if op.result.Err == nil {
			op.result.Err = err
		}
		s.releaseSlot(i)
		s.complete(e.r, 1)
	}
}

// failBatch fails any request in batch that isn't yet done, marking unplaced
// ops with err. Used during shutdown or when the leader detects a fatal
// condition.
func (s *URingScheduler) failBatch(batch []*request, err error) {
	for _, r := range batch {
		if r.pending == 0 {
			if r.err == nil {
				r.err = err
			}
			continue
		}
		// Fail any unplaced ops (those at indices >= r.nextOp; ops already
		// placed are tracked via inflight and handled by failInflight or
		// by ordinary reap).
		for i := int(r.nextOp); i < len(r.ops); i++ {
			if r.ops[i].result.Err == nil {
				r.ops[i].result.Err = err
			}
		}
		unplaced := int32(len(r.ops) - int(r.nextOp))
		if unplaced > 0 {
			r.nextOp = len(r.ops)
			r.pending -= unplaced
			if r.pending == 0 {
				s.recordTraceComplete(r)
			}
			s.releaseInflight(int64(unplaced))
		}
		if r.err == nil {
			r.err = err
		}
	}
}

// closeLocked records err as the shutdown reason if shutdown hasn't fired yet.
// s.mu must be held.
func (s *URingScheduler) closeLocked(err error) {
	s.closing.Do(func() {
		if err == nil {
			err = errSchedulerClosed
		}
		s.closing.closed = true
		s.closing.err = err
	})
}

// failPendingLocked fails all requests that have not been picked up by a
// leader. s.mu must be held.
func (s *URingScheduler) failPendingLocked(err error) {
	if err == nil {
		err = errSchedulerClosed
	}
	for _, r := range s.pending {
		if r.done {
			continue
		}
		for i := r.nextOp; i < len(r.ops); i++ {
			if r.ops[i].result.Err == nil {
				r.ops[i].result.Err = err
			}
		}
		if r.pending > 0 {
			s.releaseInflight(int64(r.pending))
			r.pending = 0
			s.recordTraceComplete(r)
		}
		r.nextOp = len(r.ops)
		r.err = err
		r.done = true
	}
	clear(s.pending)
	s.pending = s.pending[:0]
	s.cond.Broadcast()
}

// Close shuts the scheduler down. Pending Submits return errSchedulerClosed.
// If a leader is in flight, Close waits for it to finish its current cycle
// before tearing down the ring.
func (s *URingScheduler) Close() error {
	s.mu.Lock()
	s.closeLocked(errSchedulerClosed)
	// Wake any waiters so they observe closed.
	s.cond.Broadcast()

	// Wait for any active leader to finish.
	for s.leaderActive {
		s.cond.Wait()
	}

	// Fail any pending requests that never got a leader.
	s.failPendingLocked(s.closing.err)
	s.mu.Unlock()

	s.closeRing.Do(func() { s.ring.QueueExit() })
	return nil
}
