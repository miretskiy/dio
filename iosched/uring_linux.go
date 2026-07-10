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

// inflightEntry records the ticket and op holding one placed SQE slot, so reap
// can complete the right op when its CQE arrives. There is exactly one entry per
// placed SQE: a linked chain places one SQE — and so one entry — per op, so op
// names the specific op this slot's CQE completes, whereas t.Op is only the chain
// head. A zero entry (t == nil) marks a free slot.
type inflightEntry struct {
	t  *Ticket
	op *Op
}

// URingScheduler is an asynchronous Scheduler backed by io_uring.
//
// A single coordinator goroutine owns the ring. Submitters publish Tickets to
// an intrusive lock-free MPSC stack and wake the coordinator with a buffered(1)
// doorbell.
type URingScheduler struct {
	URingConfig

	ring        *giouring.Ring
	stagingHead atomic.Pointer[Ticket]
	wakeup      chan struct{}

	shutdown struct {
		once sync.Once
		stop chan struct{}
		err  error
	}

	wg sync.WaitGroup

	stats struct {
		syscalls  atomic.Uint64
		opsPlaced atomic.Uint64
	}
}

// Stats is a point-in-time snapshot of coordinator counters.
type Stats struct {
	Syscalls  uint64
	OpsPlaced uint64
}

// Stats returns a snapshot of coordinator counters.
func (s *URingScheduler) Stats() Stats {
	return Stats{
		Syscalls:  s.stats.syscalls.Load(),
		OpsPlaced: s.stats.opsPlaced.Load(),
	}
}

func (s *URingScheduler) usePool(pool *mempool.SlabPool) error {
	data := pool.RawData()
	iovs := []syscall.Iovec{{Base: &data[0], Len: uint64(len(data))}}
	if _, err := s.ring.RegisterBuffers(iovs); err != nil {
		return fmt.Errorf("iosched: io_uring_register_buffers: %w", err)
	}
	return nil
}

// NewURingScheduler creates an io_uring-backed scheduler.
func NewURingScheduler(cfg URingConfig) (*URingScheduler, error) {
	if !IOUringAvailable {
		return nil, errors.New("iosched: io_uring not available on this kernel")
	}

	depth := cfg.RingDepth
	if depth == 0 {
		depth = defaultRingDepth
	}
	cfg.RingDepth = depth // store the effective depth back into the config

	var flags uint32
	if cfg.SQPOLL {
		flags |= giouring.SetupSQPoll
	}

	ring := giouring.NewRing()
	if err := ring.QueueInit(depth, flags); err != nil {
		return nil, fmt.Errorf("iosched: io_uring_setup: %w", err)
	}
	if cfg.VFiles > 0 {
		if _, err := ring.RegisterFilesSparse(cfg.VFiles); err != nil {
			ring.QueueExit()
			return nil, fmt.Errorf("iosched: io_uring_register_files_sparse: %w", err)
		}
	}

	s := &URingScheduler{
		URingConfig: cfg,
		ring:        ring,
		wakeup:      make(chan struct{}, 1),
	}
	s.shutdown.stop = make(chan struct{})

	s.wg.Add(1)
	go s.loop()
	return s, nil
}

// Submit enqueues op for asynchronous execution.
func (s *URingScheduler) Submit(op Op) (*Ticket, error) {
	if err := s.fatalError(); err != nil {
		return nil, err
	}

	n, err := countAndValidateOps(&op)
	if err != nil {
		return nil, err
	}

	t := getTicket(op, n)

	s.push(t)
	select {
	case s.wakeup <- struct{}{}:
	default:
	}
	return t, nil
}

func (s *URingScheduler) push(t *Ticket) {
	for {
		head := s.stagingHead.Load()
		t.next = head
		if s.stagingHead.CompareAndSwap(head, t) {
			return
		}
	}
}

// Close shuts down the coordinator goroutine and releases ring resources.
// Callers must stop submitting before Close; racing Close with Submit is not
// supported.
func (s *URingScheduler) Close() error {
	s.stop(nil)
	s.wg.Wait()
	return nil
}

func (s *URingScheduler) stop(err error) {
	s.shutdown.once.Do(func() {
		if err == nil {
			err = errSchedulerClosed
		}
		s.shutdown.err = err
		close(s.shutdown.stop)
	})
}

func (s *URingScheduler) fatalError() error {
	select {
	case <-s.shutdown.stop:
		return s.shutdown.err
	default:
		return nil
	}
}

type coordinator struct {
	sched *URingScheduler
	ring  *giouring.Ring

	inflight  []inflightEntry
	freeSlots []int
	nInflight int

	// iovecBufs[slot] backs the iovec array for a vectored/coalesced op placed in
	// that SQE slot. The array must stay valid from submission to CQE — exactly the
	// slot's in-flight window — so it lives here, per slot, reused (and grown) when
	// the slot is next filled. This keeps the iovec backing off every Op.
	iovecBufs [][]syscall.Iovec

	// drain tracks in-flight data ops per file so a close can wait for its file to
	// quiesce before it runs (see drain_linux.go).
	drain drainTable

	// reapSeq is a monotonic completion counter stamped onto each op's result as
	// its CQE is reaped, recording completion order (used by tests).
	reapSeq uint64

	// pending holds every ticket the coordinator has accepted but not yet placed:
	// the ring-full remainder and drain-held closes alike. It is a contiguous slice
	// (not the intrusive next chain) so the place loop iterates cache-friendly
	// memory, and it is compacted in place each iteration — placed tickets drop out,
	// deferred ones stay. A deferred ticket is always waiting on an in-flight op
	// that will free it, which is what lets the loop block instead of spin.
	pending []*Ticket

	// closes is reused each place pass to hold the batch's closes so they are
	// evaluated only after every data op has been counted (see placeOrDefer).
	closes []*Ticket
}

func (s *URingScheduler) loop() {
	defer s.wg.Done()
	defer s.ring.QueueExit()

	c := coordinator{
		sched:     s,
		ring:      s.ring,
		inflight:  make([]inflightEntry, s.RingDepth),
		freeSlots: make([]int, s.RingDepth),
		iovecBufs: make([][]syscall.Iovec, s.RingDepth),
	}
	c.pending = make([]*Ticket, 0, 64)
	c.closes = make([]*Ticket, 0, 16)
	c.drain = newDrainTable(int(s.VFiles) + drainDenseMargin)
	for i := range s.RingDepth {
		c.freeSlots[i] = int(i)
	}

	// serve runs the steady state; shutdownPending settles everything the
	// coordinator still owns (c.pending plus in-flight SQEs) once it stops. Close
	// is handled only here.
	err := c.serve(!s.DisableCoalescing)
	c.shutdownPending(err)
}

// serve is the steady-state submit/reap loop. It runs until a close is signaled
// (nil error) or the ring fails fatally (the error), leaving whatever it had
// accepted but not yet placed in c.pending for shutdownPending to settle.
//
// The loop never spins. Each iteration it absorbs newly staged tickets, places
// what it can, and then blocks: on the doorbell when there is nothing to do
// (c.pending empty and nothing in flight), otherwise in SubmitAndWait for a
// completion. The blocking is safe because a ticket only stays in c.pending when
// an in-flight op will free it — a close waiting for its file's data ops to
// drain, or the ring being full — so c.pending non-empty implies nInflight>0.
func (c *coordinator) serve(coalesce bool) error {
	s := c.sched
	for {
		// Idle: no work to place and nothing in flight to reap. Wait for the
		// doorbell (a Submit) or for close.
		if len(c.pending) == 0 && c.nInflight == 0 {
			select {
			case <-s.wakeup:
			case <-s.shutdown.stop:
				return nil
			}
		}

		// Absorb newly staged tickets (non-blocking) onto the pending buffer.
		c.pending = c.filterRunnable(c.pending, reverseTickets(s.stagingHead.Swap(nil)))

		if coalesce {
			c.pending = coalesceWrites(c.pending)
		}
		c.pending = linkDurableSyncs(c.pending)

		before := c.nInflight
		c.pending = c.placeOrDefer(c.pending)
		if placed := c.nInflight - before; placed > 0 {
			s.stats.opsPlaced.Add(uint64(placed))
		}

		if c.nInflight > 0 {
			if err := c.submit(); err != nil {
				return fmt.Errorf("iosched: ring error: %w", err)
			}
			// reap clears the in-flight counts a deferred close waits on; the next
			// iteration re-checks c.pending and places any close that just unblocked.
			c.reap()
		}
	}
}

// shutdownPending completes every ticket the coordinator still owns once serve
// returns, so no caller waits forever. Three kinds can remain:
//
//  1. staged — an add that raced the close, or slipped in as the ring failed;
//  2. accepted but not yet placed — c.pending, including any drain-held close;
//  3. in flight — already handed to the kernel.
//
// (1) and (2) never started, so fail them. (3) the kernel owns: on a clean close
// reap it to completion; on a ring error it can't complete, so fail it too.
func (c *coordinator) shutdownPending(err error) {
	s := c.sched
	if err != nil {
		s.stop(err) // record the ring error for the tickets failed below
	}
	c.failTicketSlice(c.pending, s.shutdown.err)
	c.failStaged(s.shutdown.err)
	if err != nil {
		c.failAllInflight(err)
	} else {
		c.drainInflight()
	}
}

// drainInflight reaps the SQEs still in flight after a graceful close so the
// kernel is done with their buffers before the ring exits. Any drain-held close
// was already failed from c.pending, so a completion here only settles its own
// ticket.
func (c *coordinator) drainInflight() {
	for c.nInflight > 0 {
		if err := c.submit(); err != nil {
			c.failAllInflight(fmt.Errorf("iosched: ring error draining on close: %w", err))
			return
		}
		c.reap()
	}
}

func reverseTickets(head *Ticket) *Ticket {
	var out *Ticket
	for head != nil {
		next := head.next
		head.next = out
		out = head
		head = next
	}
	return out
}

// filterRunnable appends the valid tickets from the linked list head (drained
// staging) onto batch, returning the extended slice. A ticket is failed if its
// chain is longer than the ring depth, or if it names a virtual slot with no
// table configured or an index outside it. Validation only — the place step
// (placeOrDefer) decides when a ticket runs; the drain there holds a close until
// its file's in-flight ops complete (see Scheduler.Submit).
func (c *coordinator) filterRunnable(batch []*Ticket, head *Ticket) []*Ticket {
	for head != nil {
		t := head
		head = head.next
		t.next = nil

		if need := int(t.pending.Load()); need > int(c.sched.RingDepth) {
			c.failTicket(t, fmt.Errorf("iosched: linked chain length %d exceeds ring depth %d", need, c.sched.RingDepth))
			continue
		}

		var err error
		for op := &t.Op; op != nil; op = op.linked {
			if !op.isVirtual() {
				continue
			}
			if c.sched.VFiles == 0 {
				err = fmt.Errorf("iosched: virtual file ops require URingConfig.VFiles")
				break
			}
			if op.vfd >= c.sched.VFiles {
				err = fmt.Errorf("iosched: virtual file index %d outside configured table", op.vfd)
				break
			}
		}
		if err != nil {
			c.failTicket(t, err)
			continue
		}

		batch = append(batch, t)
	}
	return batch
}

// failTicketSlice fails every ticket in batch (used for the unplaced remainder
// at shutdown; the linked staging and parked lists use failTicketList).
func (c *coordinator) failTicketSlice(batch []*Ticket, err error) {
	for _, t := range batch {
		c.failTicket(t, err)
	}
}

// placeOrDefer places as many of pending's tickets into free SQEs as fit, and
// returns the deferred remainder compacted to the front of pending's backing —
// tickets that could not run this round and stay for the next. A ticket is
// deferred when the ring is full, or — for a close — when its file still has
// in-flight data ops (the close-drain, use-before-close); reap frees those next
// round.
//
// It runs in two passes: pass 1 places non-closes, counting each file's in-flight
// data ops via drainInc; pass 2 places or defers closes. Counting every data op
// first guarantees the drain sees a close's batch-mates regardless of their order
// in the batch. When the ring is full, no close can get an SQE either, so a close
// can never slip ahead of a same-file write that a full ring left deferred.
func (c *coordinator) placeOrDefer(pending []*Ticket) []*Ticket {
	// w is the compaction write cursor: a deferred ticket is packed to pending[:w].
	// It never overtakes the range's read cursor (we defer at most as many as we
	// have read), so writing pending[w] never clobbers an unread ticket.
	w := 0
	deferTicket := func(t *Ticket) { pending[w] = t; w++ }
	closes := c.closes[:0]
	ringFull := false
	for _, t := range pending {
		if t.Op.kind() == OpClose {
			closes = append(closes, t)
			continue
		}
		if ringFull {
			deferTicket(t)
			continue
		}
		need := int(t.pending.Load())
		if need > len(c.freeSlots) {
			ringFull = true
			deferTicket(t)
			continue
		}
		c.placeTicket(t, need)
	}
	for _, t := range closes {
		if ringFull || c.closeBlocked(t) {
			deferTicket(t)
			continue
		}
		need := int(t.pending.Load())
		if need > len(c.freeSlots) {
			ringFull = true
			deferTicket(t)
			continue
		}
		c.placeTicket(t, need)
	}
	c.closes = closes[:0] // retain the (grown) backing
	return pending[:w]
}

func (c *coordinator) failTicketList(t *Ticket, err error) {
	for t != nil {
		next := t.next
		t.next = nil
		c.failTicket(t, err)
		t = next
	}
}

func (c *coordinator) placeTicket(t *Ticket, need int) {
	if need > len(c.freeSlots) {
		err := fmt.Errorf("iosched: insufficient free SQE slots; need=%d free=%d", need, len(c.freeSlots))
		c.failTicket(t, err)
		c.sched.stop(err)
		return
	}
	for op := &t.Op; op != nil; op = op.linked {
		slot := c.freeSlots[len(c.freeSlots)-1]
		c.freeSlots = c.freeSlots[:len(c.freeSlots)-1]

		sqe := c.ring.GetSQE()
		if err := buildutil.Assert(sqe != nil); err != nil {
			err = fmt.Errorf(
				"iosched: GetSQE returned nil; slot=%d nInflight=%d freeSlots=%d depth=%d: %w",
				slot, c.nInflight, len(c.freeSlots)+1, c.sched.RingDepth, err,
			)
			c.freeSlots = append(c.freeSlots, slot)
			c.failAllInflight(err)
			c.failTicket(t, err)
			c.sched.stop(err)
			return
		}

		var iovecs []syscall.Iovec
		if k := op.kind(); k == OpReadv || k == OpWritev {
			iovecs = c.slotIovecs(slot, t, op)
		}
		prepareSQE(sqe, op, iovecs)
		sqe.SetData64(uint64(slot))

		c.inflight[slot] = inflightEntry{t: t, op: op}
		c.nInflight++
		c.drainInc(op)
	}
}

// fd returns the descriptor op names: a virtual op's slot index (vfd) or a
// regular op's process fd. Whether it's a fixed file is op.isVirtual().
func (o *Op) fd() int {
	if o.isVirtual() {
		return int(o.vfd)
	}
	return int(o.f.Fd())
}

func prepareSQE(sqe *giouring.SubmissionQueueEntry, op *Op, iovecs []syscall.Iovec) {
	// A "direct descriptor" (a.k.a. registered/fixed file) is a file held in
	// io_uring's own table and named by a slot index, not a process fd.
	// IOSQE_FIXED_FILE is the switch into that world: it tells the kernel to read
	// the SQE's fd field as a registered slot index rather than a process fd. So
	// it belongs on virtual read/write/fsync/fallocate — ops that name their file
	// through the fd field.
	//
	// openat and close instead reach the table through the separate file_index
	// field (the *Direct helpers set it via setTargetFixedFile), not the fd
	// field, so they must NOT set IOSQE_FIXED_FILE — for close,
	// io_uring_prep_close_direct(3) mandates this explicitly. Each clears it in
	// its case below.
	var sqeFlags uint32
	fd := op.fd()
	if op.isVirtual() {
		sqeFlags |= uint32(giouring.SqeFixedFile)
	}
	if op.isLinked() {
		if op.sqeFlags&sqeHardLink != 0 {
			sqeFlags |= uint32(giouring.SqeIOHardlink)
		} else {
			sqeFlags |= uint32(giouring.SqeIOLink)
		}
	}

	buf := bufferPtr(op.buf)
	switch op.kind() {
	case OpRead:
		if op.isFixed() {
			sqe.PrepareReadFixed(fd, buf, uint32(len(op.buf)), uint64(op.offset), 0)
		} else {
			sqe.PrepareRead(fd, buf, uint32(len(op.buf)), uint64(op.offset))
		}
	case OpWrite:
		if op.isFixed() {
			sqe.PrepareWriteFixed(fd, buf, uint32(len(op.buf)), uint64(op.offset), 0)
		} else {
			sqe.PrepareWrite(fd, buf, uint32(len(op.buf)), uint64(op.offset))
		}
	case OpReadv:
		sqe.PrepareReadv(fd, iovecsPtr(iovecs), uint32(len(iovecs)), uint64(op.offset))
	case OpWritev:
		sqe.PrepareWritev(fd, iovecsPtr(iovecs), uint32(len(iovecs)), uint64(op.offset))
	case OpFsync:
		sqe.PrepareFsync(fd, 0)
	case OpFdatasync:
		sqe.PrepareFsync(fd, giouring.FsyncDatasync)
	case OpFallocate:
		sqe.PrepareFallocate(fd, 0, uint64(op.offset), uint64(op.length))
	case OpOpenat:
		// openat's fd field is the dirfd (a real fd / AT_FDCWD), not a registered
		// index, so IOSQE_FIXED_FILE must stay off. The direct form installs the
		// opened file into registered slot fd (carried in file_index); the plain
		// form returns a regular fd in the completion result.
		sqeFlags &^= uint32(giouring.SqeFixedFile)
		if op.isVirtual() {
			// Durability is per-write (Op.Durable/Op.Sync via linkDurableSyncs), not a
			// property of the open, so open flags pass through unchanged.
			sqe.PrepareOpenatDirect(op.dfd, op.path, op.openFlag, op.mode, uint32(fd))
		} else {
			sqe.PrepareOpenat(op.dfd, op.path, op.openFlag, op.mode)
		}
	case OpClose:
		// Closes direct descriptor fd. Per io_uring_prep_close_direct(3) the
		// application must not set IOSQE_FIXED_FILE even though it targets a
		// direct descriptor: the slot rides in file_index (set by
		// PrepareCloseDirect), not the fd field.
		sqeFlags &^= uint32(giouring.SqeFixedFile)
		if op.isVirtual() {
			sqe.PrepareCloseDirect(uint32(fd))
		} else {
			sqe.PrepareClose(fd)
		}
	default:
		panic(fmt.Sprintf("iosched: invalid opcode %d", op.opcode))
	}

	if sqeFlags != 0 {
		sqe.SetFlags(sqeFlags)
	}
}

func bufferPtr(buf []byte) uintptr {
	if len(buf) == 0 {
		return 0
	}
	return uintptr(unsafe.Pointer(&buf[0]))
}

// slotIovecs builds the iovec array for a vectored op into the op's SQE-slot
// buffer, reusing and growing it. A user vectored op supplies its buffers in
// op.bufs; a coalesced writev leader (no bufs) supplies them as its own buf plus
// each follower's, walked via the ticket's group. The result is valid until the
// slot's CQE, when the slot — and this buffer — is reused.
func (c *coordinator) slotIovecs(slot int, t *Ticket, op *Op) []syscall.Iovec {
	iv := c.iovecBufs[slot][:0]
	if op.bufs != nil {
		for _, b := range op.bufs {
			iv = appendIovec(iv, b)
		}
	} else {
		iv = appendIovec(iv, op.buf)
		for m := t.group; m != nil; m = m.next {
			iv = appendIovec(iv, m.Op.buf)
		}
	}
	c.iovecBufs[slot] = iv
	return iv
}

// appendIovec appends an iovec for b unless b is empty.
func appendIovec(iv []syscall.Iovec, b []byte) []syscall.Iovec {
	if len(b) == 0 {
		return iv
	}
	return append(iv, syscall.Iovec{Base: &b[0], Len: uint64(len(b))})
}

func iovecsPtr(iovs []syscall.Iovec) uintptr {
	if len(iovs) == 0 {
		return 0
	}
	return uintptr(unsafe.Pointer(&iovs[0]))
}

func (c *coordinator) submit() error {
	c.sched.stats.syscalls.Add(1) // one ring-enter per submit; EINTR re-entry is the same one
	for {
		_, err := c.ring.SubmitAndWait(1)
		if err == nil {
			return nil
		}
		// io_uring_enter is io_uring's one interruptible point: a signal (e.g.
		// Go's SIGURG async preemption) can interrupt the wait-for-completion.
		// Re-enter to resume. The submitted read/write ops run asynchronously in
		// kernel context and never complete with -EINTR, so this is the only
		// EINTR handling io_uring needs. See the package doc.
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return err
	}
}

func (c *coordinator) reap() {
	var count uint32
	c.ring.ForEachCQE(func(cqe *giouring.CompletionQueueEvent) {
		count++
		slot := int(cqe.GetData64())
		e := c.inflight[slot]
		c.releaseSlot(slot)

		c.reapSeq++
		e.op.result.seq = c.reapSeq
		if cqe.Res < 0 {
			e.op.result.Err = syscall.Errno(-cqe.Res)
		} else {
			e.op.result.N = int(cqe.Res)
		}

		// Decrement the file's in-flight data-op count. When it reaches zero, a
		// close deferred in c.pending will place on the next iteration. Done before
		// the group branch so a coalesced leader's ops are counted too.
		c.drainDec(e.op)

		if e.t.group != nil {
			// Coalesced leader. Its chain is the writev, plus a linked fdatasync
			// when the slot is durable, so it can span more than one CQE; fan out
			// to the group only once the whole chain is done (pending hits zero).
			if e.t.pending.Add(-1) == 0 {
				completeCoalescedGroup(e.t)
			}
			return
		}

		completeTicket(e.t)
	})
	if count > 0 {
		c.ring.CQAdvance(count)
	}
}

func (c *coordinator) releaseSlot(slot int) {
	c.inflight[slot] = inflightEntry{}
	// The op is done (its CQE was reaped). c.iovecBufs[slot] is the reused array of
	// iovec descriptors ({Base *byte, Len}) built for it; each Base points into a
	// caller buffer. Zeroing the descriptors drops those pointers so a completed
	// op's buffers aren't pinned for GC — it does not touch the caller's data. The
	// pointer outlives putTicket zeroing the Op (Base aliases the buffer's array,
	// not the Op's slice header), which is why we drop it here. Clearing the
	// written region then truncating leaves the [len:cap] tail nil, so a slot that
	// next holds a non-vectored op clears nothing. Clear, not nil: nil would drop
	// the backing and re-allocate on the next vectored op, the reuse this per-slot
	// buffer exists for.
	clear(c.iovecBufs[slot])
	c.iovecBufs[slot] = c.iovecBufs[slot][:0]
	c.freeSlots = append(c.freeSlots, slot)
	c.nInflight--
}

func (c *coordinator) failAllInflight(err error) {
	// One inflight entry represents one placed SQE, including each SQE in a
	// linked chain. Completing each entry once preserves a ticket's remaining
	// count even if earlier linked SQEs already reaped successfully. A zero entry
	// (t == nil) is a free slot.
	for i, e := range c.inflight {
		if e.t == nil {
			continue
		}
		c.releaseSlot(i)
		if e.t.group != nil {
			failCoalescedWrite(e.t, err)
			continue
		}
		if e.op.result.Err == nil {
			e.op.result.Err = err
		}
		completeTicket(e.t)
	}
}

func (c *coordinator) failStaged(err error) {
	for {
		batch := c.sched.stagingHead.Swap(nil)
		if batch == nil {
			return
		}
		c.failTicketList(reverseTickets(batch), err)
	}
}

func (c *coordinator) failTicket(t *Ticket, err error) {
	// failTicket is for tickets not represented by inflight entries. Inflight
	// SQEs complete through reap or failAllInflight one SQE at a time.
	if t.group != nil {
		// Coalesced writev leader: fan the failure out to the whole group.
		failCoalescedWrite(t, err)
		return
	}
	for op := &t.Op; op != nil; op = op.linked {
		if op.result.Err == nil {
			op.result.Err = err
		}
	}
	if n := t.pending.Swap(0); n > 0 {
		t.wg.Done()
	}
}
