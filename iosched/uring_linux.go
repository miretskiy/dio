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

type inflightEntry struct {
	t     *Ticket
	op    *Op
	valid bool
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

	parkedOps []*Ticket

	// coalesce is reused each batch to gather write-coalescing candidates.
	coalesce []*Ticket
}

var barrierOp Ticket

func (s *URingScheduler) loop() {
	defer s.wg.Done()
	defer s.ring.QueueExit()

	c := coordinator{
		sched:     s,
		ring:      s.ring,
		inflight:  make([]inflightEntry, s.RingDepth),
		freeSlots: make([]int, s.RingDepth),
	}
	if s.VFiles > 0 {
		c.parkedOps = make([]*Ticket, s.VFiles)
	}
	c.coalesce = make([]*Ticket, 0, s.RingDepth)
	for i := range s.RingDepth {
		c.freeSlots[i] = int(i)
	}

	// serve runs the steady state; shutdownPending settles everything the
	// coordinator still owns once it stops. Close is handled only here.
	batch, err := c.serve(!s.DisableCoalescing)
	c.shutdownPending(batch, err)
}

// serve is the steady-state submit/reap loop. It returns when a close is
// signaled (nil error) or the ring fails fatally (the error), along with the
// batch it had collected but not yet placed.
func (c *coordinator) serve(coalesce bool) (*Ticket, error) {
	s := c.sched
	var batch *Ticket
	for {
		batch = c.collect(batch)
		select {
		case <-s.shutdown.stop:
			return batch, nil
		default:
		}
		if coalesce {
			batch = c.coalesceBatch(batch)
		}
		before := c.nInflight
		batch = c.fillSlots(batch)
		if placed := c.nInflight - before; placed > 0 {
			s.stats.syscalls.Add(1)
			s.stats.opsPlaced.Add(uint64(placed))
		}
		if c.nInflight == 0 {
			continue
		}
		if err := c.submit(); err != nil {
			return batch, fmt.Errorf("iosched: ring error: %w", err)
		}
		if unparked := c.reap(); unparked != nil {
			batch = c.filterRunnableAfter(unparked, batch)
		}
	}
}

// shutdownPending completes every ticket the coordinator still owns once serve
// returns, so no caller waits forever. Three kinds can remain:
//
//  1. staged — an add that raced the close, or slipped in as the ring failed;
//  2. collected but not yet placed (batch), or parked behind a barrier;
//  3. in flight — already handed to the kernel.
//
// (1) and (2) never started, so fail them. (3) the kernel owns: on a clean close
// reap it to completion; on a ring error it can't complete, so fail it too.
func (c *coordinator) shutdownPending(batch *Ticket, err error) {
	s := c.sched
	if err != nil {
		s.stop(err) // record the ring error for the tickets failed below
	}
	c.failTicketList(batch, s.shutdown.err)
	c.failStaged(s.shutdown.err)
	c.failParked(s.shutdown.err)
	if err != nil {
		c.failAllInflight(err)
	} else {
		c.drainInflight()
	}
}

// drainInflight reaps the SQEs still in flight after a graceful close so the
// kernel is done with their buffers before the ring exits. Parked tickets were
// already failed, so reap unparks nothing here.
func (c *coordinator) drainInflight() {
	for c.nInflight > 0 {
		if err := c.submit(); err != nil {
			c.failAllInflight(fmt.Errorf("iosched: ring error draining on close: %w", err))
			return
		}
		c.reap()
	}
}

func (c *coordinator) collect(batch *Ticket) *Ticket {
	s := c.sched
	if batch != nil {
		return c.filterRunnableAfter(reverseTickets(s.stagingHead.Swap(nil)), batch)
	}

	if staged := s.stagingHead.Swap(nil); staged != nil {
		return c.filterRunnableAfter(reverseTickets(staged), nil)
	}

	// No work: wait for the doorbell, or wake on close. When ops are in flight we
	// must not block — we still need to reap them — so only poll the doorbell.
	if c.nInflight == 0 {
		select {
		case <-s.wakeup:
		case <-s.shutdown.stop:
		}
	} else {
		select {
		case <-s.wakeup:
		default:
		}
	}

	return c.filterRunnableAfter(reverseTickets(s.stagingHead.Swap(nil)), nil)
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

func appendTickets(head, tail *Ticket) *Ticket {
	if head == nil {
		return tail
	}
	if tail == nil {
		return head
	}
	for t := head; ; t = t.next {
		if t.next == nil {
			t.next = tail
			return head
		}
	}
}

func (c *coordinator) filterRunnable(head *Ticket) *Ticket {
	return c.filterRunnableAfter(head, nil)
}

func (c *coordinator) filterRunnableAfter(head, runnable *Ticket) *Ticket {
	var tail *Ticket
	if runnable != nil {
		tail = runnable
		for tail.next != nil {
			tail = tail.next
		}
	}

	for head != nil {
		t := head
		head = head.next
		t.next = nil

		if need := int(t.pending.Load()); need > int(c.sched.RingDepth) {
			c.failTicket(t, fmt.Errorf("iosched: linked chain length %d exceeds ring depth %d", need, c.sched.RingDepth))
			continue
		}

		var err error
		var parkSlot uint32
		var parked bool

		for op := &t.Op; op != nil; op = op.linked {
			if !op.isVirtual() {
				continue
			}
			if c.sched.VFiles == 0 {
				err = fmt.Errorf("iosched: virtual file ops require URingConfig.VFiles")
				break
			}
			slot := op.vfd
			if slot >= c.sched.VFiles {
				err = fmt.Errorf("iosched: virtual file index %d outside configured table", slot)
				break
			}
			if c.parkedOps[slot] != nil {
				parked = true
				parkSlot = slot
				break
			}
		}

		if err != nil {
			c.failTicket(t, err)
			continue
		}
		if parked {
			c.parkTicket(parkSlot, t)
			continue
		}

		for op := &t.Op; op != nil; op = op.linked {
			if op.isVBarrier() {
				c.parkedOps[op.vfd] = &barrierOp
			}
		}

		if runnable == nil {
			runnable = t
		} else {
			tail.next = t
		}
		tail = t
	}
	return runnable
}

func (c *coordinator) parkTicket(slot uint32, t *Ticket) {
	if c.parkedOps[slot] == &barrierOp {
		c.parkedOps[slot] = t
		return
	}
	c.parkedOps[slot] = appendTickets(c.parkedOps[slot], t)
}

// coalesceBatch rewrites the runnable batch, merging contiguous same-file plain
// writes into single writev leaders (see coalesce.go). Non-coalescible ops pass
// through unchanged. Merged followers leave the batch — the leader's completion
// fans out to them — so this both shrinks SQE usage and preserves each caller's
// ticket. Returns the batch to place.
func (c *coordinator) coalesceBatch(head *Ticket) *Ticket {
	cands := c.coalesce[:0]
	var pass, passTail *Ticket
	for head != nil {
		t := head
		head = head.next
		t.next = nil
		if (&t.Op).coalescibleWrite() {
			cands = append(cands, t)
			continue
		}
		if passTail == nil {
			pass, passTail = t, t
		} else {
			passTail.next = t
			passTail = t
		}
	}
	for _, leader := range groupCoalescibleWrites(cands) {
		leader.next = nil
		if passTail == nil {
			pass, passTail = leader, leader
		} else {
			passTail.next = leader
			passTail = leader
		}
	}
	c.coalesce = cands[:0] // retain the (possibly grown) backing for reuse
	return pass
}

func (c *coordinator) fillSlots(head *Ticket) *Ticket {
	for head != nil {
		t := head
		need := int(t.pending.Load())
		if need > len(c.freeSlots) {
			return head
		}
		head = t.next
		t.next = nil
		c.placeTicket(t, need)
	}
	return nil
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

		prepareSQE(sqe, op)
		sqe.SetData64(uint64(slot))

		c.inflight[slot] = inflightEntry{
			t:     t,
			op:    op,
			valid: true,
		}
		c.nInflight++
	}
}

func prepareSQE(sqe *giouring.SubmissionQueueEntry, op *Op) {
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
	var fd int
	if op.isVirtual() {
		sqeFlags |= uint32(giouring.SqeFixedFile)
		fd = int(op.vfd)
	} else {
		fd = int(op.f.Fd())
	}
	if op.isLinked() {
		sqeFlags |= uint32(giouring.SqeIOLink)
	}

	buf := bufferPtr(op.buf)
	switch op.base() {
	case OpRead:
		sqe.PrepareRead(fd, buf, uint32(len(op.buf)), uint64(op.offset))
	case OpWrite:
		sqe.PrepareWrite(fd, buf, uint32(len(op.buf)), uint64(op.offset))
	case OpReadFixed:
		sqe.PrepareReadFixed(fd, buf, uint32(len(op.buf)), uint64(op.offset), 0)
	case OpWriteFixed:
		sqe.PrepareWriteFixed(fd, buf, uint32(len(op.buf)), uint64(op.offset), 0)
	case OpReadv:
		sqe.PrepareReadv(fd, iovecsPtr(op.iovecs), uint32(len(op.iovecs)), uint64(op.offset))
	case OpWritev:
		sqe.PrepareWritev(fd, iovecsPtr(op.iovecs), uint32(len(op.iovecs)), uint64(op.offset))
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

func iovecsPtr(iovs []syscall.Iovec) uintptr {
	if len(iovs) == 0 {
		return 0
	}
	return uintptr(unsafe.Pointer(&iovs[0]))
}

func (c *coordinator) submit() error {
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

func (c *coordinator) reap() *Ticket {
	var count uint32
	var ready, readyTail *Ticket
	c.ring.ForEachCQE(func(cqe *giouring.CompletionQueueEvent) {
		count++
		slot := int(cqe.GetData64())
		e := c.inflight[slot]
		c.releaseSlot(slot)

		if e.t.group != nil {
			// Coalesced writev: one CQE completes the whole group of tickets.
			completeCoalescedWrite(e.t, cqe.Res)
			return
		}

		if cqe.Res < 0 {
			e.op.Result.Err = syscall.Errno(-cqe.Res)
		} else {
			e.op.Result.N = int(cqe.Res)
		}
		if unparked := c.completeVOp(e.op); unparked != nil {
			if ready == nil {
				ready = unparked
			} else {
				readyTail.next = unparked
			}
			readyTail = unparked
			for readyTail.next != nil {
				readyTail = readyTail.next
			}
		}
		completeTicket(e.t)
	})
	if count > 0 {
		c.ring.CQAdvance(count)
	}
	return ready
}

func (c *coordinator) completeVOp(op *Op) *Ticket {
	if !op.isVBarrier() || c.sched.VFiles == 0 {
		return nil
	}

	index := op.vfd
	parked := c.takeParked(index)

	if op.base() == OpClose {
		c.failTicketList(parked, syscall.EBADF)
		return nil
	}

	if op.Result.Err != nil {
		c.failTicketList(parked, op.Result.Err)
		return nil
	}

	return parked
}

func (c *coordinator) takeParked(slot uint32) *Ticket {
	head := c.parkedOps[slot]
	c.parkedOps[slot] = nil
	if head == &barrierOp {
		return nil
	}
	return head
}

func (c *coordinator) releaseSlot(slot int) {
	c.inflight[slot] = inflightEntry{}
	c.freeSlots = append(c.freeSlots, slot)
	c.nInflight--
}

func (c *coordinator) failAllInflight(err error) {
	// One inflight entry represents one placed SQE, including each SQE in a
	// linked chain. Completing each entry once preserves a ticket's remaining
	// count even if earlier linked SQEs already reaped successfully.
	for i, e := range c.inflight {
		if !e.valid {
			continue
		}
		c.releaseSlot(i)
		if e.t.group != nil {
			failCoalescedWrite(e.t, err)
			continue
		}
		if e.op.Result.Err == nil {
			e.op.Result.Err = err
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

func (c *coordinator) failParked(err error) {
	for i, t := range c.parkedOps {
		c.parkedOps[i] = nil
		if t == &barrierOp {
			continue
		}
		c.failTicketList(t, err)
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
		if op.Result.Err == nil {
			op.Result.Err = err
		}
	}
	if n := t.pending.Swap(0); n > 0 {
		t.wg.Done()
	}
}
