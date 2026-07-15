//go:build linux

package iosched

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/miretskiy/dio/giouring"
	"github.com/miretskiy/dio/internal/buildutil"
	"github.com/miretskiy/dio/internal/intrusive"
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

type writeTarget struct {
	work  intrusive.Handle
	bytes int
}

type writeCompletion struct {
	targets []writeTarget
	n       int
	err     error
}

type writeResultFn func(*coordinator, intrusive.Handle, int, error)

type workItem struct {
	root      *Op
	waitCount int32
	remaining int32
	sequence  uint64
	ready     intrusive.Handle
	inflight  bool
	write     *writeCompletion
}

type completionFn func(*coordinator, *ringSlot, int, error)

// ringSlot holds state that must remain live until an SQE completes.
type ringSlot struct {
	work     intrusive.Handle
	op       *Op
	complete completionFn

	iovecs []syscall.Iovec
}

type shutdownState struct {
	err error
}

var stagingClosed Op

// URingScheduler is an asynchronous Scheduler backed by io_uring.
//
// A single coordinator goroutine owns the ring. Submitters publish Ops to
// an intrusive lock-free MPSC stack and wake the coordinator with a buffered(1)
// doorbell.
type URingScheduler struct {
	URingConfig

	ring        *giouring.Ring
	stagingHead atomic.Pointer[Op]
	wakeup      chan struct{}

	shutdown atomic.Pointer[shutdownState]

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
	s.wg.Add(1)
	go s.loop()
	return s, nil
}

// Submit enqueues op for asynchronous execution.
func (s *URingScheduler) Submit(op Op) (Ticket, error) {
	if err := s.fatalError(); err != nil {
		return Ticket{}, err
	}

	n, err := countAndValidateOps(&op)
	if err != nil {
		return Ticket{}, err
	}
	if uint32(n) > s.RingDepth {
		return Ticket{}, fmt.Errorf("iosched: linked chain length %d exceeds ring depth %d", n, s.RingDepth)
	}
	if need := transformedSlotCount(&op, n); uint32(need) > s.RingDepth {
		return Ticket{}, fmt.Errorf("iosched: operation requires %d ring slots, exceeds ring depth %d", need, s.RingDepth)
	}

	ticket := op.prepareSubmission()
	if !s.tryPush(&op) {
		return Ticket{}, s.fatalError()
	}
	select {
	case s.wakeup <- struct{}{}:
	default:
	}
	return ticket, nil
}

// transformedSlotCount accounts for SQEs synthesized by the coordinator. A
// standalone durable write is submitted as write -> fdatasync and must fit
// atomically just like a caller-built linked chain.
func transformedSlotCount(op *Op, count int32) int32 {
	if count != 1 || !op.durable() {
		return count
	}
	switch op.kind() {
	case OpWrite, OpWritev:
		return 2
	default:
		return count
	}
}

func (s *URingScheduler) tryPush(op *Op) bool {
	for {
		head := s.stagingHead.Load()
		if head == &stagingClosed {
			return false
		}
		op.staged = head
		if s.stagingHead.CompareAndSwap(head, op) {
			return true
		}
	}
}

func (s *URingScheduler) closeStaging() *Op {
	head := s.stagingHead.Swap(&stagingClosed)
	if head == &stagingClosed {
		return nil
	}
	return head
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
	if err == nil {
		err = errSchedulerClosed
	}
	if s.shutdown.CompareAndSwap(nil, &shutdownState{err: err}) {
		select {
		case s.wakeup <- struct{}{}:
		default:
		}
	}
}

func (s *URingScheduler) fatalError() error {
	state := s.shutdown.Load()
	if state == nil {
		return nil
	}
	return state.err
}

type ringQueue interface {
	GetSQE() *giouring.SubmissionQueueEntry
	SubmitAndWait(uint32) (uint, error)
	ForEachCQE(func(*giouring.CompletionQueueEvent))
	CQAdvance(uint32)
}

type coordinator struct {
	sched *URingScheduler
	ring  ringQueue

	slots intrusive.FixedList[ringSlot]

	works        intrusive.List[workItem]
	ready        intrusive.List[intrusive.Handle]
	files        fileTable
	nextSequence uint64
	coalesced    []intrusive.Handle
	stopping     bool

	reapFn    func(*giouring.CompletionQueueEvent) // cached bound method; rebuilding it in reap allocates
	reapCount uint32
}

func (s *URingScheduler) loop() {
	defer s.wg.Done()
	defer s.ring.QueueExit()

	c := coordinator{
		sched: s,
		ring:  s.ring,
		slots: intrusive.MakeFixedList[ringSlot](int(s.RingDepth)),
	}
	c.files = newFileTable(s.VFiles)
	c.reapFn = c.reapOne

	// Close is handled only after the steady-state loop returns.
	err := c.serve(!s.DisableCoalescing)
	c.shutdownPending(err)
}

// serve is the steady-state submit/reap loop. It runs until a close is signaled
// (nil error) or the ring fails fatally (the error).
func (c *coordinator) serve(coalesce bool) error {
	s := c.sched
	for {
		if c.works.Len() == 0 {
			<-s.wakeup
			if s.fatalError() != nil {
				return nil
			}
		}

		c.accept(reverseOps(s.stagingHead.Swap(nil)))

		before := c.slots.Len()
		c.placeReady(coalesce)
		if placed := c.slots.Len() - before; placed > 0 {
			s.stats.opsPlaced.Add(uint64(placed))
		}

		if c.slots.Len() > 0 {
			if err := c.submit(); err != nil {
				return fmt.Errorf("iosched: ring error: %w", err)
			}
			c.reap()
		} else if c.works.Len() > 0 {
			return errors.New("iosched: queued work is waiting with nothing in flight")
		}
	}
}

// shutdownPending completes every submission the coordinator still owns once serve
// returns, so no caller waits forever. Three kinds can remain:
//
//  1. staged — an add that raced the close, or slipped in as the ring failed;
//  2. accepted but not yet placed;
//  3. in flight — already handed to the kernel.
//
// (1) and (2) never started, so fail them. (3) the kernel owns: on a clean close
// reap it to completion; on a ring error it can't complete, so fail it too.
func (c *coordinator) shutdownPending(err error) {
	s := c.sched
	if err != nil {
		s.stop(err) // record the ring error for the tickets failed below
	}
	c.stopping = true
	c.failSubmissionList(reverseOps(s.closeStaging()), s.fatalError())
	if err != nil {
		c.failAllWork(err)
		c.failAllInflight(err)
	} else {
		c.failWaiting(s.fatalError())
		c.drainInflight()
	}
}

// drainInflight reaps the SQEs still in flight after a graceful close so the
// kernel is done with their buffers before the ring exits.
func (c *coordinator) drainInflight() {
	for c.slots.Len() > 0 {
		if err := c.submit(); err != nil {
			failure := fmt.Errorf("iosched: ring error draining on close: %w", err)
			c.failAllWork(failure)
			c.failAllInflight(failure)
			return
		}
		c.reap()
	}
}

func reverseOps(head *Op) *Op {
	var out *Op
	for head != nil {
		next := head.staged
		head.staged = out
		out = head
		head = next
	}
	return out
}

func (c *coordinator) accept(head *Op) {
	for head != nil {
		root := head
		head = head.staged
		root.staged = nil
		need := root.opCount()

		var err error
		for op := root; op != nil; op = op.linked {
			if op.isVirtual() {
				switch {
				case c.sched.VFiles == 0:
					err = fmt.Errorf("iosched: virtual file ops require URingConfig.VFiles")
				case op.vfd >= c.sched.VFiles:
					err = fmt.Errorf("iosched: virtual file index %d outside configured table", op.vfd)
				}
			}
		}
		if err != nil {
			c.failSubmission(root, err)
			continue
		}

		work := workItem{
			root:      root,
			remaining: int32(need),
			sequence:  c.nextSequence,
		}
		c.nextSequence++
		handle := c.works.PushBack(work)
		if err := c.files.admit(c, handle); err != nil {
			c.works.Remove(handle)
			c.failSubmission(root, err)
			continue
		}
		if c.works.Value(handle).waitCount == 0 {
			c.enqueue(handle)
		}
	}
}

func (c *coordinator) releaseWait(handle intrusive.Handle) {
	if c.stopping {
		return
	}
	work := c.works.Value(handle)
	work.waitCount--
	if err := buildutil.Assert(work.waitCount >= 0); err != nil {
		panic(err)
	}
	if work.waitCount == 0 {
		c.enqueue(handle)
	}
}

func (c *coordinator) enqueue(handle intrusive.Handle) {
	work := c.works.Value(handle)
	if work.ready != 0 || work.inflight {
		return
	}
	work.ready = c.ready.PushBack(handle)
}

func (c *coordinator) failSubmissionList(op *Op, err error) {
	for op != nil {
		next := op.staged
		op.staged = nil
		c.failSubmission(op, err)
		op = next
	}
}

func (c *coordinator) placeReady(coalesce bool) {
	for {
		front, ok := c.ready.Front()
		if !ok {
			return
		}
		handle := *c.ready.Value(front)
		c.coalesced = c.coalesced[:0]
		c.coalesced = append(c.coalesced, handle)
		if coalesce {
			c.coalesced = c.coalescedRun(front, c.coalesced)
		}

		durable := c.durableWrite(c.coalesced)
		writeGroup := len(c.coalesced) > 1 || durable
		need := int(c.works.Value(handle).remaining)
		if writeGroup {
			need = 1
			if durable {
				need++
			}
		}
		if need > c.slots.Cap()-c.slots.Len() {
			return
		}

		for _, ready := range c.coalesced {
			work := c.works.Value(ready)
			c.ready.Remove(work.ready)
			work.ready = 0
			work.inflight = true
		}
		if writeGroup {
			c.placeWriteGroup(c.coalesced, durable)
		} else {
			c.placeChain(handle)
		}
	}
}

func (c *coordinator) coalescedRun(ready intrusive.Handle, run []intrusive.Handle) []intrusive.Handle {
	first := c.works.Value(run[0])
	firstOp := first.root
	if firstOp.linked != nil || !firstOp.coalescibleWrite() {
		return run
	}
	sequence := first.sequence
	runEnd := firstOp.offset + int64(len(firstOp.buf))
	for len(run) < maxCoalescedWrites {
		next, ok := c.ready.Next(ready)
		if !ok {
			break
		}
		ready = next
		handle := *c.ready.Value(ready)
		work := c.works.Value(handle)
		op := work.root
		sequence++
		if work.sequence != sequence || op.linked != nil || !op.coalescibleWrite() ||
			!sameWriteTarget(firstOp, op) || op.offset != runEnd {
			break
		}
		run = append(run, handle)
		runEnd += int64(len(op.buf))
	}
	return run
}

func (c *coordinator) durableWrite(handles []intrusive.Handle) bool {
	durable := false
	for _, handle := range handles {
		op := c.works.Value(handle).root
		if op.linked != nil || (op.kind() != OpWrite && op.kind() != OpWritev) {
			return false
		}
		durable = durable || op.durable()
	}
	return durable
}

func (c *coordinator) placeChain(handle intrusive.Handle) {
	work := c.works.Value(handle)
	for op := work.root; op != nil; op = op.linked {
		slot := c.allocSlot()
		c.initSlot(slot, handle, op, completeNormal)
		c.prepareSlot(slot, op)
	}
}

func (c *coordinator) placeWriteGroup(handles []intrusive.Handle, durable bool) {
	leader := handles[0]
	first := c.works.Value(leader)
	firstOp := first.root
	completion := &writeCompletion{targets: make([]writeTarget, 0, len(handles))}
	for _, handle := range handles {
		op := c.works.Value(handle).root
		completion.targets = append(completion.targets, writeTarget{
			work:  handle,
			bytes: opBytes(op),
		})
	}
	first.write = completion

	writeSlot := c.allocSlot()
	prepared := *firstOp
	if len(handles) > 1 {
		prepared.opcode = OpWritev | (prepared.opcode & opVirtual)
	}
	complete := completeWrite
	if durable {
		prepared.sqeFlags |= sqeLink
		complete = recordDurableWrite
	}
	c.initSlot(writeSlot, leader, firstOp, complete)

	if len(handles) > 1 {
		rs := c.slots.Value(writeSlot)
		for _, handle := range handles {
			rs.iovecs = appendIovec(rs.iovecs, c.works.Value(handle).root.buf)
		}
	}

	var syncSlot intrusive.Handle
	if durable {
		syncSlot = c.allocSlot()
		c.initSlot(syncSlot, leader, firstOp, completeDurableWrite)
	}
	c.prepareSlot(writeSlot, &prepared)
	if durable {
		sync := prepared.syncOp()
		c.prepareSlot(syncSlot, &sync)
	}
}

func (c *coordinator) allocSlot() intrusive.Handle {
	return c.slots.PushBack()
}

func (c *coordinator) initSlot(
	handle intrusive.Handle, work intrusive.Handle, op *Op, complete completionFn,
) {
	rs := c.slots.Value(handle)
	rs.work = work
	rs.op = op
	rs.complete = complete
	clear(rs.iovecs)
	rs.iovecs = rs.iovecs[:0]
}

func (c *coordinator) prepareSlot(handle intrusive.Handle, op *Op) {
	rs := c.slots.Value(handle)
	sqe := c.ring.GetSQE()
	if err := buildutil.Assert(sqe != nil); err != nil {
		panic(fmt.Sprintf("iosched: GetSQE returned nil for slot %d: %v", handle, err))
	}
	if len(rs.iovecs) == 0 {
		if k := op.kind(); k == OpReadv || k == OpWritev {
			for _, buf := range op.bufs {
				rs.iovecs = appendIovec(rs.iovecs, buf)
			}
		}
	}
	prepareSQE(sqe, op, rs.iovecs)
	sqe.SetData64(uint64(handle))
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
	fd := op.dfd
	if op.kind() != OpOpenat {
		fd = op.fd()
	}
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
			// Durability is per-write (Op.Durable), not a property of the open,
			// so open flags pass through unchanged.
			sqe.PrepareOpenatDirect(op.dfd, op.path, op.openFlag, op.mode, op.vfd)
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
	if c.reapFn == nil {
		c.reapFn = c.reapOne
	}
	c.reapCount = 0
	c.ring.ForEachCQE(c.reapFn)
	if c.reapCount > 0 {
		c.ring.CQAdvance(c.reapCount)
	}
}

func (c *coordinator) reapOne(cqe *giouring.CompletionQueueEvent) {
	c.reapCount++
	handle := intrusive.Handle(cqe.GetData64())
	rs := c.slots.Value(handle)

	var n int
	var err error
	if cqe.Res < 0 {
		err = syscall.Errno(-cqe.Res)
	} else {
		n = int(cqe.Res)
	}

	rs.complete(c, rs, n, err)
	c.releaseSlot(handle)
}

func completeNormal(c *coordinator, slot *ringSlot, n int, err error) {
	c.finishOperation(slot.work, slot.op, n, err)
}

func completeWrite(c *coordinator, slot *ringSlot, n int, err error) {
	c.finishWrite(slot.work, n, err, nil)
}

func recordDurableWrite(c *coordinator, slot *ringSlot, n int, err error) {
	completion := c.works.Value(slot.work).write
	completion.n = n
	completion.err = err
	distributeWrite(c, completion, n, err, nil, recordWriteResult)
}

func completeDurableWrite(c *coordinator, slot *ringSlot, _ int, syncErr error) {
	completion := c.works.Value(slot.work).write
	c.finishWrite(slot.work, completion.n, completion.err, syncErr)
}

func (c *coordinator) finishWrite(handle intrusive.Handle, n int, writeErr, syncErr error) {
	completion := c.works.Value(handle).write
	distributeWrite(c, completion, n, writeErr, syncErr, finishWriteTarget)
}

func distributeWrite(
	c *coordinator, completion *writeCompletion, n int, writeErr, syncErr error, apply writeResultFn,
) {
	remaining := n
	for _, target := range completion.targets {
		n := 0
		err := writeErr
		if err == nil {
			n = min(target.bytes, remaining)
			remaining -= n
			if n < target.bytes {
				err = io.ErrShortWrite
			} else if syncErr != nil {
				err = syncErr
			}
		}
		apply(c, target.work, n, err)
	}
}

func recordWriteResult(c *coordinator, handle intrusive.Handle, n int, err error) {
	root := c.works.Value(handle).root
	recordResult(root, root, n, err)
}

func finishWriteTarget(c *coordinator, handle intrusive.Handle, n int, err error) {
	root := c.works.Value(handle).root
	c.finishOperation(handle, root, n, err)
}

func (c *coordinator) finishOperation(handle intrusive.Handle, op *Op, n int, err error) {
	work := c.works.Value(handle)
	root := work.root
	recordResult(root, op, n, err)
	c.files.completedOperation(c, handle, op)
	work.remaining--
	last := work.remaining == 0
	if last {
		c.files.completedWork(c, handle, root)
		c.works.Remove(handle)
		completeSubmission(root)
	}
}

func (c *coordinator) releaseSlot(handle intrusive.Handle) {
	rs := c.slots.Value(handle)
	rs.work = 0
	rs.op = nil
	rs.complete = nil
	// Retain the descriptor arrays for slot reuse, but clear every Go pointer
	// into caller buffers as soon as the CQE is consumed.
	clear(rs.iovecs)
	rs.iovecs = rs.iovecs[:0]
	c.slots.Remove(handle)
}

func (c *coordinator) failAllInflight(_ error) {
	for handle, ok := c.slots.Front(); ok; {
		next, hasNext := c.slots.Next(handle)
		rs := c.slots.Value(handle)
		if rs.complete == nil {
			handle, ok = next, hasNext
			continue
		}
		c.releaseSlot(handle)
		handle, ok = next, hasNext
	}
}

func (c *coordinator) failWaiting(err error) {
	for handle, ok := c.works.Front(); ok; {
		next, hasNext := c.works.Next(handle)
		if !c.works.Value(handle).inflight {
			c.failWork(handle, err)
		}
		handle, ok = next, hasNext
	}
}

func (c *coordinator) failAllWork(err error) {
	for handle, ok := c.works.Front(); ok; {
		next, hasNext := c.works.Next(handle)
		c.failWork(handle, err)
		handle, ok = next, hasNext
	}
}

func (c *coordinator) failWork(handle intrusive.Handle, err error) {
	work := c.works.Value(handle)
	root := work.root
	if work.ready != 0 {
		c.ready.Remove(work.ready)
	}
	c.works.Remove(handle)
	if root.err == nil {
		root.err = err
	}
	completeSubmission(root)
}

func (c *coordinator) failSubmission(root *Op, err error) {
	if root.err == nil {
		root.err = err
	}
	completeSubmission(root)
}
