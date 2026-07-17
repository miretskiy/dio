package iosched

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/miretskiy/dio/giouring"
	"github.com/miretskiy/dio/internal/buildutil"
	"github.com/miretskiy/dio/internal/intrusive"
	"github.com/miretskiy/dio/mempool"
)

// IOUringAvailable reports whether the running kernel provides the io_uring
// features required by URingScheduler.
var IOUringAvailable = probeIOUring()

func probeIOUring() bool {
	ring := giouring.NewRing()
	if err := ring.QueueInit(1, 0); err != nil {
		return false
	}
	defer ring.QueueExit()
	return ring.HasFeature(giouring.FeatNoDrop)
}

const defaultRingDepth uint32 = 256

type writeTarget struct {
	work  intrusive.Handle
	bytes int
}

// writeGroupCompletion is stored in the group's leader workItem. Its
// distribute method snapshots this value before callbacks can remove that
// leader.
type writeGroupCompletion struct {
	inline   [4]writeTarget // first targets in writev order
	overflow []writeTarget  // targets beyond inline capacity
	count    int            // total valid targets
	n        int            // write result retained until fdatasync
	err      error          // write error retained until fdatasync
}

type writeResultFn func(*coordinator, intrusive.Handle, int, error)

type workItem struct {
	root       *Op
	waitCount  int32
	remaining  int32
	sequence   uint64
	ready      intrusive.Handle
	inflight   bool
	writeGroup writeGroupCompletion
}

type completionFn func(*coordinator, *ringSlot, int, error)

// ringSlot holds state that must remain live until an SQE completes.
type ringSlot struct {
	work     intrusive.Handle
	op       *Op
	complete completionFn

	iovecs []syscall.Iovec
}

// submission is the scheduler-owned envelope for one validated Submit call.
type submission struct {
	root       Op          // scheduler-owned copy of the submitted operation chain
	completion completion  // result state shared with the returned Ticket
	count      int32       // validated number of operations in root's chain
	staged     *submission // next item in the lock-free staging stack
}

var stagingClosed submission

// URingScheduler is an asynchronous Scheduler backed by io_uring.
//
// A single coordinator goroutine owns the SQ and CQ. Submitters publish Ops to
// an intrusive lock-free MPSC stack and wake the coordinator with a buffered(1)
// doorbell. Close owns the ring lifetime and may issue synchronous cancellation
// through its fd before tearing it down.
type URingScheduler struct {
	URingConfig

	ring *giouring.Ring
	// registeredPool keeps fixed-buffer backing memory reachable until the
	// ring is closed. The caller still owns and closes the pool.
	registeredPool *mempool.SlabPool
	stagingHead    atomic.Pointer[submission]
	wakeup         chan struct{}

	stop atomic.Pointer[error]

	wg sync.WaitGroup
}

func (s *URingScheduler) usePool(pool *mempool.SlabPool) error {
	if pool == nil {
		return errors.New("iosched: cannot register a nil DMA slab")
	}
	if s.registeredPool != nil {
		return errors.New("iosched: a DMA slab is already registered")
	}

	data := pool.RawData()
	iovs := []syscall.Iovec{{Base: &data[0]}}
	iovs[0].SetLen(len(data))
	if _, err := s.ring.RegisterBuffers(iovs); err != nil {
		return fmt.Errorf("iosched: io_uring_register_buffers: %w", err)
	}
	s.registeredPool = pool
	return nil
}

// NewURingScheduler creates an io_uring-backed scheduler. The kernel must
// provide IORING_FEAT_NODROP.
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
	if err := s.stopCause(); err != nil {
		return Ticket{}, err
	}

	n, err := countAndValidateOps(&op, &s.URingConfig)
	if err != nil {
		return Ticket{}, err
	}
	if uint32(n) > s.RingDepth {
		return Ticket{}, fmt.Errorf("iosched: linked chain length %d exceeds ring depth %d", n, s.RingDepth)
	}
	if need := transformedSlotCount(&op, n); uint32(need) > s.RingDepth {
		return Ticket{}, fmt.Errorf("iosched: operation requires %d ring slots, exceeds ring depth %d", need, s.RingDepth)
	}

	request, ticket := newSubmission(op, n)
	if !s.tryPush(request) {
		return Ticket{}, s.stopCause()
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

func newSubmission(op Op, count int32) (*submission, Ticket) {
	request := &submission{root: op, count: count}
	request.root.completion = &request.completion
	request.completion.done.Add(1)
	return request, Ticket{&request.completion}
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

// Close stops the coordinator and releases ring resources. Work that has not
// completed is failed with the scheduler-closed error. Close waits for the
// coordinator goroutine, which may be blocked waiting for in-flight file I/O.
// Close must be called exactly once. Callers must stop submitting first;
// racing Close with Submit is not supported.
func (s *URingScheduler) Close() error {
	alreadyClosing := !s.signalShutdown(errSchedulerClosed)
	if !alreadyClosing {
		// ANY ignores the match key; ALL cancels every request. Bound the
		// synchronous cancellation call; Close still waits for the coordinator.
		_, _ = s.ring.RegisterSyncCancel(&giouring.SyncCancelReg{
			Flags:   giouring.AsyncCancelAny | giouring.AsyncCancelAll,
			Timeout: syscall.Timespec{Sec: 1},
		})

		// The coordinator may instead be idle on the userspace doorbell.
		select {
		case s.wakeup <- struct{}{}:
		default:
		}
	}
	s.wg.Wait()
	pool := s.registeredPool
	s.ring.QueueExit()
	runtime.KeepAlive(pool)
	s.registeredPool = nil
	return nil
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

type ringQueue interface {
	GetSQE() *giouring.SubmissionQueueEntry
	SubmitAndWait(waitFor uint32) (submitted uint, err error)
	ForEachCQE(func(*giouring.CompletionQueueEvent)) uint32
	CQAdvance(uint32)
}

type coordinator struct {
	sched *URingScheduler
	ring  ringQueue

	// slots contains one entry per SQE until its CQE is reaped.
	slots intrusive.FixedList[ringSlot]

	pending intrusive.List[workItem]
	// ready contains pending handles eligible for placement. Work waiting for
	// a barrier remains in pending until the operation it depends on completes.
	ready intrusive.List[intrusive.Handle]
	// coalesced contains every pending handle selected for the next placement,
	// in ready order. Contiguous writes extend it beyond the first item.
	coalesced []intrusive.Handle

	files        fileTable
	nextSequence uint64
}

func (s *URingScheduler) loop() {
	defer s.wg.Done()

	c := coordinator{
		sched: s,
		ring:  s.ring,
		slots: intrusive.MakeFixedList[ringSlot](int(s.RingDepth)),
	}
	c.files = newFileTable(s.VFiles)

	cause := c.run()
	s.signalShutdown(cause)
	staged := reverseSubmissions(s.closeStaging())
	c.reap()
	c.failRemaining(staged, cause)
	c.releaseAllSlots()
}

func (c *coordinator) run() error {
	for {
		if stop := c.sched.stopCause(); stop != nil {
			return stop
		}
		if c.pending.Len() == 0 {
			<-c.sched.wakeup
		}
		// Else: we still grab newly submitted ops even if we have previously
		// un-started work to keep pipeline full.

		c.accept(reverseSubmissions(c.sched.stagingHead.Swap(nil)))
		c.placeReady(!c.sched.DisableCoalescing)

		// After placement, pending work requires at least one occupied ring slot.
		if err := buildutil.Assert(c.slots.Len() > 0 || c.pending.Len() == 0); err != nil {
			return fmt.Errorf("iosched: pending work has no runnable or in-flight operation: %w", err)
		}

		if c.slots.Len() > 0 {
			if err := c.submitAndWait(); err != nil {
				return fmt.Errorf("iosched: ring error: %w", err)
			}
			c.reap()
		}
	}
}

func reverseSubmissions(head *submission) *submission {
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
		request := head
		head = head.staged
		request.staged = nil
		root := &request.root

		work := workItem{
			root:      root,
			remaining: request.count,
			sequence:  c.nextSequence,
		}
		c.nextSequence++
		handle := c.pending.PushBack(work)
		if err := c.files.admit(c, handle); err != nil {
			c.pending.Remove(handle)
			completeFailed(root, err)
			continue
		}
		if c.pending.Value(handle).waitCount == 0 {
			c.enqueue(handle)
		}
	}
}

func (c *coordinator) releaseWait(handle intrusive.Handle) {
	work := c.pending.Value(handle)
	work.waitCount--
	if err := buildutil.Assert(work.waitCount >= 0); err != nil {
		panic(err)
	}
	if work.waitCount == 0 {
		c.enqueue(handle)
	}
}

func (c *coordinator) enqueue(handle intrusive.Handle) {
	work := c.pending.Value(handle)
	if work.ready != 0 || work.inflight {
		return
	}
	work.ready = c.ready.PushBack(handle)
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
		need := int(c.pending.Value(handle).remaining)
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
			work := c.pending.Value(ready)
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
	first := c.pending.Value(run[0])
	firstOp := first.root
	if firstOp.linked != nil || !firstOp.coalescibleWrite() {
		return run
	}
	sequence := first.sequence
	runEnd := firstOp.offset + int64(len(firstOp.buf))
	// Only adjacent submissions are coalesced. Searching past unrelated work
	// would require proving that every skipped operation is independent of this
	// file; sorting by offset would invent ordering that separate Submit calls do
	// not provide. Keep the placement rule local and predictable instead.
	for len(run) < maxCoalescedWrites {
		next, ok := c.ready.Next(ready)
		if !ok {
			break
		}
		ready = next
		handle := *c.ready.Value(ready)
		work := c.pending.Value(handle)
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
		op := c.pending.Value(handle).root
		if op.linked != nil || (op.kind() != OpWrite && op.kind() != OpWritev) {
			return false
		}
		durable = durable || op.durable()
	}
	return durable
}

func (c *coordinator) placeChain(handle intrusive.Handle) {
	work := c.pending.Value(handle)
	for op := work.root; op != nil; op = op.linked {
		slot := c.allocSlot()
		c.initSlot(slot, handle, op, completeNormal)
		c.prepareSlot(slot, op)
	}
}

func (c *coordinator) placeWriteGroup(handles []intrusive.Handle, durable bool) {
	leader := handles[0]
	first := c.pending.Value(leader)
	firstOp := first.root
	completion := &first.writeGroup
	completion.count = len(handles)
	if extra := len(handles) - len(completion.inline); extra > 0 {
		completion.overflow = make([]writeTarget, extra)
	}
	for i, handle := range handles {
		op := c.pending.Value(handle).root
		target := writeTarget{work: handle, bytes: opBytes(op)}
		if i < len(completion.inline) {
			completion.inline[i] = target
		} else {
			completion.overflow[i-len(completion.inline)] = target
		}
	}

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
			rs.iovecs = appendIovec(rs.iovecs, c.pending.Value(handle).root.buf)
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

	buf := slicePtr(op.buf)
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
		sqe.PrepareReadv(fd, slicePtr(iovecs), uint32(len(iovecs)), uint64(op.offset))
	case OpWritev:
		sqe.PrepareWritev(fd, slicePtr(iovecs), uint32(len(iovecs)), uint64(op.offset))
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

func slicePtr[T any](buf []T) uintptr {
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
	iovec := syscall.Iovec{Base: &b[0]}
	iovec.SetLen(len(b))
	return append(iv, iovec)
}

// submitAndWait submits queued SQEs and asks io_uring to wait for at least one
// completion. Operation results arrive through CQEs, not as enter errors.
func (c *coordinator) submitAndWait() error {
	_, err := c.ring.SubmitAndWait(1)
	switch {
	case err == nil, errors.Is(err, syscall.EINTR):
		return nil
	case errors.Is(err, syscall.EAGAIN), errors.Is(err, syscall.EBUSY):
		// io_uring_enter documents both errors as temporary resource
		// conditions: reap available completions and retry. If none are ready,
		// pause before returning to run so repeated failures do not busy-loop.
		if c.reap() == 0 {
			time.Sleep(time.Microsecond)
		}
		return nil
	default:
		return err
	}
}

func (c *coordinator) reap() uint32 {
	count := c.ring.ForEachCQE(c.reapOne)
	if count > 0 {
		c.ring.CQAdvance(count)
	}
	return count
}

func (c *coordinator) reapOne(cqe *giouring.CompletionQueueEvent) {
	handle := intrusive.Handle(cqe.GetData64())
	rs := c.slots.Value(handle)

	var n int
	var err error
	if cqe.Res < 0 {
		err = syscall.Errno(-cqe.Res)
		if err == syscall.ECANCELED && c.sched.stopCause() == errSchedulerClosed {
			err = errSchedulerClosed
		}
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
	completion := &c.pending.Value(slot.work).writeGroup
	completion.n = n
	completion.err = err
	completion.distribute(c, n, err, nil, recordWriteResult)
}

func completeDurableWrite(c *coordinator, slot *ringSlot, _ int, syncErr error) {
	completion := &c.pending.Value(slot.work).writeGroup
	c.finishWrite(slot.work, completion.n, completion.err, syncErr)
}

func (c *coordinator) finishWrite(handle intrusive.Handle, n int, writeErr, syncErr error) {
	completion := &c.pending.Value(handle).writeGroup
	completion.distribute(c, n, writeErr, syncErr, finishWriteTarget)
}

// distribute has a value receiver intentionally. Applying the leader's result
// can remove and zero the pending workItem that owns the original completion;
// the value copy keeps the inline targets and overflow slice header stable for
// the rest of the group.
func (completion writeGroupCompletion) distribute(
	c *coordinator, n int, writeErr, syncErr error, apply writeResultFn,
) {
	remaining := n
	applyTarget := func(target writeTarget) {
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
	for i := range completion.count {
		var target writeTarget
		if i < len(completion.inline) {
			target = completion.inline[i]
		} else {
			target = completion.overflow[i-len(completion.inline)]
		}
		applyTarget(target)
	}
}

func recordWriteResult(c *coordinator, handle intrusive.Handle, n int, err error) {
	root := c.pending.Value(handle).root
	recordResult(root, root, n, err)
}

func finishWriteTarget(c *coordinator, handle intrusive.Handle, n int, err error) {
	root := c.pending.Value(handle).root
	c.finishOperation(handle, root, n, err)
}

func (c *coordinator) finishOperation(handle intrusive.Handle, op *Op, n int, err error) {
	work := c.pending.Value(handle)
	root := work.root
	recordResult(root, op, n, err)
	c.files.completedOperation(c, handle, op)
	work.remaining--
	last := work.remaining == 0
	if last {
		c.files.completedWork(c, handle, root)
		c.pending.Remove(handle)
		root.done.Done()
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

func (c *coordinator) releaseAllSlots() {
	for handle := range c.slots.All() {
		c.releaseSlot(handle)
	}
}

func (c *coordinator) failRemaining(staged *submission, err error) {
	for handle, work := range c.pending.All() {
		if work.ready != 0 {
			c.ready.Remove(work.ready)
		}
		root := work.root
		c.pending.Remove(handle)
		completeFailed(root, err)
	}
	for staged != nil {
		next := staged.staged
		staged.staged = nil
		completeFailed(&staged.root, err)
		staged = next
	}
}

func completeFailed(root *Op, err error) {
	if root.err == nil {
		root.err = err
	}
	root.done.Done()
}
