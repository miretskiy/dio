package iosched

import (
	"errors"
	"fmt"
	"io"
	"iter"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/miretskiy/dio/internal/buildutil"
	"github.com/miretskiy/dio/internal/intrusive"
	"github.com/miretskiy/dio/mempool"
	"github.com/miretskiy/dio/ringo"
)

// IOUringAvailable reports whether the running kernel provides the io_uring
// features required by URingScheduler.
var IOUringAvailable = probeIOUring()

func probeIOUring() bool {
	return ringo.Available()
}

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
	config schedulerConfig

	ring              *ringo.Ring
	fixedFiles        []ringo.FixedFile
	registeredPool    *mempool.SlabPool
	registeredBuffers *ringo.FixedBuffers
	stagingHead       atomic.Pointer[submission]
	wakeup            chan struct{}

	stop atomic.Pointer[error]

	wg sync.WaitGroup
}

// NewURingScheduler creates an io_uring-backed scheduler. The kernel must
// provide the setup and feature guarantees required by ringo.New.
func NewURingScheduler(opts ...Option) (*URingScheduler, error) {
	if !IOUringAvailable {
		return nil, errors.New("iosched: io_uring not available on this kernel")
	}

	cfg := makeSchedulerConfig(opts)
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
	ring, err := ringo.New(ringOptions...)
	if err != nil {
		return nil, fmt.Errorf("iosched: io_uring_setup: %w", err)
	}
	cfg.ringDepth = uint32(ring.Capacity())

	fixedFiles := make([]ringo.FixedFile, cfg.vfiles)
	for index := range fixedFiles {
		fixedFiles[index], err = ring.FixedFiles().File(uint32(index))
		if err != nil {
			_ = ring.Close()
			return nil, fmt.Errorf("iosched: fixed-file slot %d: %w", index, err)
		}
	}

	s := &URingScheduler{
		config:     cfg,
		ring:       ring,
		fixedFiles: fixedFiles,
		wakeup:     make(chan struct{}, 1),
	}
	if dmaPool != nil {
		buffers, err := ring.RegisterBuffers(dmaPool.RawData())
		if err != nil {
			_ = ring.Close()
			return nil, fmt.Errorf("iosched: io_uring_register_buffers: %w", err)
		}
		s.registeredPool = dmaPool
		s.registeredBuffers = buffers
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

	n, err := countAndValidateOps(&op, &s.config)
	if err != nil {
		return Ticket{}, err
	}
	if uint32(n) > s.config.ringDepth {
		return Ticket{}, fmt.Errorf("iosched: linked chain length %d exceeds ring depth %d", n, s.config.ringDepth)
	}
	if need := transformedSlotCount(&op, n); uint32(need) > s.config.ringDepth {
		return Ticket{}, fmt.Errorf("iosched: operation requires %d ring slots, exceeds ring depth %d", need, s.config.ringDepth)
	}
	if err := s.validateFixedBuffers(&op); err != nil {
		return Ticket{}, err
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
		_ = s.ring.CancelAll(time.Second)

		// The coordinator may instead be idle on the userspace doorbell.
		select {
		case s.wakeup <- struct{}{}:
		default:
		}
	}
	s.wg.Wait()
	err := s.ring.Close()
	s.registeredPool = nil
	s.registeredBuffers = nil
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

type ringQueue interface {
	Push(ringo.Op) (ringHandle, error)
	PushLinked(ringo.Op, ringo.Link, ...ringo.Link) ([]ringHandle, error)
	SubmitAndWait(minComplete int) (submitted int, err error)
	Reap() iter.Seq[ringCompletion]
}

type ringHandle struct {
	handle ringo.Handle
	testID uint64
}

type ringCompletion struct {
	handle ringHandle
	result int
	err    error
}

type liveRingQueue struct {
	ring *ringo.Ring
}

func (queue liveRingQueue) Push(op ringo.Op) (ringHandle, error) {
	handle, err := queue.ring.Push(op)
	return ringHandle{handle: handle}, err
}

func (queue liveRingQueue) PushLinked(
	first ringo.Op,
	link ringo.Link,
	other ...ringo.Link,
) ([]ringHandle, error) {
	handles, err := queue.ring.PushLinked(first, link, other...)
	if err != nil {
		return nil, err
	}
	wrapped := make([]ringHandle, len(handles))
	for i := range handles {
		wrapped[i].handle = handles[i]
	}
	return wrapped, nil
}

func (queue liveRingQueue) SubmitAndWait(minComplete int) (int, error) {
	return queue.ring.SubmitAndWait(minComplete)
}

func (queue liveRingQueue) Reap() iter.Seq[ringCompletion] {
	return func(yield func(ringCompletion) bool) {
		for completion := range queue.ring.Reap() {
			if !yield(ringCompletion{
				handle: ringHandle{handle: completion.Handle},
				result: completion.Result,
				err:    completion.Err,
			}) {
				return
			}
		}
	}
}

type coordinator struct {
	sched *URingScheduler
	ring  ringQueue

	// slots maps Ringo's opaque completion identities back to scheduler-owned
	// logical completion state.
	slots map[ringHandle]ringSlot

	pending intrusive.List[workItem]
	// ready contains pending handles eligible for placement. Work waiting for
	// a barrier remains in pending until the operation it depends on completes.
	ready intrusive.List[intrusive.Handle]
	// coalesced contains every pending handle selected for the next placement,
	// in ready order. Contiguous writes extend it beyond the first item.
	coalesced []intrusive.Handle
	// writeBuffers is coordinator-owned scratch for translating a coalesced
	// write group. Ringo snapshots the slice headers into the resulting Op.
	writeBuffers [][]byte

	files        fileTable
	nextSequence uint64
}

func (s *URingScheduler) loop() {
	defer s.wg.Done()

	c := coordinator{
		sched: s,
		ring:  liveRingQueue{ring: s.ring},
		slots: make(map[ringHandle]ringSlot, s.config.ringDepth),
	}
	c.files = newFileTable(s.config.vfiles)

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
		c.placeReady(c.sched.config.coalescing)

		// After placement, pending work requires at least one occupied ring slot.
		if err := buildutil.Assert(len(c.slots) > 0 || c.pending.Len() == 0); err != nil {
			return fmt.Errorf("iosched: pending work has no runnable or in-flight operation: %w", err)
		}

		if len(c.slots) > 0 {
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
		if err := c.admit(handle); err != nil {
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
		if need > int(c.sched.config.ringDepth)-len(c.slots) {
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

func ringoLinkType(op *Op) ringo.LinkType {
	if op.sqeFlags&sqeHardLink != 0 {
		return ringo.LinkHard
	}
	return ringo.LinkSoft
}

func (c *coordinator) placeChain(handle intrusive.Handle) {
	work := c.pending.Value(handle)
	root := work.root
	first := c.translateOp(root)
	if root.linked == nil {
		ringHandle, err := c.ring.Push(first)
		if err != nil {
			panic(fmt.Sprintf("iosched: Ringo rejected a validated operation: %v", err))
		}
		c.slots[ringHandle] = ringSlot{work: handle, op: root, complete: completeNormal}
		return
	}

	links := make([]ringo.Link, 0, root.opCount()-1)
	for op := root; op.linked != nil; op = op.linked {
		links = append(
			links,
			ringo.Then(ringoLinkType(op), c.translateOp(op.linked)),
		)
	}
	ringHandles, err := c.ring.PushLinked(first, links[0], links[1:]...)
	if err != nil {
		panic(fmt.Sprintf("iosched: Ringo rejected a validated chain: %v", err))
	}
	op := root
	for _, ringHandle := range ringHandles {
		c.slots[ringHandle] = ringSlot{work: handle, op: op, complete: completeNormal}
		op = op.linked
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

	prepared := *firstOp
	if len(handles) > 1 {
		prepared.opcode = OpWritev | (prepared.opcode & opVirtual)
		c.writeBuffers = c.writeBuffers[:0]
		for _, handle := range handles {
			c.writeBuffers = append(
				c.writeBuffers,
				c.pending.Value(handle).root.buf,
			)
		}
		prepared.bufs = c.writeBuffers
	}
	complete := completeWrite
	if durable {
		complete = recordDurableWrite
	}
	write := c.translateOp(&prepared)
	if !durable {
		ringHandle, err := c.ring.Push(write)
		if err != nil {
			panic(fmt.Sprintf("iosched: Ringo rejected a validated write: %v", err))
		}
		c.slots[ringHandle] = ringSlot{work: leader, op: firstOp, complete: complete}
		return
	}

	sync := prepared.syncOp()
	ringHandles, err := c.ring.PushLinked(
		write,
		ringo.Then(ringo.LinkSoft, c.translateOp(&sync)),
	)
	if err != nil {
		panic(fmt.Sprintf("iosched: Ringo rejected a validated durable write: %v", err))
	}
	c.slots[ringHandles[0]] = ringSlot{work: leader, op: firstOp, complete: complete}
	c.slots[ringHandles[1]] = ringSlot{
		work: leader, op: firstOp, complete: completeDurableWrite,
	}
}

func (c *coordinator) translateOp(op *Op) ringo.Op {
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
			return ringo.ReadFixed(fd, buffer, op.offset)
		}
		return ringo.Read(fd, op.buf, op.offset)
	case OpWrite:
		if op.isFixed() {
			buffer, err := c.sched.registeredBuffers.Bind(op.buf)
			if err != nil {
				panic(fmt.Sprintf("iosched: validated fixed buffer became invalid: %v", err))
			}
			return ringo.WriteFixed(fd, buffer, op.offset)
		}
		return ringo.Write(fd, op.buf, op.offset)
	case OpReadv:
		return ringo.Readv(fd, op.bufs, op.offset, 0)
	case OpWritev:
		return ringo.Writev(fd, op.bufs, op.offset, 0)
	case OpFsync:
		return ringo.Fsync(fd)
	case OpFdatasync:
		return ringo.Fdatasync(fd)
	case OpFallocate:
		return ringo.Fallocate(fd, op.offset, op.length)
	case OpOpenat:
		path := string(op.path[:len(op.path)-1])
		if op.isVirtual() {
			return ringo.OpenAtDirect(
				ringo.BorrowedFD(op.dfd),
				path,
				op.openFlag,
				op.mode,
				direct,
			)
		}
		return ringo.OpenAt(
			ringo.BorrowedFD(op.dfd),
			path,
			op.openFlag,
			op.mode,
		)
	case OpClose:
		if op.isVirtual() {
			return ringo.CloseDirect(direct)
		}
		return ringo.Nop()
	default:
		panic(fmt.Sprintf("iosched: invalid opcode %d", op.opcode))
	}
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

func (c *coordinator) reap() int {
	count := 0
	for completion := range c.ring.Reap() {
		c.reapOne(completion)
		count++
	}
	return count
}

func (c *coordinator) reapOne(completion ringCompletion) {
	slot, ok := c.slots[completion.handle]
	if !ok {
		panic(fmt.Sprintf(
			"iosched: Ringo returned unknown completion handle with error %v",
			completion.err,
		))
	}
	err := completion.err
	if err != nil {
		if errors.Is(err, syscall.ECANCELED) && c.sched.stopCause() == errSchedulerClosed {
			err = errSchedulerClosed
		}
	}

	slot.complete(c, &slot, completion.result, err)
	delete(c.slots, completion.handle)
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
	err = writeResultError(op, n, err)
	recordResult(root, op, n, err)
	c.completedOperation(handle, op)
	work.remaining--
	last := work.remaining == 0
	if last {
		c.completedWork(handle, root)
		c.pending.Remove(handle)
		root.done.Done()
	}
}

func (c *coordinator) releaseAllSlots() {
	clear(c.slots)
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
