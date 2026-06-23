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
	ring        *giouring.Ring
	depth       uint32
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

	var flags uint32
	if cfg.SQPOLL {
		flags |= giouring.SetupSQPoll
	}

	ring := giouring.NewRing()
	if err := ring.QueueInit(depth, flags); err != nil {
		return nil, fmt.Errorf("iosched: io_uring_setup: %w", err)
	}

	s := &URingScheduler{
		ring:   ring,
		depth:  depth,
		wakeup: make(chan struct{}, 1),
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

	closing bool
}

func (s *URingScheduler) loop() {
	defer s.wg.Done()
	defer s.ring.QueueExit()

	c := coordinator{
		sched:     s,
		ring:      s.ring,
		inflight:  make([]inflightEntry, s.depth),
		freeSlots: make([]int, s.depth),
	}
	for i := range s.depth {
		c.freeSlots[i] = int(i)
	}

	var batch *Ticket
	for {
		batch = c.collect(batch)
		before := c.nInflight
		if c.closing {
			c.failTicketList(batch, s.shutdown.err)
			batch = nil
			c.failStaged(s.shutdown.err)
		} else {
			batch = c.fillSlots(batch)
		}
		if placed := c.nInflight - before; placed > 0 {
			s.stats.syscalls.Add(1)
			s.stats.opsPlaced.Add(uint64(placed))
		}

		if c.nInflight == 0 {
			if c.closing {
				return
			}
			continue
		}

		if err := c.submit(); err != nil {
			ringErr := fmt.Errorf("iosched: ring error: %w", err)
			c.failAllInflight(ringErr)
			s.stop(ringErr)
			c.failTicketList(batch, ringErr)
			c.failStaged(ringErr)
			return
		}
		c.reap()

		select {
		case <-s.shutdown.stop:
			c.closing = true
		default:
		}
	}
}

func (c *coordinator) collect(batch *Ticket) *Ticket {
	s := c.sched
	if batch != nil {
		select {
		case <-s.shutdown.stop:
			c.closing = true
		default:
		}
		return appendTickets(batch, reverseTickets(s.stagingHead.Swap(nil)))
	}

	staged := s.stagingHead.Swap(nil)
	if staged != nil {
		select {
		case <-s.shutdown.stop:
			c.closing = true
		default:
		}
		return reverseTickets(staged)
	}

	if c.nInflight == 0 {
		select {
		case <-s.wakeup:
		case <-s.shutdown.stop:
			c.closing = true
		}
	} else {
		select {
		case <-s.wakeup:
		case <-s.shutdown.stop:
			c.closing = true
		default:
		}
	}

	return reverseTickets(s.stagingHead.Swap(nil))
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

func (c *coordinator) fillSlots(head *Ticket) *Ticket {
	for head != nil {
		t := head
		need := int(t.pending.Load())
		if need > int(c.sched.depth) {
			head = t.next
			t.next = nil
			err := fmt.Errorf("iosched: linked chain length %d exceeds ring depth %d", need, c.sched.depth)
			c.failTicket(t, err)
			continue
		}
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
		c.closing = true
		return
	}
	for op := &t.Op; op != nil; op = op.linked {
		slot := c.freeSlots[len(c.freeSlots)-1]
		c.freeSlots = c.freeSlots[:len(c.freeSlots)-1]

		sqe := c.ring.GetSQE()
		if err := buildutil.Assert(sqe != nil); err != nil {
			err = fmt.Errorf(
				"iosched: GetSQE returned nil; slot=%d nInflight=%d freeSlots=%d depth=%d: %w",
				slot, c.nInflight, len(c.freeSlots)+1, c.sched.depth, err,
			)
			c.freeSlots = append(c.freeSlots, slot)
			c.failAllInflight(err)
			c.failTicket(t, err)
			c.sched.stop(err)
			c.closing = true
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
	default:
		panic(fmt.Sprintf("iosched: invalid opcode %d", op.opcode))
	}
	if op.sqeFlags&sqeLink != 0 && op.linked != nil {
		sqe.SetFlags(uint32(giouring.SqeIOLink))
	}
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

func (c *coordinator) reap() {
	var count uint32
	c.ring.ForEachCQE(func(cqe *giouring.CompletionQueueEvent) {
		count++
		slot := int(cqe.GetData64())
		e := c.inflight[slot]
		c.releaseSlot(slot)

		if cqe.Res < 0 {
			e.op.Result.Err = syscall.Errno(-cqe.Res)
		} else {
			e.op.Result.N = int(cqe.Res)
		}
		completeTicket(e.t)
	})
	if count > 0 {
		c.ring.CQAdvance(count)
	}
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
		if e.op.Result.Err == nil {
			e.op.Result.Err = err
		}
		c.releaseSlot(i)
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
	for op := &t.Op; op != nil; op = op.linked {
		if op.Result.Err == nil {
			op.Result.Err = err
		}
	}
	if n := t.pending.Swap(0); n > 0 {
		t.wg.Done()
	}
}
