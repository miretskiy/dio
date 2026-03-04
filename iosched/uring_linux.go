//go:build linux

package iosched

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/pawelgaczynski/giouring"
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

// reqOp identifies the operation type for an in-flight io_uring request.
type reqOp uint8

const (
	opRead  reqOp = iota
	opWrite reqOp = iota
)

// uringReq is a single in-flight I/O request. Pooled to avoid allocation per call.
type uringReq struct {
	fd     int
	buf    []byte
	offset int64
	op     reqOp
	n      int
	err    error
	done   chan struct{}
}

var errSchedulerClosed = errors.New("iosched: scheduler closed")

// URingScheduler is an asynchronous IOScheduler backed by io_uring.
//
// Architecture: a single coordinator goroutine owns the ring exclusively.
// Callers append requests to a mutex-protected queue and signal via a
// buffered(1) channel. The coordinator runs a sliding-window loop:
// drain pending→fill free SQ slots→SubmitAndWait(1)→reap CQEs→repeat.
// This keeps the ring as full as possible, maximizing NVMe queue depth.
type URingScheduler struct {
	ioLatency

	mu      sync.Mutex
	pending []*uringReq

	signal  chan struct{} // buffered(1): wakes the coordinator
	reqPool sync.Pool

	// stopCh is closed to signal shutdown (by Close or on fatal ring error).
	stopCh   chan struct{}
	stopOnce sync.Once

	// fatalErr stores the first non-recoverable ring error.
	// All subsequent ReadAt/WriteAt calls return this error immediately.
	fatalErr atomic.Pointer[error]

	wg sync.WaitGroup

	// Stats — written only by the coordinator goroutine.
	batches  atomic.Int64
	requests atomic.Int64
	maxBatch atomic.Int64
}

// Stats returns a snapshot of scheduler statistics.
func (s *URingScheduler) Stats() Stats {
	batches := s.batches.Load()
	reqs := s.requests.Load()
	var avg float64
	if batches > 0 {
		avg = float64(reqs) / float64(batches)
	}
	return Stats{
		ReadLatency:  s.readSnapshot(),
		WriteLatency: s.writeSnapshot(),
		Batches:      batches,
		Requests:     reqs,
		MaxBatch:     s.maxBatch.Load(),
		AvgBatch:     avg,
	}
}

// NewURingScheduler creates an io_uring-backed scheduler.
// Returns an error if io_uring is unavailable or ring setup fails.
func NewURingScheduler(cfg URingConfig) (*URingScheduler, error) {
	if !IOUringAvailable {
		return nil, errors.New("iosched: io_uring not available on this kernel")
	}

	s := &URingScheduler{
		signal: make(chan struct{}, 1),
		stopCh: make(chan struct{}),
	}
	s.initLatency()
	s.reqPool.New = func() any {
		return &uringReq{done: make(chan struct{}, 1)}
	}

	depth := cfg.RingDepth
	if depth < defaultRingDepth {
		depth = defaultRingDepth
	}

	readyCh := make(chan error, 1)
	s.wg.Add(1)
	go s.loop(depth, cfg.SQPOLL, readyCh)

	if err := <-readyCh; err != nil {
		return nil, err
	}
	return s, nil
}

// ReadAt submits a positioned read to the io_uring ring and blocks until
// the kernel completes it. buf must remain valid until ReadAt returns.
func (s *URingScheduler) ReadAt(fd int, buf []byte, offset int64) (int, error) {
	if err := s.fatalError(); err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}

	start := time.Now()
	req := s.getReq(fd, buf, offset, opRead)
	s.enqueue(req)

	n, err := s.wait(req)
	s.putReq(req)
	s.recordRead(start)

	// Prevent GC from collecting buf before the kernel has finished with it.
	runtime.KeepAlive(buf)
	return n, err
}

// WriteAt submits a positioned write to the io_uring ring and blocks until
// the kernel completes it. buf must remain valid until WriteAt returns.
func (s *URingScheduler) WriteAt(fd int, buf []byte, offset int64) (int, error) {
	if err := s.fatalError(); err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}

	start := time.Now()
	req := s.getReq(fd, buf, offset, opWrite)
	s.enqueue(req)

	n, err := s.wait(req)
	s.putReq(req)
	s.recordWrite(start)

	runtime.KeepAlive(buf)
	return n, err
}

// Close shuts down the coordinator goroutine and releases the ring resources.
func (s *URingScheduler) Close() error {
	s.stop()
	s.wg.Wait()
	return nil
}

func (s *URingScheduler) enqueue(req *uringReq) {
	s.mu.Lock()
	s.pending = append(s.pending, req)
	s.mu.Unlock()

	// Non-blocking wakeup: if signal is already pending, no-op.
	select {
	case s.signal <- struct{}{}:
	default:
	}
}

func (s *URingScheduler) wait(req *uringReq) (int, error) {
	select {
	case <-req.done:
	case <-s.stopCh:
		// Coordinator stopped while we were waiting; drain may have filled req.done.
		select {
		case <-req.done:
		default:
			return 0, errSchedulerClosed
		}
	}
	return req.n, req.err
}

func (s *URingScheduler) getReq(fd int, buf []byte, offset int64, op reqOp) *uringReq {
	req := s.reqPool.Get().(*uringReq)
	req.fd = fd
	req.buf = buf
	req.offset = offset
	req.op = op
	req.n = 0
	req.err = nil
	return req
}

func (s *URingScheduler) putReq(req *uringReq) {
	*req = uringReq{done: req.done}
	s.reqPool.Put(req)
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

// ─── Coordinator ────────────────────────────────────────────────────────────

// coordinator encapsulates the io_uring sliding-window event loop.
// All fields are owned exclusively by the coordinator goroutine.
type coordinator struct {
	sched *URingScheduler
	ring  *giouring.Ring

	// inflight maps ring slot index → in-flight request (nil = free slot).
	inflight []*uringReq
	// freeSlots is a LIFO stack of available slot indices.
	freeSlots []int
	nInflight int

	// batch holds requests collected from the pending queue.
	batch []*uringReq
	// nSubmitted counts SQEs prepared since the last SubmitAndWait.
	nSubmitted int
}

// loop is the coordinator goroutine; it exclusively owns the ring.
//
// Sliding-window: fill free SQ slots → SubmitAndWait(1) → reap CQEs → repeat.
// The ring stays as full as possible, keeping the storage pipeline saturated.
func (s *URingScheduler) loop(depth uint32, sqpoll bool, readyCh chan<- error) {
	defer s.wg.Done()

	var flags uint32
	if sqpoll {
		flags |= giouring.SetupSQPoll
	}

	ring := giouring.NewRing()
	if err := ring.QueueInit(depth, flags); err != nil {
		readyCh <- fmt.Errorf("iosched: io_uring_setup: %w", err)
		return
	}
	defer ring.QueueExit()

	c := coordinator{
		sched:    s,
		ring:     ring,
		inflight: make([]*uringReq, depth),
		batch:    make([]*uringReq, 0, depth),
	}
	c.freeSlots = make([]int, depth)
	for i := range depth {
		c.freeSlots[i] = int(i)
	}

	readyCh <- nil
	defer c.drainPending()

	for {
		if !c.collect() {
			return
		}
		c.fillSlots()

		if c.nSubmitted == 0 && c.nInflight == 0 {
			// Spurious wakeup — nothing to do.
			continue
		}

		if err := c.submitAndReap(); err != nil {
			s.setFatalErr(fmt.Errorf("iosched: ring error: %w", err))
			s.stop()
			return
		}
	}
}

// collect grabs new requests from the pending queue.
// Blocks when idle; non-blocking when in-flight work exists.
// Returns false on shutdown.
func (c *coordinator) collect() bool {
	if c.nInflight == 0 && len(c.batch) == 0 {
		// Idle: wait for new work or shutdown.
		select {
		case <-c.sched.signal:
		case <-c.sched.stopCh:
			return false
		}
	} else {
		// Busy: non-blocking check for more work.
		select {
		case <-c.sched.signal:
		case <-c.sched.stopCh:
			return false
		default:
			return true
		}
	}

	c.sched.mu.Lock()
	collected := len(c.sched.pending)
	c.batch = append(c.batch, c.sched.pending...)
	c.sched.pending = c.sched.pending[:0]
	c.sched.mu.Unlock()

	if n := int64(collected); n > 0 {
		c.sched.batches.Add(1)
		c.sched.requests.Add(n)
		for {
			cur := c.sched.maxBatch.Load()
			if n <= cur || c.sched.maxBatch.CompareAndSwap(cur, n) {
				break
			}
		}
	}
	return true
}

// fillSlots fills as many free SQ slots as possible from the batch.
func (c *coordinator) fillSlots() {
	for len(c.batch) > 0 && len(c.freeSlots) > 0 {
		req := c.batch[0]
		c.batch = c.batch[1:]

		slot := c.freeSlots[len(c.freeSlots)-1]
		c.freeSlots = c.freeSlots[:len(c.freeSlots)-1]

		sqe := c.ring.GetSQE()
		if sqe == nil {
			// SQ ring unexpectedly full — fail and recycle slot.
			req.err = errors.New("iosched: SQ ring full")
			req.done <- struct{}{}
			c.freeSlots = append(c.freeSlots, slot)
			continue
		}

		ptr := uintptr(unsafe.Pointer(&req.buf[0]))
		size := uint32(len(req.buf))
		off := uint64(req.offset)

		switch req.op {
		case opRead:
			sqe.PrepareRead(req.fd, ptr, size, off)
		case opWrite:
			sqe.PrepareWrite(req.fd, ptr, size, off)
		}
		sqe.SetData64(uint64(slot))

		c.inflight[slot] = req
		c.nInflight++
		c.nSubmitted++
	}
}

// submitAndReap submits pending SQEs, waits for ≥1 completion, and reaps CQEs.
// Returns a non-nil error only for fatal ring failures.
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
			// Fatal: fail all in-flight requests.
			for i, req := range c.inflight {
				if req != nil {
					req.err = fmt.Errorf("iosched: ring error: %w", err)
					req.done <- struct{}{}
					c.inflight[i] = nil
					c.freeSlots = append(c.freeSlots, i)
					c.nInflight--
				}
			}
			return err
		}

		// Reap all ready CQEs.
		var count uint32
		c.ring.ForEachCQE(func(cqe *giouring.CompletionQueueEvent) {
			count++
			slot := int(cqe.GetData64())
			req := c.inflight[slot]
			c.inflight[slot] = nil
			c.freeSlots = append(c.freeSlots, slot)
			c.nInflight--

			if cqe.Res < 0 {
				req.err = syscall.Errno(-cqe.Res)
			} else {
				req.n = int(cqe.Res)
			}
			req.done <- struct{}{}
		})
		if count > 0 {
			c.ring.CQAdvance(count)
		}
		return nil
	}
}

// drainPending fails all in-flight, batched, and pending requests on shutdown.
func (c *coordinator) drainPending() {
	for i, req := range c.inflight {
		if req != nil {
			req.err = errSchedulerClosed
			req.done <- struct{}{}
			c.inflight[i] = nil
		}
	}
	for _, req := range c.batch {
		req.err = errSchedulerClosed
		req.done <- struct{}{}
	}
	c.batch = c.batch[:0]

	c.sched.mu.Lock()
	for _, req := range c.sched.pending {
		req.err = errSchedulerClosed
		req.done <- struct{}{}
	}
	c.sched.pending = c.sched.pending[:0]
	c.sched.mu.Unlock()
}
