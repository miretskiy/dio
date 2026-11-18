package iosched

import (
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/ringo"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// newTestCoordinator returns a coordinator over a real ring with no
// coordinator goroutine: the test drives acceptance, placement, submission and
// reaping itself, and blocking files decide when operations complete. Cleanup
// cancels and drains whatever is still in flight, fails the remaining tickets
// and closes the ring.
func newTestCoordinator(t *testing.T, depth int, vfiles uint32, opts ...Option) *coordinator {
	t.Helper()
	if !IOUringAvailable {
		t.Skip("io_uring not available")
	}
	options := []ringo.Option{ringo.WithDepth(uint32(depth))}
	if vfiles > 0 {
		options = append(options, ringo.WithFixedFiles(vfiles))
	}
	ring, err := ringo.New(options...)
	require.NoError(t, err)
	fixedFiles := make([]ringo.FixedFile, vfiles)
	for i := range fixedFiles {
		fixedFiles[i], err = ring.FixedFiles().File(uint32(i))
		require.NoError(t, err)
	}
	cfg := makeSchedulerConfig(append([]Option{diskOptions(WithoutIOBudget()), WithVFiles(vfiles)}, opts...))
	// Manually driven tests use the ring's own depth, below the scheduler's
	// minimum, and reserve no doorbell entry.
	cfg.ringDepth = uint32(ring.Capacity())
	c := newCoordinator(&URingScheduler{
		config:     cfg,
		ring:       ring,
		fixedFiles: fixedFiles,
		doorbellFD: -1,
		wakeup:     make(chan struct{}, 1),
	})
	t.Cleanup(func() { closeTestCoordinator(t, c) })
	return c
}

func closeTestCoordinator(t *testing.T, c *coordinator) {
	if c.sched.drainErr != nil {
		// The test abandoned the ring with operations in flight, as a drain
		// that gives up does; there is nothing left to wait for.
		return
	}
	_ = c.ring.CancelAll(shutdownCancelTimeout)
	require.NoError(t, c.drain(false))
	c.failRemaining(c.sched.closeStaging(), errSchedulerClosed, errSchedulerClosed)
	require.NoError(t, c.ring.Close())
}

// submitAndReapUntil hands placed operations to the kernel and applies
// completions until done reports true. The caller must have arranged for the
// completions it waits for to arrive.
func submitAndReapUntil(t *testing.T, c *coordinator, done func() bool) {
	t.Helper()
	for !done() {
		require.NoError(t, c.enter(1))
		c.reap()
	}
}

// testFile returns a temporary file of size bytes, all zero, open for reading
// and writing.
func testFile(t *testing.T, size int) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "data"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	require.NoError(t, f.Truncate(int64(size)))
	return f
}

// issueForTest marks accepted work issued without placing it, so a test can
// deliver its completions itself.
func issueForTest(c *coordinator, work *submission) {
	if work.state != workIssued && !c.sched.config.disk.dispatch(work, &c.inFlight) {
		panic("test exceeded dispatch budget")
	}
	switch work.state {
	case workReady:
		c.ready.remove(work)
	case workWaiting:
	}
	work.state = workIssued
}

// issueWriteGroupForTest marks works issued as one coalesced run led by the
// first, as placeCoalescedRun does, without placing anything. The kernel cannot
// be made to write short, so the test delivers the run's completion itself.
func issueWriteGroupForTest(c *coordinator, works ...*submission) {
	for _, work := range works {
		issueForTest(c, work)
	}
	for i, work := range works[1:] {
		works[i].staged = work
	}
}

// acceptOps stages ops in order, has c accept them, and returns their tickets
// and the submissions c accepted, in acceptance order.
func acceptOps(c *coordinator, ops ...Op) ([]Ticket, []*submission) {
	tickets := make([]Ticket, len(ops))
	var head, tail *submission
	for i := range ops {
		request, err := c.sched.prepareSubmission(&ops[i])
		if err != nil {
			panic(err)
		}
		if err := c.sched.config.disk.admit(request); err != nil {
			panic(ErrRejected)
		}
		tickets[i] = Ticket{&request.completion}
		if head == nil {
			head = request
		} else {
			tail.staged = request
		}
		tail = request
	}
	c.accept(head)
	var accepted []*submission
	for work := c.accepted.head; work != nil; work = c.accepted.next(work) {
		accepted = append(accepted, work)
	}
	return tickets, accepted
}

func TestStagingClosePartitionsConcurrentPushes(t *testing.T) {
	const count = 256
	stopErr := errors.New("stopped")
	var s URingScheduler
	requests := make([]submission, count)
	accepted := make([]bool, count)

	accepted[0] = s.tryPush(&requests[0])
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 1; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			accepted[i] = s.tryPush(&requests[i])
		}()
	}
	close(start)
	s.signalShutdown(stopErr)
	batch := s.closeStaging()
	wg.Wait()

	seen := make(map[*submission]bool, count)
	for request := batch; request != nil; request = request.staged {
		if seen[request] {
			t.Fatal("staged operation appeared more than once")
		}
		seen[request] = true
	}
	for i := range requests {
		if accepted[i] != seen[&requests[i]] {
			t.Fatalf("operation %d: accepted=%v staged=%v", i, accepted[i], seen[&requests[i]])
		}
	}
	if s.tryPush(new(submission)) {
		t.Fatal("push succeeded after staging closed")
	}
	if !errors.Is(s.stopCause(), stopErr) {
		t.Fatalf("stop cause: got %v want %v", s.stopCause(), stopErr)
	}
}

func TestSubmitRejectsInvalidVirtualFileBeforeStaging(t *testing.T) {
	for _, tc := range []struct {
		name   string
		vfiles uint32
		vfd    uint32
		op     func(uint32) Op
	}{
		{name: "read table disabled", vfiles: 0, vfd: 0, op: func(vfd uint32) Op {
			return VReadOp(vfd, make([]byte, 1), 0)
		}},
		{name: "read index out of range", vfiles: 2, vfd: 2, op: func(vfd uint32) Op {
			return VReadOp(vfd, make([]byte, 1), 0)
		}},
		{name: "open table disabled", vfiles: 0, vfd: 0, op: func(vfd uint32) Op {
			return VOpenatOp(0, "file", 0, 0, vfd)
		}},
		{name: "open index out of range", vfiles: 2, vfd: 2, op: func(vfd uint32) Op {
			return VOpenatOp(0, "file", 0, 0, vfd)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &URingScheduler{
				config: schedulerConfig{ringDepth: minRingDepth, vfiles: tc.vfiles, disk: &DiskPolicy{}},
			}
			_, err := s.Submit(tc.op(tc.vfd))
			require.Error(t, err)
			require.Nil(t, s.stagingHead.Load())
		})
	}
}

// TestChainKeepsEachLinkType checks that every edge of a chain reaches the
// kernel with its own link type, by failing a read in the middle and watching
// which writes after it land: Link cancels the rest of the chain, HardLink
// carries on. Each chain mixes both types, so swapped edges change the result.
func TestChainKeepsEachLinkType(t *testing.T) {
	s := newURingForDoorbellTest(t, WithRingDepth(8))
	defer func() { require.NoError(t, s.Close()) }()
	f := testFile(t, 4)
	writeOnly, err := os.OpenFile(f.Name(), os.O_WRONLY, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, writeOnly.Close()) }()
	failingRead := ReadOp(writeOnly, make([]byte, 1), 0) // EBADF

	for _, chain := range []Op{
		// Link, then HardLink: A lands, the read fails, B still runs.
		WriteOp(f, []byte("A"), 0).Link(failingRead).HardLink(WriteOp(f, []byte("B"), 1)),
		// HardLink, then Link: the read fails, C still runs, and so does D.
		failingRead.HardLink(WriteOp(f, []byte("C"), 2)).Link(WriteOp(f, []byte("D"), 3)),
	} {
		ticket, err := s.Submit(chain)
		require.NoError(t, err)
		_, err = waitTicket(t, ticket, 5*time.Second)
		require.ErrorIs(t, err, syscall.EBADF)
	}
	got := make([]byte, 4)
	_, err = f.ReadAt(got, 0)
	require.NoError(t, err)
	require.Equal(t, "ABCD", string(got))
}

func TestRunStopsWithoutPlacingSubmissionAcceptedBeforeClose(t *testing.T) {
	c := newTestCoordinator(t, 2, 1)
	s := c.sched
	op := VReadOp(0, make([]byte, 1), 0)
	request := prepareForTest(op)
	ticket := Ticket{&request.completion}
	require.NoError(t, s.config.disk.admit(request))
	if !s.tryPush(request) {
		t.Fatal("submission was rejected before close")
	}
	s.signalShutdown(errSchedulerClosed)

	cause := c.run()
	if !errors.Is(cause, errSchedulerClosed) {
		t.Fatalf("run error: got %v want %v", cause, errSchedulerClosed)
	}
	staged := s.closeStaging()
	require.NoError(t, c.drain(false))
	c.failRemaining(staged, cause, cause)

	_, err := ticket.Wait()
	if !errors.Is(err, errSchedulerClosed) {
		t.Fatalf("ticket error: got %v want %v", err, errSchedulerClosed)
	}
	require.Zero(t, c.occupied, "shutdown placed work")
	if s.tryPush(new(submission)) {
		t.Fatal("submission succeeded after close")
	}
}

// TestDrainGivesUpAndReportsUnknownOutcome covers a ring that stops reporting
// completions while operations are still placed. The coordinator must stop
// waiting instead of spinning forever, must not release the placed operations,
// and must tell whoever holds their tickets that the outcome is unknown.
func TestDrainGivesUpAndReportsUnknownOutcome(t *testing.T) {
	oldTimeout := shutdownTimeout
	shutdownTimeout = 20 * time.Millisecond
	defer func() { shutdownTimeout = oldTimeout }()

	c := newTestCoordinator(t, 4, 0)
	first, _ := blockingRead(t)
	second, _ := blockingRead(t)
	released := false
	op := ReadOp(first, make([]byte, 8), 0)
	op.whenDone = func(int, error) { released = true }
	tickets, _ := acceptOps(c, op, ReadOp(second, make([]byte, 8), 0))
	c.placeReady(false)
	require.NoError(t, c.enter(0))
	require.Equal(t, 2, c.occupied, "operations were not placed")

	// Closing the ring under the coordinator leaves it with operations in
	// flight that can never be reaped: the ring has stopped reporting.
	require.ErrorIs(t, c.ring.Close(), ringo.ErrPending)
	err := c.drain(false)
	require.ErrorIs(t, err, ringo.ErrClosed)
	c.sched.drainErr = err
	require.Equal(t, 2, c.occupied,
		"drain released operations it could not prove had finished")

	c.failRemaining(nil, errSchedulerClosed, err)
	require.False(t, released, "forced shutdown must retain a buffer whose I/O was not reaped")
	for i, ticket := range tickets {
		_, waitErr := ticket.Wait()
		require.ErrorIsf(t, waitErr, ringo.ErrClosed, "ticket %d", i)
	}
}

// TestDrainOutcomeOverridesEarlierOperationError is the intersection of the two
// cases above: one operation of a chain reported a failure, another never
// reported at all, and the drain gave up. The ticket must say the outcome is
// unknown, because the earlier failure reads as settled and would tell its
// holder the buffers are free.
func TestDrainOutcomeOverridesEarlierOperationError(t *testing.T) {
	oldTimeout := shutdownTimeout
	shutdownTimeout = 20 * time.Millisecond
	defer func() { shutdownTimeout = oldTimeout }()

	c := newTestCoordinator(t, 4, 0)
	f := testFile(t, 1)
	writeOnly, err := os.OpenFile(f.Name(), os.O_WRONLY, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, writeOnly.Close()) }()
	stuck, _ := blockingRead(t)
	tickets, _ := acceptOps(c,
		ReadOp(writeOnly, make([]byte, 1), 0).HardLink(ReadOp(stuck, make([]byte, 8), 0)),
	)
	c.placeReady(false)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 1 }) // the EBADF read

	require.ErrorIs(t, c.ring.Close(), ringo.ErrPending)
	drainErr := c.drain(false)
	require.ErrorIs(t, drainErr, ringo.ErrClosed)
	c.sched.drainErr = drainErr

	c.failRemaining(nil, errSchedulerClosed, drainErr)
	_, waitErr := tickets[0].Wait()
	require.ErrorIs(t, waitErr, ringo.ErrClosed)
	require.NotErrorIs(t, waitErr, syscall.EBADF,
		"an unknown outcome was reported as a settled failure")
}

// completeForTest delivers the completion of work's index'th operation without
// placing it.
func completeForTest(c *coordinator, work *submission, index uint32, n int, err error) {
	issueForTest(c, work)
	op := work.op(int(index))
	c.finishOperation(work, op, n, err)
}

func TestFailRemainingPreservesCompletedRoot(t *testing.T) {
	err := errors.New("ring failed")
	c := newTestCoordinator(t, 2, 1)
	tickets, handles := acceptOps(c, VReadOp(0, nil, 0).Link(VReadOp(0, nil, 0)))
	ticket := tickets[0]

	completeForTest(c, handles[0], 0, 1, nil)
	c.failRemaining(nil, err, err)
	n, gotErr := ticket.Wait()

	if n != 1 {
		t.Fatalf("completed root count changed: %d", n)
	}
	if !errors.Is(gotErr, err) {
		t.Fatalf("ticket error: got %v want %v", gotErr, err)
	}
}

func TestShutdownReapsCancellationBeforeFailingUnplacedWork(t *testing.T) {
	cause := errors.New("ring failed")
	c := newTestCoordinator(t, 1, 0)
	f, _ := blockingRead(t)
	released := false
	op := ReadOp(f, make([]byte, 8), 0)
	op.whenDone = func(_ int, err error) {
		require.ErrorIs(t, err, syscall.ECANCELED)
		released = true
	}
	tickets, _ := acceptOps(c, op, ReadOp(f, make([]byte, 8), 0))
	c.placeReady(false)
	require.Equal(t, 1, c.occupied, "the second read should not fit")
	require.NoError(t, c.enter(0))

	require.ErrorIs(t, c.shutdown(cause), cause)
	_, err := tickets[0].Wait()
	require.ErrorIs(t, err, syscall.ECANCELED)
	require.True(t, released, "the kernel completion must release the I/O reference")
	_, err = tickets[1].Wait()
	require.ErrorIs(t, err, cause, "unplaced work reports the scheduler failure")
	require.Zero(t, c.accepted.len)
	require.Zero(t, c.occupied)
}

func TestShutdownCancelsBlockedOperation(t *testing.T) {
	oldTimeout := shutdownTimeout
	shutdownTimeout = 100 * time.Millisecond
	defer func() { shutdownTimeout = oldTimeout }()
	c := newTestCoordinator(t, 2, 0)
	stuck, _ := blockingRead(t)
	completed := false
	op := ReadOp(stuck, make([]byte, 8), 0)
	op.whenDone = func(_ int, err error) {
		require.ErrorIs(t, err, errSchedulerClosed)
		completed = true
	}
	tickets, _ := acceptOps(c, op)
	c.placeReady(false)
	require.NoError(t, c.enter(0))

	start := time.Now()
	require.NoError(t, c.shutdown(errSchedulerClosed))
	require.Less(t, time.Since(start), time.Second)
	_, err := tickets[0].Wait()
	require.ErrorIs(t, err, errSchedulerClosed)
	require.True(t, completed, "cancellation completion must release the caller's resources")
	require.Zero(t, c.accepted.len)
	require.Zero(t, c.occupied)
}

// withDefaultBudget enables admission limits, which test coordinators leave off.
func withDefaultBudget(c *coordinator) *coordinator {
	c.sched.config.disk = NewDiskPolicy()
	return c
}

func TestBudgetDispatchesOneOversizedOperation(t *testing.T) {
	c := withDefaultBudget(newTestCoordinator(t, 16, 0))
	f := testFile(t, 4<<20)
	big := make([]byte, 2<<20)
	tickets, works := acceptOps(c, WriteOp(f, big, 0), WriteOp(f, big, 2<<20))
	for i := range tickets {
		placed, _ := c.placeReady(true)
		require.Equal(t, 1, placed, "coalescing cannot enlarge the oversized-request exception")
		require.Equal(t, workIssued, works[i].state)
		submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
		_, err := tickets[i].Wait()
		require.NoError(t, err)
	}
	require.Equal(t, DiskStats{}, c.sched.config.disk.Stats())
}

// TestBudgetChargesCoalescedRun charges a coalesced run the service time of all its
// members and returns it when the run's write completes.
func TestBudgetChargesCoalescedRun(t *testing.T) {
	c := withDefaultBudget(newTestCoordinator(t, 16, 0))
	f := testFile(t, 4*4096)
	ops := make([]Op, 4)
	for i := range ops {
		ops[i] = WriteOp(f, make([]byte, 4096), int64(i*4096))
	}
	tickets, handles := acceptOps(c, ops...)
	placed, _ := c.placeReady(true)
	require.Equal(t, 1, placed)
	require.EqualValues(t, 4*handles[0].writes.inFlight, c.sched.config.disk.Stats().Writes.InFlight)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	require.Zero(t, c.sched.config.disk.Stats().Writes.InFlight)
	for _, ticket := range tickets {
		_, err := ticket.Wait()
		require.NoError(t, err)
		require.Positive(t, ticket.Slat, "every coalesced member records ring placement")
		require.Positive(t, ticket.Clat)
	}
}

func TestBudgetChargesOpenFollowersUntilTheirCompletion(t *testing.T) {
	c := withDefaultBudget(newTestCoordinator(t, 16, 1))
	_, works := acceptOps(c,
		VOpenatOp(0, "unused", 0, 0, 0).Link(VWriteOp(0, make([]byte, 1<<20), 0)),
	)
	require.Positive(t, works[0].writes.inFlight)
	issueForTest(c, works[0]) // model CQEs without accessing the fake path
	completeForTest(c, works[0], 0, 0, nil)
	require.Positive(t, c.sched.config.disk.Stats().Writes.InFlight, "open CQE released its follower's write charge")
	completeForTest(c, works[0], 1, 1<<20, nil)
	require.Zero(t, c.sched.config.disk.Stats().Writes.InFlight)
}

// Placement is in acceptance order: a head that exceeds its class's in-flight
// allowance ends the pass, holding the work behind it, and dispatches once its
// ring's own I/O of that class completes.
func TestPlaceReadyStopsAtHeadOverAllowance(t *testing.T) {
	c := newTestCoordinator(t, 16, 0, diskOptions(WithLatencyGoal(DefaultLatencyGoal), WithoutReadIOBudget()))
	f := testFile(t, 4<<20)
	mib := make([]byte, 1<<20)
	ops := make([]Op, 4)
	for i := range ops {
		ops[i] = WriteOp(f, mib, int64(i)<<20)
	}
	tickets, _ := acceptOps(c, append(ops, ReadOp(f, make([]byte, 4096), 0))...)
	placed, room := c.placeReady(false)
	require.Equal(t, 1, placed, "a second 1 MiB write exceeds the 1.5 ms allowance")
	require.True(t, room)
	require.Equal(t, 4, c.ready.len, "the read waits behind the blocked write")
	for c.accepted.len != 0 {
		c.placeReady(false)
		require.NoError(t, c.enter(1))
		c.reap()
	}
	for _, ticket := range tickets {
		_, err := ticket.Wait()
		require.NoError(t, err)
	}
	require.Zero(t, c.ready.len)
	require.Equal(t, DiskStats{}, c.sched.config.disk.Stats())
}

func TestStandaloneReadRetriesCompletionEAGAIN(t *testing.T) {
	for _, vectored := range []bool{false, true} {
		t.Run(map[bool]string{false: "read", true: "readv"}[vectored], func(t *testing.T) {
			policy := NewDiskPolicy(WithLatencyGoal(time.Millisecond), WithReadBacklogLimit(time.Second))
			c := newTestCoordinator(t, 4, 0, WithDiskPolicy(policy))
			payload := []byte("read survives completion EAGAIN")
			f := testFile(t, len(payload))
			_, err := f.WriteAt(payload, 0)
			require.NoError(t, err)
			buffer := make([]byte, len(payload))
			op := ReadOp(f, buffer, 0)
			if vectored {
				op = ReadvOp(f, [][]byte{buffer[:7], buffer[7:]}, 0)
			}
			callbacks := 0
			op.whenDone = func(n int, err error) {
				callbacks++
				require.NoError(t, err)
				require.Equal(t, len(payload), n)
			}
			tickets, works := acceptOps(c, op, DrainOp(f))
			work := works[0]
			backlog := policy.readBudget.backlog.Load()
			var firstSlat time.Duration
			for attempt := range 2 {
				require.True(t, policy.dispatch(work, &c.inFlight))
				c.take(work)
				work.markIssued()
				if attempt == 0 {
					firstSlat = work.Slat
				}
				c.finishOperation(work, &work.root, 0, syscall.EAGAIN)
				require.Equal(t, workReady, work.state)
				require.EqualValues(t, 1, work.remaining)
				require.Equal(t, backlog, policy.readBudget.backlog.Load())
				require.Zero(t, policy.readBudget.inFlight.Load())
				require.Zero(t, c.inFlight.reads)
				require.EqualValues(t, 1, works[1].waitCount, "Drain must keep waiting for the retried read")
				require.Zero(t, callbacks, "the I/O buffer must stay owned until the final completion")
				select {
				case <-tickets[0].Done():
					t.Fatal("ticket completed before the retry")
				default:
				}
			}
			c.placeReady(false)
			submitAndReapUntil(t, c, func() bool { return work.state == workDone })
			n, err := tickets[0].Wait()
			require.NoError(t, err)
			require.Equal(t, len(payload), n)
			require.Equal(t, payload, buffer)
			require.Equal(t, 1, callbacks)
			require.Equal(t, firstSlat, tickets[0].Slat, "retry time belongs in completion latency")
			require.Zero(t, policy.readBudget.backlog.Load())
			require.Zero(t, policy.readBudget.inFlight.Load())
			c.placeReady(false)
			submitAndReapUntil(t, c, func() bool { return works[1].state == workDone })
			_, err = tickets[1].Wait()
			require.NoError(t, err)
		})
	}
}

func TestCompletionEAGAINNotRetriedForWritesChainsOrShutdown(t *testing.T) {
	for _, kind := range []string{"write", "linked-read", "shutdown"} {
		t.Run(kind, func(t *testing.T) {
			c := newTestCoordinator(t, 4, 0)
			f := testFile(t, 8)
			op := ReadOp(f, make([]byte, 8), 0)
			switch kind {
			case "write":
				op = WriteOp(f, make([]byte, 8), 0)
			case "linked-read":
				op = op.HardLink(ReadOp(f, make([]byte, 8), 0))
			}
			callbacks := 0
			op.whenDone = func(_ int, err error) { callbacks++; require.ErrorIs(t, err, syscall.EAGAIN) }
			tickets, works := acceptOps(c, op)
			work := works[0]
			require.True(t, c.sched.config.disk.dispatch(work, &c.inFlight))
			c.take(work)
			if kind == "shutdown" {
				c.sched.signalShutdown(errSchedulerClosed)
			}
			c.finishOperation(work, &work.root, 0, syscall.EAGAIN)
			for i := 1; i < work.opCount(); i++ {
				c.finishOperation(work, work.op(i), 8, nil)
			}
			_, err := tickets[0].Wait()
			require.ErrorIs(t, err, syscall.EAGAIN)
			require.Equal(t, 1, callbacks)
			require.Zero(t, c.ready.len)
			require.Zero(t, c.accepted.len)
		})
	}
}

// newTestDoorbell gives c's scheduler a real doorbell eventfd, so tests can
// observe whether Submit rang it.
func newTestDoorbell(t *testing.T, c *coordinator) {
	t.Helper()
	doorbell, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, unix.Close(doorbell)) })
	c.sched.doorbellFD = doorbell
}

// doorbellCount reads and resets the doorbell counter without blocking; zero
// means nobody rang it.
func doorbellCount(t *testing.T, s *URingScheduler) uint64 {
	t.Helper()
	fds := []unix.PollFd{{Fd: int32(s.doorbellFD), Events: unix.POLLIN}}
	n, err := unix.Poll(fds, 0)
	for err == unix.EINTR { // the Go runtime's preemption signal
		n, err = unix.Poll(fds, 0)
	}
	require.NoError(t, err)
	if n == 0 {
		return 0
	}
	var buf [8]byte
	_, err = unix.Read(s.doorbellFD, buf[:])
	require.NoError(t, err)
	return binary.NativeEndian.Uint64(buf[:])
}

// waitTicket fails the test instead of hanging if the ticket does not
// complete in time.
func waitTicket(t *testing.T, ticket Ticket, timeout time.Duration) (int, error) {
	t.Helper()
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := ticket.Wait()
		done <- result{n, err}
	}()
	select {
	case r := <-done:
		return r.n, r.err
	case <-time.After(timeout):
		t.Fatalf("ticket did not complete within %v", timeout)
		return 0, nil
	}
}

// TestSleepWithoutRoomLeavesDoorbellQuiet covers a coordinator that sleeps
// because the head of the ready list does not fit. New work would queue behind
// that head, so the coordinator must not ask for the doorbell, and a Submit
// during the sleep must not write it.
func TestSleepWithoutRoomLeavesDoorbellQuiet(t *testing.T) {
	c := newTestCoordinator(t, 2, 0)
	newTestDoorbell(t, c)
	s := c.sched
	first, releaseFirst := blockingRead(t)
	second, _ := blockingRead(t)
	acceptOps(c,
		ReadOp(first, make([]byte, 8), 0),
		ReadOp(second, make([]byte, 8), 0),
		ReadOp(second, make([]byte, 8), 0),
	)
	placed, room := c.placeReady(false)
	require.Equal(t, 1, c.ready.len, "the third read should be waiting for an entry")
	require.False(t, room, "placement reported room in a full ring")

	slept := make(chan error, 1)
	go func() { slept <- c.submitAndWait(placed, room) }()
	_, err := s.Submit(ReadOp(second, make([]byte, 8), 0))
	require.NoError(t, err)
	require.Equal(t, wakeNone, s.wake.Load(), "asked to be woken with no room to place work")
	require.Zero(t, doorbellCount(t, s), "Submit rang the doorbell with no room")

	releaseFirst()
	select {
	case err := <-slept:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("sleep did not end on the completion")
	}
	c.reap()
}

// TestManyInFlightLeavesDoorbellQuiet covers a coordinator that waits with
// doorbellMaxInFlight operations in flight: the next completion is due soon,
// so it does not ask to be woken, and a Submit does not ring.
func TestManyInFlightLeavesDoorbellQuiet(t *testing.T) {
	c := newTestCoordinator(t, 2*doorbellMaxInFlight, 0)
	newTestDoorbell(t, c)
	s := c.sched
	c.armDoorbell() // as run does; it is not counted as an operation in flight
	stuck, release := blockingRead(t)
	reads := make([]Op, doorbellMaxInFlight)
	for i := range reads {
		reads[i] = ReadOp(stuck, make([]byte, 8), 0)
	}
	acceptOps(c, reads...)
	placed, room := c.placeReady(false)
	require.Zero(t, c.ready.len, "every read should fit")

	waited := make(chan error, 1)
	go func() { waited <- c.submitAndWait(placed, room) }()
	_, err := s.Submit(ReadOp(stuck, make([]byte, 8), 0))
	require.NoError(t, err)
	require.Equal(t, wakeNone, s.wake.Load(), "asked to be woken with many operations in flight")
	require.Zero(t, doorbellCount(t, s), "Submit rang the doorbell with many operations in flight")

	release()
	select {
	case err := <-waited:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the wait did not end on the completion")
	}
	c.reap()
}

func TestSubmitRingsSleepingCoordinatorOnce(t *testing.T) {
	c := newTestCoordinator(t, 4, 1)
	newTestDoorbell(t, c)
	s := c.sched
	s.wake.Store(wakeDoorbell)

	_, err := s.Submit(VReadOp(0, make([]byte, 1), 0))
	require.NoError(t, err)
	require.Equal(t, wakeNone, s.wake.Load(), "the ringing Submit did not clear the request")
	require.EqualValues(t, 1, doorbellCount(t, s), "Submit did not ring the sleeping coordinator")

	_, err = s.Submit(VReadOp(0, make([]byte, 1), 0))
	require.NoError(t, err)
	require.Zero(t, doorbellCount(t, s), "a second Submit rang again during the same sleep")
}

// TestSleepRechecksStagingAfterPublishingSleep covers the lost-wakeup window:
// a Submit that pushed before the coordinator asked to be woken did not wake
// it, so the coordinator must find that work instead of waiting.
func TestSleepRechecksStagingAfterPublishingSleep(t *testing.T) {
	c := newTestCoordinator(t, 4, 1)
	newTestDoorbell(t, c)
	s := c.sched
	c.armDoorbell() // only the doorbell in the ring: a wait would park
	request := prepareForTest(VReadOp(0, make([]byte, 1), 0))
	require.NoError(t, s.config.disk.admit(request))
	require.True(t, s.tryPush(request)) // pushed, but no doorbell

	slept := make(chan error, 1)
	go func() { slept <- c.submitAndWait(0, true) }() // nothing ready: new work would fit
	select {
	case err := <-slept:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		s.wakeChannel() // end the wait it should not have entered
		<-slept
		t.Fatal("slept with staged work")
	}
	require.Equal(t, wakeNone, s.wake.Load())
}

// TestIdleCoordinatorParksOnChannel checks that with nothing but the doorbell
// in the ring the coordinator parks on the wakeup channel, and that a Submit
// then wakes it through the channel without writing the doorbell.
func TestIdleCoordinatorParksOnChannel(t *testing.T) {
	c := newTestCoordinator(t, 4, 1)
	newTestDoorbell(t, c)
	s := c.sched
	c.armDoorbell()

	parked := make(chan error, 1)
	go func() { parked <- c.submitAndWait(0, true) }()
	waitUntilWaiting(t, s, wakeChannel)
	_, err := s.Submit(VReadOp(0, make([]byte, 1), 0))
	require.NoError(t, err)
	select {
	case err := <-parked:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Submit did not wake the parked coordinator")
	}
	require.Zero(t, doorbellCount(t, s), "Submit rang the doorbell for a parked coordinator")
}

// TestRunStopsWhenDoorbellReadFails covers a doorbell that stops working
// while I/O is in flight: new work could no longer wake the coordinator from
// io_uring_enter, so run must return. A doorbell read on a descriptor opened
// only for writing fails with EBADF. An idle coordinator parks on the wakeup
// channel instead, so a stuck read keeps it in the ring.
func TestRunStopsWhenDoorbellReadFails(t *testing.T) {
	c := newTestCoordinator(t, 4, 0)
	writeOnly, err := os.OpenFile(filepath.Join(t.TempDir(), "doorbell"), os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	defer func() { require.NoError(t, writeOnly.Close()) }()
	c.sched.doorbellFD = int(writeOnly.Fd())
	stuck, _ := blockingRead(t)
	request := prepareForTest(ReadOp(stuck, make([]byte, 8), 0))
	require.NoError(t, c.sched.config.disk.admit(request))
	require.True(t, c.sched.tryPush(request))

	err = c.run()
	require.ErrorIs(t, err, syscall.EBADF)
	require.ErrorContains(t, err, "doorbell read")
}

// blockingRead returns a file whose reads complete only when the test writes
// to it: a blocking eventfd, standing in for I/O that is still in flight.
func blockingRead(t *testing.T) (*os.File, func()) {
	t.Helper()
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	require.NoError(t, err)
	file := os.NewFile(uintptr(fd), "blocking-read")
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	release := func() {
		_, err := file.Write(doorbellIncrement[:])
		require.NoError(t, err)
	}
	return file, release
}

// waitUntilWaiting waits for the coordinator to wait for new work, asking
// Submit to wake it the way want says.
func waitUntilWaiting(t *testing.T, s *URingScheduler, want uint32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for s.wake.Load() != want {
		if time.Now().After(deadline) {
			t.Fatal("coordinator did not go to sleep")
		}
		time.Sleep(100 * time.Microsecond)
	}
}

func newURingForDoorbellTest(t *testing.T, opts ...Option) *URingScheduler {
	t.Helper()
	if !IOUringAvailable {
		t.Skip("io_uring not available")
	}
	s, err := NewURingScheduler(append([]Option{diskOptions(WithoutIOBudget())}, opts...)...)
	require.NoError(t, err)
	return s
}

// TestURingSubmitIsIssuedWhileOtherIOIsInFlight is the doorbell's purpose: the
// coordinator sleeps in io_uring_enter with I/O that will not complete, and new
// work must still be issued rather than wait for that I/O. Each round's wakeup
// completes the doorbell read, so later rounds also check that it is armed
// again.
func TestURingSubmitIsIssuedWhileOtherIOIsInFlight(t *testing.T) {
	s := newURingForDoorbellTest(t, WithRingDepth(16))
	stuck, release := blockingRead(t)
	stuckBuf := make([]byte, 8)
	stuckTicket, err := s.Submit(ReadOp(stuck, stuckBuf, 0))
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(t.TempDir(), "data"))
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	data := make([]byte, 4096)
	for round := range 3 {
		waitUntilWaiting(t, s, wakeDoorbell)
		writeTicket, err := s.Submit(WriteOp(f, data, int64(round*len(data))))
		require.NoError(t, err)
		n, err := waitTicket(t, writeTicket, 5*time.Second)
		require.NoErrorf(t, err, "round %d", round)
		require.Equalf(t, len(data), n, "round %d", round)
	}

	release()
	n, err := waitTicket(t, stuckTicket, 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, 8, n)
	require.NoError(t, s.Close())
}

func TestURingCloseCancelsIOThatNeverCompletes(t *testing.T) {
	s := newURingForDoorbellTest(t, WithRingDepth(16))
	stuck, _ := blockingRead(t)
	ticket, err := s.Submit(ReadOp(stuck, make([]byte, 8), 0))
	require.NoError(t, err)
	waitUntilWaiting(t, s, wakeDoorbell)

	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
	_, err = waitTicket(t, ticket, time.Second)
	require.ErrorIs(t, err, errSchedulerClosed)
	require.Equal(t, -1, s.doorbellFD, "Close did not close the doorbell")
}

// TestURingConcurrentSubmittersAreAllServed races many submitters against a
// coordinator that keeps going back to sleep with I/O in flight, so every
// wakeup goes through the doorbell.
func TestURingConcurrentSubmittersAreAllServed(t *testing.T) {
	const (
		writers = 8
		writes  = 500
		block   = 4096
	)
	s := newURingForDoorbellTest(t, WithRingDepth(32))
	stuck, release := blockingRead(t)
	stuckTicket, err := s.Submit(ReadOp(stuck, make([]byte, 8), 0))
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(t.TempDir(), "data"))
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, block)
			for i := range writes {
				if rand.IntN(4) == 0 {
					time.Sleep(time.Duration(rand.IntN(50)) * time.Microsecond)
				}
				ticket, err := s.Submit(WriteOp(f, buf, int64((w*writes+i)*block)))
				if err != nil {
					errs <- err
					return
				}
				if n, err := ticket.Wait(); err != nil || n != block {
					errs <- errors.Join(err, errors.New("short or failed write"))
					return
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("submitters were not all served")
	}
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	release()
	_, err = waitTicket(t, stuckTicket, 5*time.Second)
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

func TestURingSmallRingDepthKeepsFullChainLimit(t *testing.T) {
	s := newURingForDoorbellTest(t, WithRingDepth(4))
	defer func() { require.NoError(t, s.Close()) }()
	require.GreaterOrEqual(t, s.config.ringDepth, uint32(minRingDepth))
	f, err := os.Create(filepath.Join(t.TempDir(), "data"))
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	buf := make([]byte, 4096)
	chain := func(n int) Op {
		rest := make([]Op, n-1)
		for i := range rest {
			rest[i] = WriteOp(f, buf, int64(i+1)*4096)
		}
		return WriteOp(f, buf, 0).Link(rest...)
	}

	ticket, err := s.Submit(chain(maxChainLength))
	require.NoError(t, err)
	_, err = waitTicket(t, ticket, 5*time.Second)
	require.NoError(t, err)

	_, err = s.Submit(chain(maxChainLength + 1))
	require.ErrorContains(t, err, "exceeds the limit of 8")
}

func TestSubmitDoorbellFailureCancelsAsynchronously(t *testing.T) {
	s := newURingForDoorbellTest(t)
	blocked, _ := blockingRead(t)
	first, err := s.Submit(ReadOp(blocked, make([]byte, 8), 0))
	require.NoError(t, err)
	waitUntilWaiting(t, s, wakeDoorbell)
	// The coordinator is in io_uring_enter. Make the submitting goroutine's
	// doorbell write fail; recovery must cancel the blocking read and join
	// before ring teardown. The registered read still holds the original fd.
	fd := s.doorbellFD
	s.doorbellFD = -1
	second, err := s.Submit(ReadOp(blocked, make([]byte, 8), 0))
	require.NoError(t, err)
	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("doorbell failure did not stop the coordinator")
	}
	s.doorbellFD = fd
	require.ErrorIs(t, s.Close(), syscall.EBADF)
	_, err = first.Wait()
	require.ErrorIs(t, err, syscall.ECANCELED)
	_, err = second.Wait()
	require.ErrorIs(t, err, syscall.EBADF)
	select {
	case <-s.cancelDone:
	default:
		t.Fatal("Close did not join cancellation")
	}
}

// diskOptions gives a scheduler its own policy built from opts.
func diskOptions(opts ...DiskPolicyOption) Option { return WithDiskPolicy(NewDiskPolicy(opts...)) }

func TestURingDMABuffersCallerOwnership(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	buffer := alloc.NewMemory(4096)
	released := false
	defer func() {
		if !released {
			buffer.Unpin()
		}
	}()
	s, err := NewURingScheduler(WithRingDepth(8), WithDMABuffers(buffer.Bytes()))
	require.NoError(t, err)
	closed := false
	defer func() {
		if !closed {
			require.NoError(t, s.Close())
		}
	}()
	require.Equal(t, 1, s.registeredBuffers.Len())
	require.Empty(t, s.config.dmaBuffers)
	err = s.Close()
	closed = true
	require.NoError(t, err)
	require.Nil(t, s.registeredBuffers)
	// Scheduler shutdown must neither release the caller's reference nor leave
	// behind an extra reference. The caller's one Unpin releases the buffer.
	require.True(t, buffer.TryInc())
	buffer.Unpin()
	buffer.Bytes()[0] = 42
	buffer.Unpin()
	released = true
	require.False(t, buffer.TryInc())
}

func TestURingFixedIOWithExplicitBuffers(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	const chunk = 4096
	allocator := alloc.NewAllocator(7 * chunk)
	defer allocator.Close()
	var err error
	var buffers []alloc.Memory
	defer func() {
		for _, b := range buffers {
			b.Unpin()
		}
	}()
	held := make([]alloc.Memory, 7)
	released := make([]bool, len(held))
	defer func() {
		for i, b := range held {
			if !released[i] {
				b.Unpin()
			}
		}
	}()
	for i := range held {
		held[i], err = allocator.TryAcquire(chunk)
		require.NoError(t, err)
	}
	// Register individual allocations without including their neighbors.
	for _, i := range []int{0, 2, 4} {
		held[i].Unpin()
		released[i] = true
	}
	for range 3 {
		buffer, err := allocator.TryAcquire(chunk)
		require.NoError(t, err)
		buffers = append(buffers, buffer)
	}
	for range 3 {
		buffers = append(buffers, alloc.NewMemory(chunk))
	}
	regions := make([][]byte, len(buffers))
	for i, b := range buffers {
		regions[i] = b.Bytes()
	}
	s, err := NewURingScheduler(WithRingDepth(8), WithDMABuffers(regions...))
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	require.Equal(t, len(buffers), s.registeredBuffers.Len())
	f, err := os.CreateTemp(t.TempDir(), "explicit-buffers")
	require.NoError(t, err)
	defer f.Close()
	_, err = s.Submit(WriteFixedOp(f, held[1].Bytes(), 0))
	require.ErrorContains(t, err, "outside the registered DMA buffers")
	for i, buffer := range buffers {
		data := buffer.Bytes()
		for j := range data {
			data[j] = byte(i + 1)
		}
		ticket, err := s.Submit(WriteFixedOp(f, data, int64(i*chunk)))
		require.NoError(t, err)
		n, err := ticket.Wait()
		require.NoError(t, err)
		require.Equal(t, chunk, n)
	}
	for i, buffer := range buffers {
		data := buffer.Bytes()
		clear(data)
		ticket, err := s.Submit(ReadFixedOp(f, data, int64(i*chunk)))
		require.NoError(t, err)
		n, err := ticket.Wait()
		require.NoError(t, err)
		require.Equal(t, chunk, n)
		for _, v := range data {
			require.Equal(t, byte(i+1), v)
		}
	}
}

// A stream larger than the entire allocator reuses one caller-owned registered buffer.
func TestURingAllocatorBufferFixedIO(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring not available on this kernel")
	}
	const chunkSize = 128 << 10 // the whole arena: one span
	allocator := alloc.NewAllocator(chunkSize)
	defer allocator.Close()
	buf, err := allocator.TryAcquire(chunkSize)
	require.NoError(t, err)
	defer buf.Unpin()
	bufferSize := len(buf.Bytes())
	s, err := NewURingScheduler(WithRingDepth(8), WithDMABuffers(buf.Bytes()))
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	f, err := os.CreateTemp(t.TempDir(), "fixed")
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	_, err = allocator.TryAcquire(align.BlockSize)
	require.ErrorIs(t, err, alloc.ErrExhausted)
	payload := make([]byte, 3*chunkSize+100)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	for off := 0; off < len(payload); off += bufferSize {
		n := min(bufferSize, len(payload)-off)
		piece := buf.Bytes()[:int(align.PageAlign(int64(n)))]
		clear(piece)
		copy(piece, payload[off:off+n])
		ticket, err := s.Submit(WriteFixedOp(f, piece, int64(off)))
		require.NoError(t, err)
		written, err := ticket.Wait()
		require.NoError(t, err)
		require.Equal(t, len(piece), written)
	}
	for off := 0; off < len(payload); off += bufferSize {
		n := min(bufferSize, len(payload)-off)
		piece := buf.Bytes()[:int(align.PageAlign(int64(n)))]
		clear(piece)
		ticket, err := s.Submit(ReadFixedOp(f, piece, int64(off)))
		require.NoError(t, err)
		read, err := ticket.Wait()
		require.NoError(t, err)
		require.Equal(t, len(piece), read)
		require.Equal(t, payload[off:off+n], piece[:n])
	}
}

func TestSchedulersHaveIndependentSlotsAndBarriers(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	var scheds []*URingScheduler
	for range 2 {
		s, err := NewURingScheduler(WithVFiles(1), WithRingDepth(16))
		require.NoError(t, err)
		scheds = append(scheds, s)
		t.Cleanup(func() { require.NoError(t, s.Close()) })
	}
	wait := func(ticket Ticket, err error) {
		t.Helper()
		require.NoError(t, err)
		select {
		case <-ticket.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("ticket did not complete")
		}
		_, err = ticket.Wait()
		require.NoError(t, err)
	}
	dir := t.TempDir()
	paths := []string{filepath.Join(dir, "a"), filepath.Join(dir, "b")}
	for i, s := range scheds {
		wait(s.Submit(VOpenatOp(unix.AT_FDCWD, paths[i], unix.O_CREAT|unix.O_RDWR, 0600, 0)))
	}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock) // release before scheduler cleanup, including on failure
	tail := []byte("old")
	first, err := SubmitNotify(scheds[0], VWriteOp(0, []byte("aaa"), 0), func(_ int, _ error) {
		entered <- struct{}{}
		<-release
		copy(tail, "new")
	})
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("callback did not run")
	}
	// This close-containing chain must wait for the earlier callback, not
	// merely the write CQE. Its buffer is changed by that callback before issue.
	seal, err := scheds[0].Submit(VWriteOp(0, tail, 3).Link(VFdatasyncOp(0), VCloseOp(0)))
	require.NoError(t, err)
	// Another ring progresses while the first ring's callback is held.
	wait(scheds[1].Submit(VWriteOp(0, []byte("bbb"), 0)))
	unblock()
	wait(first, nil)
	wait(seal, nil)
	got, err := os.ReadFile(paths[0])
	require.NoError(t, err)
	require.Equal(t, "aaanew", string(got))
	buf := make([]byte, 3)
	wait(scheds[1].Submit(VReadOp(0, buf, 0).Link(VCloseOp(0))))
	require.Equal(t, "bbb", string(buf), "slot zero belongs to a different file on each ring")
	// Closing one ring's slot leaves the same index reusable on that ring.
	wait(scheds[0].Submit(VOpenatOp(unix.AT_FDCWD, paths[1], unix.O_RDONLY, 0, 0).Link(VReadOp(0, buf, 0))))
	require.Equal(t, "bbb", string(buf))
	wait(scheds[0].Submit(VCloseOp(0)))
}

// A pinned coordinator runs completions on its CPU; an unavailable CPU only logs.
func TestCoordinatorCPU(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	var allowed unix.CPUSet
	require.NoError(t, unix.SchedGetaffinity(0, &allowed))
	cpu := 0
	for !allowed.IsSet(cpu) {
		cpu++
	}
	for _, requested := range []int{cpu, int(^uint(0) >> 1)} {
		s, err := NewURingScheduler(WithCoordinatorCPU(requested))
		require.NoError(t, err)
		f, err := os.CreateTemp(t.TempDir(), "pinned")
		require.NoError(t, err)
		var mask unix.CPUSet
		var maskErr error
		ticket, err := SubmitNotify(s, WriteOp(f, []byte("pinned"), 0), func(int, error) {
			maskErr = unix.SchedGetaffinity(0, &mask) // Callbacks run on the coordinator.
		})
		require.NoError(t, err)
		_, err = ticket.Wait()
		require.NoError(t, err)
		require.NoError(t, maskErr)
		if requested == cpu {
			require.Equal(t, 1, mask.Count())
			require.True(t, mask.IsSet(cpu))
		}
		require.NoError(t, s.Close())
		require.NoError(t, f.Close())
	}
}

func TestResultStorageAllocations(t *testing.T) {
	f := new(os.File)
	s := URingScheduler{config: makeSchedulerConfig(nil)}
	standalone := WriteOp(f, nil, 0)
	linked := standalone.Link(ReadOp(f, nil, 0))
	for _, tc := range []struct {
		name   string
		op     Op
		allocs float64
	}{
		{"standalone", standalone, 1},
		{"durable", standalone.Durable(), 1},
		{"linked", linked, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(100, func() {
				op := tc.op
				work, err := s.prepareSubmission(&op)
				if err != nil {
					panic(err)
				}
				resultStorageTicket = Ticket{&work.completion}
			})
			require.Equal(t, tc.allocs, allocs, "linked outcomes share the submission allocation")
		})
	}
	resultStorageTicket = Ticket{}
}

// Ringo can reject a caller's arguments before it queues any SQEs. The ticket
// must finish and release dio's accounting and reserved slot on that path.
func TestPlacementFailureReleasesChain(t *testing.T) {
	c := newTestCoordinator(t, 32, 1)
	op := VOpenatOp(unix.AT_FDCWD, "invalid\x00path", os.O_CREATE|os.O_RDWR, 0600, AllocateVFD).Link(
		VWriteOp(AllocateVFD, []byte("data"), 0), VCloseOp(AllocateVFD))
	ticket, err := c.sched.Submit(op)
	require.NoError(t, err)
	c.accept(c.sched.takeStaged())
	c.placeReady(false)
	_, err = ticket.Wait()
	require.Error(t, err)
	for _, result := range ticket.AllResults() {
		require.Error(t, result.Err)
	}
	require.Zero(t, c.accepted.len)
	require.Zero(t, c.occupied)
	require.Zero(t, c.sched.config.disk.Stats().Writes.Backlog)
	require.Zero(t, c.sched.config.disk.Stats().Writes.Backlog)
	require.EqualValues(t, 0, c.allocVFD(), "failed placement returns the reserved slot")
	c.freeVFD(0)
}
