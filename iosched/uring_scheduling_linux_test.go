//go:build linux

package iosched

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/v2/ringo"
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
		if !c.sched.config.disk.admit(request) {
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

var benchmarkURingRequest *submission

func BenchmarkURingSubmissionState(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		request := prepareForTest(Op{})
		ticket := Ticket{&request.completion}
		benchmarkURingRequest = request
		request.finish()
		benchmarkTicket = ticket
	}
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

func TestCloseCancellationReportsSchedulerClosed(t *testing.T) {
	c := newTestCoordinator(t, 2, 0)
	stuck, _ := blockingRead(t)
	tickets, _ := acceptOps(c, ReadOp(stuck, make([]byte, 8), 0))
	c.placeReady(false)
	require.NoError(t, c.enter(0))
	c.sched.signalShutdown(errSchedulerClosed)
	require.NoError(t, c.ring.CancelAll(shutdownCancelTimeout))
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })

	_, err := tickets[0].Wait()
	require.ErrorIs(t, err, errSchedulerClosed)
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
	require.True(t, s.config.disk.admit(request))
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

func TestFileDependenciesCloseDrainsOlderRead(t *testing.T) {
	c := newTestCoordinator(t, 4, 0)
	f := testFile(t, 4)
	_, handles := acceptOps(c,
		ReadOp(f, make([]byte, 1), 0),
		DrainOp(f),
	)

	if handles[1].waitCount != 1 {
		t.Fatal("close did not wait for older read")
	}
	completeForTest(c, handles[0], 0, 1, nil)
	if handles[1].state != workReady {
		t.Fatal("close did not become ready after read")
	}
	completeForTest(c, handles[1], 0, 0, nil)
}

func TestFileDependenciesCloseAtEndOfChainDrainsOnlyOlderWork(t *testing.T) {
	c := newTestCoordinator(t, 4, 0)
	f := testFile(t, 4)
	_, handles := acceptOps(c,
		ReadOp(f, make([]byte, 1), 0),
		WriteOp(f, make([]byte, 1), 1).Link(
			WriteOp(f, make([]byte, 1), 2),
			DrainOp(f),
		),
	)

	chain := handles[1]
	if chain.waitCount != 1 || chain.state == workReady {
		t.Fatal("close chain did not wait exactly for older work")
	}
	if got := c.files.regular[f].closeRemaining; got != 1 {
		t.Fatalf("close drain counted its own chain: got %d older operations want 1", got)
	}

	lateRoot := ReadOp(f, make([]byte, 1), 3)
	late, err := c.sched.Submit(lateRoot)
	require.NoError(t, err)
	c.accept(c.sched.takeStaged())
	_, err = late.Wait()
	if err == nil {
		t.Fatal("work submitted behind a close at the end of a chain was accepted")
	}

	completeForTest(c, handles[0], 0, 1, nil)
	if handles[1].state != workReady {
		t.Fatal("close chain did not become ready after older work completed")
	}
	completeForTest(c, handles[1], 0, 1, nil)
	completeForTest(c, handles[1], 1, 1, nil)
	completeForTest(c, handles[1], 2, 0, nil)
}

func TestFileDependenciesRejectWorkBehindClose(t *testing.T) {
	c := newTestCoordinator(t, 4, 0)
	f := testFile(t, 4)
	tickets, handles := acceptOps(c,
		DrainOp(f),
		ReadOp(f, make([]byte, 1), 0),
	)

	if len(handles) != 1 {
		t.Fatalf("accepted work: got %d want 1", len(handles))
	}
	_, err := tickets[1].Wait()
	if err == nil {
		t.Fatal("work submitted behind close was accepted")
	}
	completeForTest(c, handles[0], 0, 0, nil)
}

// TestCoalescedDurableWritesShareSyncError coalesces two durable writes. Each
// keeps its own byte count, and both report the error of the one fdatasync
// they share. The writes are delivered by hand; the fdatasync is real and
// fails because virtual slot 0 holds no file.
func TestCoalescedDurableWritesShareSyncError(t *testing.T) {
	c := newTestCoordinator(t, 4, 1)
	tickets, handles := acceptOps(c,
		VWriteOp(0, make([]byte, 4), 0).Durable(),
		VWriteOp(0, make([]byte, 4), 4).Durable(),
	)
	issueWriteGroupForTest(c, handles...)
	c.finishWrite(handles[0], 8, nil)

	c.placeReady(false)
	require.Equal(t, 1, c.occupied, "the two writes should share one fdatasync")
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	for _, ticket := range tickets {
		n, err := ticket.Wait()
		require.ErrorIs(t, err, syscall.EBADF)
		require.Equal(t, 4, n)
	}
}

// TestDurableWritePreservesCountsOnRingFailureAfterWrite fails a durable write
// whose write completed while it waited for its fdatasync. The ticket keeps
// the byte count the write reported.
func TestDurableWritePreservesCountsOnRingFailureAfterWrite(t *testing.T) {
	ringErr := errors.New("ring failed")
	c := newTestCoordinator(t, 2, 0)
	f := testFile(t, 4)
	tickets, _ := acceptOps(c, WriteOp(f, make([]byte, 4), 0).Durable())
	c.placeReady(false)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	require.Len(t, c.syncPending, 1, "the write should be waiting for its fdatasync")

	c.failRemaining(nil, ringErr, ringErr)
	n, err := tickets[0].Wait()
	if n != 4 || !errors.Is(err, ringErr) {
		t.Fatalf("result: got N=%d error=%v, want N=4 error=%v", n, err, ringErr)
	}
}

func TestFileTableReusesRegularState(t *testing.T) {
	var files fileTable
	firstFile := new(os.File)
	firstOp := ReadOp(firstFile, nil, 0)
	first := files.state(&firstOp.operation)
	files.removeIfEmpty(&firstOp.operation, first)

	secondFile := new(os.File)
	secondOp := ReadOp(secondFile, nil, 0)
	second := files.state(&secondOp.operation)
	if second != first {
		t.Fatal("regular file state was not reused")
	}
}

// withDefaultBudget enables admission limits, which test coordinators leave off.
func withDefaultBudget(c *coordinator) *coordinator {
	c.sched.config.disk = NewDiskPolicy()
	return c
}

func TestBudgetQueuesWritesAndServesReads(t *testing.T) {
	c := withDefaultBudget(newTestCoordinator(t, 16, 0))
	f := testFile(t, 3<<20)
	tickets, works := acceptOps(c, WriteOp(f, make([]byte, 1<<20), 0),
		WriteOp(f, make([]byte, 1<<20), 1<<20), ReadOp(f, make([]byte, 4096), 0))
	require.Zero(t, c.sched.config.disk.Stats().Writes.ServiceTime)
	placed, _ := c.placeReady(false)
	require.Equal(t, 2, placed)
	require.Equal(t, workReady, works[1].state)
	require.Equal(t, workIssued, works[2].state, "read bypasses a queued write")
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	placed, _ = c.placeReady(false)
	require.Equal(t, 1, placed)
	submitAndReapUntil(t, c, func() bool { return c.accepted.len == 0 })
	for _, ticket := range tickets {
		_, err := ticket.Wait()
		require.NoError(t, err)
	}
	require.Equal(t, DiskStats{}, c.sched.config.disk.Stats())
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
	require.EqualValues(t, 4*handles[0].writes.serviceTime, c.sched.config.disk.Stats().Writes.ServiceTime)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	require.Zero(t, c.sched.config.disk.Stats().Writes.ServiceTime)
	for _, ticket := range tickets {
		_, err := ticket.Wait()
		require.NoError(t, err)
		require.Positive(t, ticket.Slat, "every coalesced member records ring placement")
		require.Positive(t, ticket.Clat)
	}
}

func TestBudgetReleaseOfUnplacedWork(t *testing.T) {
	c := withDefaultBudget(newTestCoordinator(t, 4, 1))
	_, handles := acceptOps(c, VReadOp(0, make([]byte, 4096), 0))
	require.Zero(t, c.sched.config.disk.Stats().Reads.ServiceTime)
	completeForTest(c, handles[0], 0, 4096, nil)
	require.Zero(t, c.sched.config.disk.Stats().Reads.ServiceTime)
}

func TestNewURingSchedulerWithZeroBudget(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring not available")
	}
	for _, option := range []DiskPolicyOption{WithLatencyGoal(0), WithLatencyGoal(-1), WithoutIOBudget()} {
		s, err := NewURingScheduler(diskOptions(option))
		require.NoError(t, err)
		require.Zero(t, s.config.disk.device.reads.latency)
		require.Zero(t, s.config.disk.device.writes.latency)
		require.NoError(t, s.Close())
	}
}

func TestBudgetChargesOpenFollowersUntilTheirCompletion(t *testing.T) {
	c := withDefaultBudget(newTestCoordinator(t, 16, 1))
	_, works := acceptOps(c,
		VOpenatOp(0, "unused", 0, 0, 0).Link(VWriteOp(0, make([]byte, 1<<20), 0)),
	)
	require.Positive(t, works[0].writes.serviceTime)
	issueForTest(c, works[0]) // model CQEs without accessing the fake path
	completeForTest(c, works[0], 0, 0, nil)
	require.Positive(t, c.sched.config.disk.Stats().Writes.ServiceTime, "open CQE released its follower's write charge")
	completeForTest(c, works[0], 1, 1<<20, nil)
	require.Zero(t, c.sched.config.disk.Stats().Writes.ServiceTime)
}

func TestVirtualOperationsHaveNoLifecycleBarriers(t *testing.T) {
	c := newTestCoordinator(t, 8, 1)
	tickets, works := acceptOps(c,
		VOpenatOp(0, "unused", 0, 0, 0),
		VReadOp(0, make([]byte, 1), 0),
		VCloseOp(0),
		VOpenatOp(0, "another", 0, 0, 0),
	)
	require.Len(t, works, len(tickets))
	for _, work := range works {
		require.Equal(t, workReady, work.state)
		require.Zero(t, work.waitCount)
	}
	for _, work := range works {
		completeForTest(c, work, 0, 0, nil)
	}
}

func TestShutdownReportsCoordinatorFailure(t *testing.T) {
	c := newTestCoordinator(t, 2, 0)
	cause := errors.New("ring failed")
	require.ErrorIs(t, c.shutdown(cause), cause)
}

// Unbudgeted reads remain admissible while the write allowance is full.
func TestUnbudgetedReadsPassFullWriteAllowance(t *testing.T) {
	c := newTestCoordinator(t, 16, 0, diskOptions(WithLatencyGoal(DefaultLatencyGoal), WithoutReadIOBudget()))
	f := testFile(t, 5<<20)
	mib := make([]byte, 1<<20)
	ops := []Op{WriteOp(f, mib, 0)}
	for i := range 5 {
		ops = append(ops, ReadOp(f, make([]byte, 1<<20), int64(i<<20)))
	}
	tickets, handles := acceptOps(c, ops...)
	placed, _ := c.placeReady(false)
	require.Equal(t, 6, placed)
	for _, h := range handles {
		require.Equal(t, workIssued, h.state)
	}
	require.Zero(t, c.sched.config.disk.Stats().Reads.ServiceTime)
	require.Positive(t, c.sched.config.disk.Stats().Writes.ServiceTime)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	for _, ticket := range tickets {
		_, err := ticket.Wait()
		require.NoError(t, err)
	}
}
