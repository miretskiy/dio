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
	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/v2/ringo"
)

// newTestCoordinator returns a coordinator over a real ring with no
// coordinator goroutine: the test drives acceptance, placement, submission and
// reaping itself, and blocking files decide when operations complete. Cleanup
// cancels and drains whatever is still in flight, fails the remaining tickets
// and closes the ring.
func newTestCoordinator(t *testing.T, depth int, vfiles uint32) *coordinator {
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
	c := newCoordinator(&URingScheduler{
		config: schedulerConfig{
			ringDepth: uint32(ring.Capacity()),
			vfiles:    vfiles,
		},
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
	require.NoError(t, c.waitForInflight())
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
	if work.state == workReady {
		c.ready.remove(work)
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
	works[0].coalesced = works
}

// acceptOps stages ops in order, has c accept them, and returns their tickets
// and the submissions c accepted, in acceptance order.
func acceptOps(c *coordinator, ops ...Op) ([]Ticket, []*submission) {
	tickets := make([]Ticket, len(ops))
	var head, tail *submission
	for i := range ops {
		request, ticket := newSubmission(ops[i], int32(ops[i].opCount()))
		tickets[i] = ticket
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
		request, ticket := newSubmission(Op{}, 1)
		benchmarkURingRequest = request
		request.root.done.Done()
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
				config: schedulerConfig{ringDepth: 1, vfiles: tc.vfiles},
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

func TestShutdownCancellationRetriesUntilCoordinatorStops(t *testing.T) {
	done := make(chan struct{})
	attempts := 0
	cancelUntilCoordinatorDone(done, func(timeout time.Duration) error {
		require.Equal(t, shutdownCancelTimeout, timeout)
		attempts++
		if attempts == 2 {
			close(done)
		}
		return syscall.ETIME
	})
	require.Equal(t, 2, attempts)
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
	request, ticket := newSubmission(op, 1)
	if !s.tryPush(request) {
		t.Fatal("submission was rejected before close")
	}
	s.signalShutdown(errSchedulerClosed)

	cause := c.run()
	if !errors.Is(cause, errSchedulerClosed) {
		t.Fatalf("run error: got %v want %v", cause, errSchedulerClosed)
	}
	staged := s.closeStaging()
	require.NoError(t, c.waitForInflight())
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
	defer func(limit int, delay time.Duration) {
		drainStallLimit, drainStallDelay = limit, delay
	}(drainStallLimit, drainStallDelay)
	drainStallLimit, drainStallDelay = 4, time.Microsecond

	c := newTestCoordinator(t, 4, 0)
	first, _ := blockingRead(t)
	second, _ := blockingRead(t)
	tickets, _ := acceptOps(c,
		ReadOp(first, make([]byte, 8), 0),
		ReadOp(second, make([]byte, 8), 0),
	)
	c.placeReady(false)
	require.NoError(t, c.enter(0))
	require.Equal(t, 2, c.occupied, "operations were not placed")

	// Closing the ring under the coordinator leaves it with operations in
	// flight that can never be reaped: the ring has stopped reporting.
	require.ErrorIs(t, c.ring.Close(), ringo.ErrPending)
	err := c.waitForInflight()
	require.ErrorIs(t, err, ringo.ErrClosed)
	c.sched.drainErr = err
	require.Equal(t, 2, c.occupied,
		"drain released operations it could not prove had finished")

	c.failRemaining(nil, errSchedulerClosed, err)
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
	defer func(limit int, delay time.Duration) {
		drainStallLimit, drainStallDelay = limit, delay
	}(drainStallLimit, drainStallDelay)
	drainStallLimit, drainStallDelay = 4, time.Microsecond

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
	drainErr := c.waitForInflight()
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
	op := &work.root
	for i := uint32(0); i < index; i++ {
		op = op.linked
	}
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

func TestCleanupDrainsPlacedCompletionsBeforeFailingUnplacedWork(t *testing.T) {
	ringErr := errors.New("ring failed")
	c := newTestCoordinator(t, 1, 0)
	f := testFile(t, 1)
	tickets, _ := acceptOps(c,
		ReadOp(f, make([]byte, 1), 0),
		ReadOp(f, make([]byte, 1), 0),
	)
	c.placeReady(false)
	require.Equal(t, 1, c.occupied, "the second read should not fit")
	require.NoError(t, c.enter(0))

	require.NoError(t, c.waitForInflight())
	c.failRemaining(nil, ringErr, ringErr)

	n, err := tickets[0].Wait()
	if n != 1 || err != nil {
		t.Fatalf("consumed request: N=%d error=%v", n, err)
	}
	_, err = tickets[1].Wait()
	if !errors.Is(err, ringErr) {
		t.Fatalf("remaining request error: got %v want %v", err, ringErr)
	}
	if c.accepted.len != 0 || c.occupied != 0 {
		t.Fatalf("coordinator retained failed work: pending=%d placed=%d", c.accepted.len, c.occupied)
	}
}

func TestDrainWaitsForFinalCompletionBeforeCompletingTicket(t *testing.T) {
	c := newTestCoordinator(t, 2, 0)
	c.sched.signalShutdown(errSchedulerClosed)
	stuck, _ := blockingRead(t)
	tickets, _ := acceptOps(c, ReadOp(stuck, make([]byte, 8), 0))
	c.placeReady(false)
	require.NoError(t, c.enter(0))

	drained := make(chan error, 1)
	go func() { drained <- c.waitForInflight() }()
	select {
	case <-drained:
		t.Fatal("drain returned before the final completion")
	case <-time.After(20 * time.Millisecond):
	}

	// CancelAll is the one Ring call that may overlap the drain's wait.
	require.NoError(t, c.ring.CancelAll(shutdownCancelTimeout))
	select {
	case err := <-drained:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not return after the final completion")
	}
	if _, err := tickets[0].Wait(); !errors.Is(err, errSchedulerClosed) {
		t.Fatalf("ticket error: got %v want %v", err, errSchedulerClosed)
	}
	if c.accepted.len != 0 || c.occupied != 0 {
		t.Fatalf("coordinator retained drained work: pending=%d placed=%d", c.accepted.len, c.occupied)
	}
}

func TestFileDependenciesOpenBlocksLaterRead(t *testing.T) {
	c := newTestCoordinator(t, 4, 2)
	_, handles := acceptOps(c,
		VOpenatOp(0, "file", 0, 0, 1),
		VReadOp(1, make([]byte, 1), 0),
	)

	if handles[1].waitCount != 1 || handles[1].state == workReady {
		t.Fatal("read did not wait for open")
	}
	waiterCap := cap(c.files.virtual[1].openWaiters)
	require.NotZero(t, waiterCap)
	completeForTest(c, handles[0], 0, 0, nil)
	require.Equal(t, waiterCap, cap(c.files.virtual[1].openWaiters))
	if handles[1].state != workReady {
		t.Fatal("read did not become ready at open completion")
	}
	completeForTest(c, handles[1], 0, 1, nil)
}

func TestFileDependenciesCloseDrainsOlderRead(t *testing.T) {
	c := newTestCoordinator(t, 4, 2)
	_, handles := acceptOps(c,
		VReadOp(1, make([]byte, 1), 0),
		VCloseOp(1),
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
	c := newTestCoordinator(t, 4, 1)
	_, handles := acceptOps(c,
		VReadOp(0, make([]byte, 1), 0),
		VWriteOp(0, make([]byte, 1), 1).Link(
			VWriteOp(0, make([]byte, 1), 2),
			VCloseOp(0),
		),
	)

	chain := handles[1]
	if chain.waitCount != 1 || chain.state == workReady {
		t.Fatal("close chain did not wait exactly for older work")
	}
	if got := c.files.virtual[0].closeRemaining; got != 1 {
		t.Fatalf("close drain counted its own chain: got %d older operations want 1", got)
	}

	lateRoot := VReadOp(0, make([]byte, 1), 3)
	lateRequest, late := newSubmission(lateRoot, int32(lateRoot.opCount()))
	c.accept(lateRequest)
	_, err := late.Wait()
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
	c := newTestCoordinator(t, 4, 1)
	tickets, handles := acceptOps(c,
		VCloseOp(0),
		VReadOp(0, make([]byte, 1), 0),
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

func TestDeferredMultiFileWorkIsKnownAtAdmission(t *testing.T) {
	c := newTestCoordinator(t, 8, 2)
	_, handles := acceptOps(c,
		VOpenatOp(0, "a", 0, 0, 0),
		VReadOp(0, make([]byte, 1), 0).Link(VWriteOp(1, make([]byte, 1), 0)),
		VCloseOp(1),
	)

	if handles[1].waitCount != 1 {
		t.Fatal("linked work did not wait for open")
	}
	if handles[2].waitCount != 1 {
		t.Fatal("close overtook older blocked write")
	}
	completeForTest(c, handles[0], 0, 0, nil)
	completeForTest(c, handles[1], 0, 1, nil)
	if handles[2].state == workReady {
		t.Fatal("close became ready before the linked write completed")
	}
	completeForTest(c, handles[1], 1, 1, nil)
	completeForTest(c, handles[2], 0, 0, nil)
}

func TestOpenBarrierWaitsForWholeLinkedChain(t *testing.T) {
	c := newTestCoordinator(t, 4, 2)
	_, handles := acceptOps(c,
		VOpenatOp(0, "a", 0, 0, 0).Link(
			VFallocateOp(0, 4096),
			VReadOp(1, make([]byte, 1), 0),
		),
		VReadOp(0, make([]byte, 1), 0),
	)

	completeForTest(c, handles[0], 0, 0, nil)
	if handles[1].state == workReady {
		t.Fatal("open dependent escaped after only the open completed")
	}
	completeForTest(c, handles[0], 1, 0, nil)
	if handles[1].state == workReady {
		t.Fatal("open dependent escaped before the whole linked chain completed")
	}
	completeForTest(c, handles[0], 2, 1, nil)
	if handles[1].state != workReady {
		t.Fatal("open dependent did not become ready with the linked chain")
	}
	completeForTest(c, handles[1], 0, 1, nil)
}

func TestFailedOpenRetryWaitsForReleasedSlotWork(t *testing.T) {
	c := newTestCoordinator(t, 4, 1)
	_, handles := acceptOps(c,
		VOpenatOp(0, "a", 0, 0, 0),
		VWriteOp(0, make([]byte, 1), 0),
	)

	completeForTest(c, handles[0], 0, 0, syscall.ENOENT)
	if handles[1].state != workReady {
		t.Fatal("failed open did not release waiting write")
	}

	retryRoot := VOpenatOp(0, "b", 0, 0, 0)
	retryRequest, retry := newSubmission(retryRoot, int32(retryRoot.opCount()))
	c.accept(retryRequest)
	_, err := retry.Wait()
	if err == nil {
		t.Fatal("retry open was accepted while prior slot work remained")
	}

	completeForTest(c, handles[1], 0, 0, syscall.EBADF)
	finalRoot := VOpenatOp(0, "b", 0, 0, 0)
	finalRequest, final := newSubmission(finalRoot, int32(finalRoot.opCount()))
	c.accept(finalRequest)
	if c.accepted.len != 1 {
		t.Fatal("retry open was not accepted after prior slot work completed")
	}
	handle := c.accepted.head
	completeForTest(c, handle, 0, 0, nil)
	_, err = final.Wait()
	if err != nil {
		t.Fatalf("final open failed: %v", err)
	}
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
	first := files.state(&firstOp)
	files.removeIfEmpty(&firstOp, first)

	secondFile := new(os.File)
	secondOp := ReadOp(secondFile, nil, 0)
	second := files.state(&secondOp)
	if second != first {
		t.Fatal("regular file state was not reused")
	}
}

// TestOpenBarrierHoldsSlotWorkUntilChainCompletes accepts a same-slot read
// after the open's chain is already in the ring. The read must stay out of the
// ring while that chain runs, even after the open itself and unrelated work
// complete, and be placed once the chain does.
func TestOpenBarrierHoldsSlotWorkUntilChainCompletes(t *testing.T) {
	c := newTestCoordinator(t, 8, 1)
	want := []byte("opened")
	path := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(path, want, 0o600))
	chainTail, releaseChain := blockingRead(t)
	other, releaseOther := blockingRead(t)
	acceptOps(c,
		VOpenatOp(unix.AT_FDCWD, path, unix.O_RDONLY, 0, 0).Link(ReadOp(chainTail, make([]byte, 8), 0)),
		ReadOp(other, make([]byte, 8), 0),
	)
	c.placeReady(false)
	require.NoError(t, c.enter(0))

	got := make([]byte, len(want))
	readTickets, _ := acceptOps(c, VReadOp(0, got, 0))
	c.placeReady(false)
	require.Equal(t, 3, c.occupied, "same-slot read reached the ring while the open was in flight")

	releaseOther()
	submitAndReapUntil(t, c, func() bool { return c.occupied == 1 }) // the open and the unrelated read
	c.placeReady(false)
	require.Equal(t, 1, c.occupied, "same-slot read reached the ring before the open's chain completed")

	releaseChain()
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	c.placeReady(false)
	require.Equal(t, 1, c.occupied, "same-slot read was not placed after the open's chain completed")
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })

	n, err := readTickets[0].Wait()
	require.NoError(t, err)
	require.Equal(t, len(want), n)
	require.Equal(t, want, got)
}

// TestCloseDrainHoldsCloseUntilSlotWorkCompletes accepts a close behind a
// same-slot read that is still in flight. The close must stay out of the ring
// after unrelated work completes, and be placed once the read does. The slot
// holds a FIFO, so the read completes when the test writes to it.
func TestCloseDrainHoldsCloseUntilSlotWorkCompletes(t *testing.T) {
	c := newTestCoordinator(t, 8, 1)
	fifo := filepath.Join(t.TempDir(), "fifo")
	require.NoError(t, unix.Mkfifo(fifo, 0o600))
	openTickets, _ := acceptOps(c, VOpenatOp(unix.AT_FDCWD, fifo, unix.O_RDWR, 0, 0))
	c.placeReady(false)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	_, err := openTickets[0].Wait()
	require.NoError(t, err)

	other, releaseOther := blockingRead(t)
	got := make([]byte, 4)
	readTickets, _ := acceptOps(c, VReadOp(0, got, 0), ReadOp(other, make([]byte, 8), 0))
	c.placeReady(false)
	require.NoError(t, c.enter(0))
	closeTickets, _ := acceptOps(c, VCloseOp(0))
	c.placeReady(false)
	require.Equal(t, 2, c.occupied, "close reached the ring before the older same-slot read completed")

	releaseOther()
	submitAndReapUntil(t, c, func() bool { return c.occupied == 1 })
	c.placeReady(false)
	require.Equal(t, 1, c.occupied, "unrelated completion released the close")

	// The slot's FIFO is open for reading, so this open does not block.
	writer, err := os.OpenFile(fifo, os.O_WRONLY, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.Close()) }()
	_, err = writer.Write([]byte("ping"))
	require.NoError(t, err)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	c.placeReady(false)
	require.Equal(t, 1, c.occupied, "close was not placed after the same-slot read completed")
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })

	n, err := readTickets[0].Wait()
	require.NoError(t, err)
	require.Equal(t, "ping", string(got[:n]))
	_, err = closeTickets[0].Wait()
	require.NoError(t, err)
}

// TestChainWaitsOnceForAnOpen accepts a chain with two operations on a slot
// whose open is unfinished: the chain waits for the open once, not once per
// operation, and is released when the open's chain completes.
func TestChainWaitsOnceForAnOpen(t *testing.T) {
	c := newTestCoordinator(t, 4, 1)
	_, handles := acceptOps(c,
		VOpenatOp(0, "file", 0, 0, 0),
		VReadOp(0, make([]byte, 1), 0).Link(VReadOp(0, make([]byte, 1), 1)),
	)
	require.EqualValues(t, 1, handles[1].waitCount)
	require.Len(t, c.files.virtual[0].openWaiters, 1)

	completeForTest(c, handles[0], 0, 0, nil)
	require.Equal(t, workReady, handles[1].state, "the chain was not released by the open")
	completeForTest(c, handles[1], 0, 1, nil)
	completeForTest(c, handles[1], 1, 1, nil)
}

// withDefaultBudget gives a test coordinator the scheduler's default in-flight
// budget, which newTestCoordinator leaves off.
func withDefaultBudget(c *coordinator) *coordinator {
	c.sched.config.budget = defaultBudget()
	return c
}

// TestBudgetHoldsWritesNotReads queues more writes than the write budget
// allows, then a read. The read is placed past the held writes, and the next
// write is placed once the first one completes and returns its cost.
func TestBudgetHoldsWritesNotReads(t *testing.T) {
	c := withDefaultBudget(newTestCoordinator(t, 16, 0))
	f := testFile(t, 3<<20)
	mib := make([]byte, 1<<20)
	tickets, handles := acceptOps(c,
		WriteOp(f, mib, 0),
		WriteOp(f, mib, 1<<20),
		WriteOp(f, mib, 2<<20),
		ReadOp(f, make([]byte, 4096), 0),
	)
	// A 1 MiB write costs 0.86 ms of the 1.5 ms goal, so only one fits.
	placed, room := c.placeReady(false)
	require.Equal(t, 2, placed, "want the first write and the read")
	require.Equal(t, workIssued, handles[0].state)
	require.Equal(t, workReady, handles[1].state)
	require.Equal(t, workReady, handles[2].state)
	require.Equal(t, workIssued, handles[3].state, "the read waited for the write budget")
	require.True(t, room, "reads still have budget, so new work could be placed")

	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	require.Equal(t, [budgetedClasses]int64{}, c.inFlightCost, "completed work kept its cost")
	placed, _ = c.placeReady(false)
	require.Equal(t, 1, placed, "want the second write alone")
	require.Equal(t, workIssued, handles[1].state)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	c.placeReady(false)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	for i, ticket := range tickets {
		_, err := ticket.Wait()
		require.NoErrorf(t, err, "ticket %d", i)
	}
}

// TestBudgetKeepsClassOrder holds a small read behind a large one that does
// not fit: within a class, later work waits for earlier work, so a large
// operation is never overtaken indefinitely.
func TestBudgetKeepsClassOrder(t *testing.T) {
	c := withDefaultBudget(newTestCoordinator(t, 16, 0))
	f := testFile(t, 4<<20)
	_, handles := acceptOps(c,
		ReadOp(f, make([]byte, 1<<20), 0),
		ReadOp(f, make([]byte, 1<<20), 1<<20),
		ReadOp(f, make([]byte, 1<<20), 2<<20),
		ReadOp(f, make([]byte, 1<<20), 3<<20), // 4 × 0.41 ms > 1.5 ms
		ReadOp(f, make([]byte, 4096), 0),
	)
	placed, room := c.placeReady(false)
	require.Equal(t, 3, placed)
	require.Equal(t, workReady, handles[3].state)
	require.Equal(t, workReady, handles[4].state, "a small read overtook a large one")
	require.True(t, room, "writes still have budget")
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
}

// TestBudgetAdmitsOneOversizedOperation places an operation that costs more
// than the whole goal when its class has nothing in flight.
func TestBudgetAdmitsOneOversizedOperation(t *testing.T) {
	c := withDefaultBudget(newTestCoordinator(t, 16, 0))
	f := testFile(t, 4<<20)
	big := make([]byte, 2<<20) // 1.7 ms of write time, more than the 1.5 ms goal
	_, handles := acceptOps(c, WriteOp(f, big, 0), WriteOp(f, big, 2<<20))
	placed, room := c.placeReady(false)
	require.Equal(t, 1, placed, "want the first oversized write alone")
	require.Equal(t, workReady, handles[1].state)
	require.True(t, room)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	placed, _ = c.placeReady(false)
	require.Equal(t, 1, placed)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
}

// TestBudgetChargesCoalescedRun charges a coalesced run the cost of all its
// members and returns it when the run's write completes.
func TestBudgetChargesCoalescedRun(t *testing.T) {
	c := withDefaultBudget(newTestCoordinator(t, 16, 0))
	f := testFile(t, 4*4096)
	ops := make([]Op, 4)
	for i := range ops {
		ops[i] = WriteOp(f, make([]byte, 4096), int64(i*4096))
	}
	_, handles := acceptOps(c, ops...)
	placed, _ := c.placeReady(true)
	require.Equal(t, 1, placed)
	require.Equal(t, 4*handles[0].cost, c.inFlightCost[classWrite])
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	require.Zero(t, c.inFlightCost[classWrite])
}

// TestBudgetReleaseOfUnchargedWork completes work that was never placed, as
// tests and shutdown do: there is nothing to return to the budget.
func TestBudgetReleaseOfUnchargedWork(t *testing.T) {
	c := withDefaultBudget(newTestCoordinator(t, 4, 1))
	_, handles := acceptOps(c, VReadOp(0, make([]byte, 4096), 0))
	completeForTest(c, handles[0], 0, 4096, nil)
	require.Zero(t, c.inFlightCost[classRead])
}

func TestNewURingSchedulerValidatesBudget(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring not available")
	}
	_, err := NewURingScheduler(WithLatencyGoal(0))
	require.ErrorContains(t, err, "WithoutIOBudget")
	s, err := NewURingScheduler(WithoutIOBudget())
	require.NoError(t, err)
	require.NoError(t, s.Close())
}
