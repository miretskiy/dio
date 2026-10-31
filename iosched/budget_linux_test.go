package iosched

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestURingRejectsBeforeOpeningChain(t *testing.T) {
	c := newTestCoordinator(t, 32, 1)
	c.sched.config.disk.writeBudget.limit = 1 // any write fills the backlog
	p := c.sched.config.disk
	blocker := policyWrite(p, 4)
	p.account(blocker)
	p.updateAdmission()
	path := filepath.Join(t.TempDir(), "must-not-exist")
	op := VOpenatOp(-100, path, os.O_CREATE|os.O_RDWR, 0600, AllocateVFD).
		Link(VWriteOp(AllocateVFD, make([]byte, 5), 0), VCloseOp(AllocateVFD))
	called := false
	ticket, err := SubmitNotify(c.sched, op, func(int, error) { called = true })
	require.ErrorIs(t, err, ErrRejected)
	require.Nil(t, ticket.completion)
	require.Nil(t, c.sched.stagingHead.Load())
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.False(t, called)
	p.release(blocker, &testRing)
	// Once the circuit reopens, a chain owns its open/close lifecycle normally.
	op = VOpenatOp(-100, path, os.O_CREATE|os.O_RDWR, 0600, AllocateVFD).
		Link(VWriteOp(AllocateVFD, make([]byte, 4), 0), VCloseOp(AllocateVFD))
	ticket, err = c.sched.Submit(op)
	require.NoError(t, err)
	c.accept(c.sched.takeStaged())
	c.placeReady(false)
	submitAndReapUntil(t, c, func() bool { return c.accepted.len == 0 })
	_, err = ticket.Wait()
	require.NoError(t, err)
	require.Zero(t, c.sched.config.disk.Stats().Writes.Backlog)
}

func TestURingRejectionCapacityThroughDurabilityAndCallback(t *testing.T) {
	c := newTestCoordinator(t, 32, 0, diskOptions(WithLatencyGoal(DefaultLatencyGoal)))
	c.sched.config.disk.writeBudget.limit = 1
	f := testFile(t, 16)
	var next Ticket
	var nextErr error
	ticket, err := SubmitNotify(c.sched, WriteOp(f, make([]byte, 4), 0).Durable(), func(int, error) {
		next, nextErr = c.sched.Submit(WriteOp(f, make([]byte, 4), 4))
	})
	require.NoError(t, err)
	c.accept(c.sched.takeStaged())
	_, err = c.sched.Submit(WriteOp(f, []byte{1}, 0))
	require.ErrorIs(t, err, ErrRejected, "admitted operations retain their charge")
	work := c.accepted.head
	issueForTest(c, work)
	c.finishOperation(work, &work.root, 4, nil)
	_, err = c.sched.Submit(WriteOp(f, []byte{1}, 0))
	require.ErrorIs(t, err, ErrRejected, "durable write keeps capacity until sync")
	state := work.fileState(work.durable())
	batch := state.syncBatch
	state.syncBatch = submissionQueue{}
	c.syncPending = nil
	c.finishSync(batch, nil)
	_, err = ticket.Wait()
	require.NoError(t, err)
	require.NoError(t, nextErr, "release before callback can submit replacement work")
	c.accept(c.sched.takeStaged())
	c.placeReady(false)
	submitAndReapUntil(t, c, func() bool { return c.accepted.len == 0 })
	_, err = next.Wait()
	require.NoError(t, err)
	require.Zero(t, c.sched.config.disk.Stats().Writes.Backlog)
}

func TestURingRejectionCapacityOnFailuresAndShutdown(t *testing.T) {
	for _, staged := range []bool{true, false} {
		t.Run(map[bool]string{true: "staged", false: "accepted"}[staged], func(t *testing.T) {
			c := newTestCoordinator(t, 32, 0, diskOptions(WithLatencyGoal(DefaultLatencyGoal)))
			c.sched.config.disk.writeBudget.limit = 1
			f := testFile(t, 16)
			ticket, err := c.sched.Submit(WriteOp(f, make([]byte, 4), 0))
			require.NoError(t, err)
			if !staged {
				c.accept(c.sched.takeStaged())
			}
			c.failRemaining(c.sched.closeStaging(), errSchedulerClosed, errSchedulerClosed)
			_, err = ticket.Wait()
			require.ErrorIs(t, err, errSchedulerClosed)
			require.Equal(t, DiskStats{}, c.sched.config.disk.Stats())
		})
	}
	c := newTestCoordinator(t, 32, 0, diskOptions(WithLatencyGoal(DefaultLatencyGoal)))
	c.sched.config.disk.writeBudget.limit = 1
	f := testFile(t, 16)
	ticket, err := c.sched.Submit(WriteOp(f, make([]byte, 4), 0))
	require.NoError(t, err)
	c.accept(c.sched.takeStaged())
	work := c.accepted.head
	issueForTest(c, work)
	c.finishOperation(work, &work.root, 0, syscall.EIO)
	_, err = ticket.Wait()
	require.ErrorIs(t, err, syscall.EIO)
	require.Equal(t, DiskStats{}, c.sched.config.disk.Stats())
}

// Reuse one buffer to fill a two-second backlog (2 GB at one byte per
// nanosecond) without allocating or issuing 2 GB. The test drives admission.
func TestURingBacklogLimitWithReusedBuffer(t *testing.T) {
	c := newTestCoordinator(t, 32, 0, diskOptions(append(nanosPerByte, WithLatencyGoal(DefaultLatencyGoal),
		WithWriteBacklogLimit(2*time.Second))...))
	f := testFile(t, 1)
	buf := make([]byte, 1_000_000)
	for range 2000 {
		_, err := c.sched.Submit(WriteOp(f, buf, 0))
		require.NoError(t, err)
	}
	c.accept(c.sched.takeStaged())
	require.Equal(t, 2*time.Second, c.sched.config.disk.Stats().Writes.Backlog)
	ticket, err := c.sched.Submit(WriteOp(f, buf[:1], 0))
	require.ErrorIs(t, err, ErrRejected)
	require.Nil(t, ticket.completion)
	c.failRemaining(c.sched.closeStaging(), errSchedulerClosed, errSchedulerClosed)
	require.Equal(t, DiskStats{}, c.sched.config.disk.Stats())
}

func TestCoordinatorCountsMixedChainExcludingMustAdmit(t *testing.T) {
	c := newTestCoordinator(t, 32, 0, diskOptions(append(nanosPerByte, WithLatencyGoal(DefaultLatencyGoal),
		WithReadBacklogLimit(4), WithWriteBacklogLimit(4))...))
	f := testFile(t, 16)
	op := ReadOp(f, make([]byte, 4), 0).Link(
		WriteOp(f, make([]byte, 2), 0), WriteOp(f, make([]byte, 2), 2))
	ticket, err := c.sched.Submit(op)
	require.NoError(t, err)
	c.accept(c.sched.takeStaged())
	q := c.sched.config.disk.Stats()
	require.EqualValues(t, 4, q.Reads.Backlog)
	require.EqualValues(t, 4, q.Writes.Backlog)
	_, err = c.sched.Submit(WriteOp(f, []byte{1}, 0))
	require.ErrorIs(t, err, ErrRejected)
	exempt, err := c.sched.Submit(WriteOp(f, []byte{1}, 0).MustAdmit())
	require.NoError(t, err)
	c.accept(c.sched.takeStaged())
	require.Equal(t, q, c.sched.config.disk.Stats())
	c.placeReady(false)
	submitAndReapUntil(t, c, func() bool { return c.accepted.len == 0 })
	_, err = ticket.Wait()
	require.NoError(t, err)
	_, err = exempt.Wait()
	require.NoError(t, err)
	require.Zero(t, c.sched.config.disk.Stats().Reads.Backlog)
	require.Zero(t, c.sched.config.disk.Stats().Writes.Backlog)
}

func TestDiskAcrossCoordinators(t *testing.T) {
	p := NewDiskPolicy(WithLatencyGoal(time.Millisecond), WithWriteBandwidth(1000),
		WithWriteBacklogLimit(8*time.Millisecond), WithoutReadIOBudget())
	a := newTestCoordinator(t, 32, 0, WithDiskPolicy(p))
	b := newTestCoordinator(t, 32, 0, WithDiskPolicy(p))
	f := testFile(t, 16)
	first, err := a.sched.Submit(WriteOp(f, make([]byte, 4), 0))
	require.NoError(t, err)
	second, err := b.sched.Submit(WriteOp(f, make([]byte, 4), 4))
	require.NoError(t, err)
	third, err := b.sched.Submit(WriteOp(f, make([]byte, 4), 8))
	require.NoError(t, err)
	require.Equal(t, DiskStats{}, p.Stats(), "staged work is not yet counted")
	a.accept(a.sched.takeStaged())
	b.accept(b.sched.takeStaged())
	require.Equal(t, 12*time.Millisecond, p.Stats().Writes.Backlog)
	placed, _ := a.placeReady(false)
	require.Equal(t, 1, placed)
	require.Equal(t, 4*time.Millisecond, p.Stats().Writes.InFlight)
	placed, _ = b.placeReady(false)
	require.Equal(t, 1, placed, "a ring with no writes in flight does not wait on another ring")
	require.Equal(t, 8*time.Millisecond, p.Stats().Writes.InFlight)
	require.Equal(t, 1, b.ready.len, "with its own write in flight, the ring waits for the allowance")
	submitAndReapUntil(t, a, func() bool { return a.accepted.len == 0 })
	submitAndReapUntil(t, b, func() bool { return b.occupied == 0 })
	placed, _ = b.placeReady(false)
	require.Equal(t, 1, placed, "the ring's own completion releases its queued write")
	submitAndReapUntil(t, b, func() bool { return b.accepted.len == 0 })
	for _, ticket := range []Ticket{first, second, third} {
		_, err = ticket.Wait()
		require.NoError(t, err)
	}
	require.Equal(t, DiskStats{}, p.Stats())
}

func TestDiskMixedAdmissionHasNoCharges(t *testing.T) {
	for _, limit := range []DiskPolicyOption{WithReadBacklogLimit(1), WithWriteBacklogLimit(1)} {
		p := policy(WithReadBacklogLimit(8), WithWriteBacklogLimit(8), limit)
		c := newTestCoordinator(t, 32, 0, WithDiskPolicy(p))
		f := testFile(t, 16)
		op := ReadOp(f, make([]byte, 1), 0).Link(WriteOp(f, make([]byte, 1), 0))
		blocker, err := c.sched.prepareSubmission(&op)
		require.NoError(t, err)
		p.account(blocker)
		p.updateAdmission()
		before := p.Stats()
		ticket, err := c.sched.Submit(op)
		require.ErrorIs(t, err, ErrRejected)
		require.Nil(t, ticket.completion)
		require.Nil(t, c.sched.stagingHead.Load())
		require.Equal(t, before, p.Stats(), "rejected mixed chains acquire nothing")
		p.release(blocker, &c.inFlight)
		require.Equal(t, DiskStats{}, p.Stats())
	}
}

func TestWriteBacklogRecoveryAcrossRings(t *testing.T) {
	// One write byte models as 125 ms: 16 bytes fill the two-second limit and
	// the pause drains to 4 bytes' worth.
	p := NewDiskPolicy(WithoutIOBudget(), WithWriteBandwidth(8), WithWriteBacklogLimit(2*time.Second),
		WithWriteBacklogRecovery(500*time.Millisecond), WithReadBacklogLimit(2*time.Second))
	a := newTestCoordinator(t, 32, 0, WithDiskPolicy(p))
	b := newTestCoordinator(t, 32, 0, WithDiskPolicy(p))
	f := testFile(t, 16)
	first, err := a.sched.Submit(WriteOp(f, make([]byte, 12), 0))
	require.NoError(t, err)
	second, err := b.sched.Submit(WriteOp(f, make([]byte, 4), 12))
	require.NoError(t, err)
	require.Zero(t, p.Stats().Writes.Backlog)
	a.accept(a.sched.takeStaged())
	b.accept(b.sched.takeStaged())
	require.Equal(t, 2*time.Second, p.Stats().Writes.Backlog)
	require.True(t, p.writeBudget.rejects())
	a.placeReady(false)
	submitAndReapUntil(t, a, func() bool { return a.accepted.len == 0 })
	_, err = first.Wait()
	require.NoError(t, err)
	require.Equal(t, 500*time.Millisecond, p.Stats().Writes.Backlog)
	for _, c := range []*coordinator{a, b} {
		op := ReadOp(f, make([]byte, 1), 0).Link(WriteOp(f, []byte{1}, 0))
		ticket, err := c.sched.Submit(op)
		require.ErrorIs(t, err, ErrRejected, "both rings remain closed until the estimate expires")
		require.Nil(t, ticket.completion)
		require.Zero(t, p.Stats().Reads.Backlog, "mixed-chain rejection acquires no read charges")
	}
	read, err := a.sched.Submit(ReadOp(f, make([]byte, 1), 0))
	require.NoError(t, err)
	a.accept(a.sched.takeStaged())
	a.placeReady(false)
	submitAndReapUntil(t, a, func() bool { return a.accepted.len == 0 })
	_, err = read.Wait()
	require.NoError(t, err)
	require.Eventually(t, func() bool { return !p.writeBudget.rejects() }, 3*time.Second, time.Millisecond)
	resumed, err := a.sched.Submit(WriteOp(f, []byte{1}, 0))
	require.NoError(t, err)
	require.Equal(t, 500*time.Millisecond, p.Stats().Writes.Backlog, "new work is still staged")
	for _, c := range []*coordinator{a, b} {
		c.accept(c.sched.takeStaged())
		c.placeReady(false)
		submitAndReapUntil(t, c, func() bool { return c.accepted.len == 0 })
	}
	_, err = second.Wait()
	require.NoError(t, err)
	_, err = resumed.Wait()
	require.NoError(t, err)
	require.Equal(t, DiskStats{}, p.Stats())
}

func TestDiskMustAdmitBypassesLimitsAndAccounting(t *testing.T) {
	p := NewDiskPolicy(WithLatencyGoal(time.Nanosecond), WithWriteBacklogLimit(1), WithReadBacklogLimit(1))
	c := newTestCoordinator(t, 16, 0, WithDiskPolicy(p))
	f := testFile(t, 8)
	blockerOp := ReadOp(f, make([]byte, 1), 0).Link(WriteOp(f, make([]byte, 1), 0))
	blocker, err := c.sched.prepareSubmission(&blockerOp)
	require.NoError(t, err)
	require.True(t, p.admit(blocker))
	p.account(blocker)
	p.updateAdmission()
	before := p.Stats()
	op := WriteOp(f, make([]byte, 2), 0).Link(
		ReadOp(f, make([]byte, 2), 0), WriteOp(f, make([]byte, 2), 2)).MustAdmit()
	ticket, err := c.sched.Submit(op)
	require.NoError(t, err)
	require.Equal(t, before, p.Stats(), "MustAdmit is invisible to admission accounting")
	c.accept(c.sched.takeStaged())
	c.placeReady(false)
	submitAndReapUntil(t, c, func() bool { return c.accepted.len == 0 })
	_, err = ticket.Wait()
	require.NoError(t, err)
	require.Equal(t, before, p.Stats(), "completion must not release uncharged work")
	p.release(blocker, &testRing)
	require.Equal(t, DiskStats{}, p.Stats())
}

func TestDiskConcurrentRings(t *testing.T) {
	// A 4 KiB write models as 1/IOPS, about 12 µs: the limit holds two.
	p := NewDiskPolicy(WithoutIOBudget(), WithWriteBacklogLimit(25*time.Microsecond))
	const rings, requests = 4, 128
	schedulers := make([]*URingScheduler, rings)
	for i := range schedulers {
		schedulers[i] = newURingForDoorbellTest(t, WithDiskPolicy(p))
		t.Cleanup(func() { require.NoError(t, schedulers[i].Close()) })
	}
	f := testFile(t, rings*requests*4096)
	var wg sync.WaitGroup
	for i, s := range schedulers {
		wg.Go(func() {
			for j := range requests {
				ticket, err := s.Submit(WriteOp(f, make([]byte, 4096), int64((i*requests+j)*4096)))
				if errors.Is(err, ErrRejected) {
					continue
				}
				if !assert.NoError(t, err) {
					return
				}
				_, err = ticket.Wait()
				assert.NoError(t, err)
			}
		})
	}
	wg.Wait()
	require.Equal(t, DiskStats{}, p.Stats())
}

func TestSubmitReturnsWhileCoordinatorAndDiskAreBlocked(t *testing.T) {
	// The held 4 KiB write models as four seconds and fills the write backlog.
	d := NewDiskPolicy(WithLatencyGoal(time.Nanosecond), WithoutReadIOBudget(), WithWriteBandwidth(1024),
		WithWriteBacklogLimit(4*time.Second), WithWriteBacklogRecovery(0), WithReadBacklogLimit(time.Hour))
	a := newURingForDoorbellTest(t, WithDiskPolicy(d))
	b := newURingForDoorbellTest(t, WithDiskPolicy(d))
	t.Cleanup(func() { require.NoError(t, a.Close()); require.NoError(t, b.Close()) })
	f := testFile(t, 4096)
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblockCoordinator := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(unblockCoordinator)
	_, err := SubmitNotify(a, ReadOp(f, make([]byte, 8), 0), func(int, error) {
		close(entered)
		<-resume
	})
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator did not enter callback")
	}

	blocked, release := blockingRead(t)
	var releaseOnce sync.Once
	unblockIO := func() { releaseOnce.Do(release) }
	t.Cleanup(unblockIO)
	held, err := b.Submit(ReadOp(blocked, make([]byte, 8), 0).Link(WriteOp(f, make([]byte, 4096), 0)))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		q := d.Stats()
		return q.Reads.Backlog > 0 && q.Writes.InFlight > 0 && q.Writes.Backlog == 4*time.Second
	}, 5*time.Second, time.Millisecond)

	type result struct {
		tickets       []Ticket
		err, rejected error
	}
	done := make(chan result, 1)
	go func() {
		var r result
		for range 64 {
			ticket, err := a.Submit(ReadOp(f, make([]byte, 8), 0))
			if err != nil {
				r.err = err
				break
			}
			r.tickets = append(r.tickets, ticket)
		}
		_, r.rejected = a.Submit(WriteOp(f, []byte{1}, 0))
		done <- r
	}()
	var r result
	select {
	case r = <-done:
	case <-time.After(time.Second):
		t.Fatal("Submit waited for the coordinator or shared disk")
	}
	require.NoError(t, r.err)
	require.Len(t, r.tickets, 64)
	require.ErrorIs(t, r.rejected, ErrRejected, "published pause rejects immediately")
	staged := 0
	for work := a.stagingHead.Load(); work != nil; work = work.staged {
		staged++
	}
	require.Equal(t, 64, staged)
	unblockCoordinator()
	unblockIO()
	_, err = waitTicket(t, held, 5*time.Second)
	require.NoError(t, err)
	for _, ticket := range r.tickets {
		_, err = waitTicket(t, ticket, 5*time.Second)
		require.NoError(t, err)
	}
}

func TestPreparationCollectsAdmissionCharges(t *testing.T) {
	limits := []DiskPolicyOption{WithReadBacklogLimit(time.Second), WithWriteBacklogLimit(time.Second)}
	s := URingScheduler{config: makeSchedulerConfig([]Option{WithVFiles(1), diskOptions(limits...)})}
	data := make([]byte, 1<<20)
	op := VOpenatOp(0, "file", 0, 0, 0).Link(
		VWriteOp(0, data, 0), VReadvOp(0, [][]byte{data[:400], data[400:]}, 0), VWriteOp(0, data, 1<<20).Durable())
	work, err := s.prepareSubmission(&op)
	require.NoError(t, err)
	require.EqualValues(t, 4, work.opCount())
	require.Equal(t, ioCharge{backlog: 409600, inFlight: 409600}, work.reads)
	require.Equal(t, ioCharge{backlog: 2 * 859488, inFlight: 2 * 859488}, work.writes)
	require.Same(t, work.op(3), work.durable())

	s.config = makeSchedulerConfig([]Option{WithVFiles(1), diskOptions(append(limits, WithoutReadIOBudget())...)})
	work, err = s.prepareSubmission(&op)
	require.NoError(t, err)
	s.config.disk.account(work)
	require.Equal(t, ioCharge{backlog: 409600}, work.reads, "no read goal: the backlog is still charged")
	require.Equal(t, ioCharge{backlog: 2 * 859488, inFlight: 2 * 859488}, work.writes)
	s.config.disk.release(work, &testRing)

	s.config = makeSchedulerConfig([]Option{WithVFiles(1), diskOptions(WithoutIOBudget())})
	work, err = s.prepareSubmission(&op)
	require.NoError(t, err)
	require.True(t, s.config.disk.admit(work))
	s.config.disk.account(work)
	require.Equal(t, ioCharge{}, work.reads)
	require.Equal(t, ioCharge{}, work.writes)
	s.config.disk.release(work, &testRing)
	require.Equal(t, DiskStats{}, s.config.disk.Stats())
}

func TestDiskMustAdmitDoesNotConsumeAllowance(t *testing.T) {
	p := NewDiskPolicy(WithLatencyGoal(time.Nanosecond), WithWriteBacklogLimit(1))
	s := URingScheduler{config: makeSchedulerConfig([]Option{WithDiskPolicy(p)})}
	op := WriteOp(nil, make([]byte, 16), 0).MustAdmit()
	exempt, err := s.prepareSubmission(&op)
	require.NoError(t, err)
	require.Positive(t, exempt.writes.backlog)
	require.True(t, p.admit(exempt))
	p.account(exempt)
	require.Equal(t, ioCharge{}, exempt.reads)
	require.Equal(t, ioCharge{}, exempt.writes)
	require.Equal(t, DiskStats{}, p.Stats())

	ordinary := policyWrite(p, 1)
	require.True(t, p.admit(ordinary), "mandatory work leaves the entire allowance available")
	p.account(ordinary)
	p.updateAdmission()
	before := p.Stats()
	p.release(exempt, &testRing)
	require.Equal(t, before, p.Stats(), "releasing mandatory work must not subtract ordinary work")
	p.release(ordinary, &testRing)
	require.Equal(t, DiskStats{}, p.Stats())
}

// A ring with no writes of its own in flight has no completion coming to wake
// it, so it must not wait for allowance another ring holds.
func TestIdleRingDoesNotWaitOnSharedAllowance(t *testing.T) {
	p := NewDiskPolicy(WithLatencyGoal(time.Nanosecond), WithoutReadIOBudget())
	a := newURingForDoorbellTest(t, WithDiskPolicy(p))
	b := newURingForDoorbellTest(t, WithDiskPolicy(p))
	t.Cleanup(func() { require.NoError(t, a.Close()); require.NoError(t, b.Close()) })
	f := testFile(t, 8192)
	gate, release := blockingRead(t)
	var once sync.Once
	unblock := func() { once.Do(release) }
	t.Cleanup(unblock)
	first, err := a.Submit(ReadOp(gate, make([]byte, 8), 0).Link(WriteOp(f, make([]byte, 4096), 0)))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return p.Stats().Writes.InFlight > 0 }, time.Second, time.Millisecond)
	second, err := b.Submit(WriteOp(f, make([]byte, 4096), 4096))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { _, err := second.Wait(); done <- err }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("an idle ring waited for another ring's allowance")
	}
	unblock()
	_, err = first.Wait()
	require.NoError(t, err)
	require.Equal(t, DiskStats{}, p.Stats())
}
