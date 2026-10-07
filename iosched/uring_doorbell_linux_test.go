//go:build linux

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

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

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
	request, _ := newSubmission(VReadOp(0, make([]byte, 1), 0), 1)
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
	request, _ := newSubmission(ReadOp(stuck, make([]byte, 8), 0), 1)
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
	s, err := NewURingScheduler(opts...)
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

func TestURingCloseWhileIdle(t *testing.T) {
	s := newURingForDoorbellTest(t, WithRingDepth(16))
	waitUntilWaiting(t, s, wakeChannel)
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
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

func TestURingChainUsesAllButTheDoorbellEntry(t *testing.T) {
	s := newURingForDoorbellTest(t, WithRingDepth(4))
	defer func() { require.NoError(t, s.Close()) }()
	f, err := os.Create(filepath.Join(t.TempDir(), "data"))
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	buf := make([]byte, 4096)

	ticket, err := s.Submit(WriteOp(f, buf, 0).Link(WriteOp(f, buf, 4096), WriteOp(f, buf, 8192)))
	require.NoError(t, err)
	_, err = waitTicket(t, ticket, 5*time.Second)
	require.NoError(t, err)

	_, err = s.Submit(WriteOp(f, buf, 0).Link(WriteOp(f, buf, 4096), WriteOp(f, buf, 8192), WriteOp(f, buf, 12288)))
	require.ErrorContains(t, err, "exceeds the limit of 3")
}

func TestNewURingSchedulerNeedsRoomBesideDoorbell(t *testing.T) {
	if !IOUringAvailable {
		t.Skip("io_uring not available")
	}
	_, err := NewURingScheduler(WithRingDepth(1))
	require.ErrorContains(t, err, "doorbell")
}
