package iosched

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestCoalescedRun(t *testing.T) {
	fa := os.NewFile(100, "a")
	fb := os.NewFile(101, "b")
	w := func(f *os.File, off int64, n int) Op {
		return WriteOp(f, make([]byte, n), off)
	}
	vw := func(vfd uint32, off int64, n int) Op {
		return VWriteOp(vfd, make([]byte, n), off)
	}
	rd := func(f *os.File, off int64, n int) Op {
		return ReadOp(f, make([]byte, n), off)
	}

	tests := []struct {
		name string
		ops  []Op
		want int
	}{
		{"contiguous run", []Op{w(fa, 0, 4), w(fa, 4, 6), w(fa, 10, 2)}, 3},
		{"gap", []Op{w(fa, 0, 4), w(fa, 100, 4)}, 1},
		{"overlap", []Op{w(fa, 0, 4), w(fa, 0, 4)}, 1},
		{"different file", []Op{w(fa, 0, 4), w(fb, 4, 4)}, 1},
		{"read breaks run", []Op{w(fa, 0, 4), rd(fa, 4, 4), w(fa, 4, 4)}, 1},
		{"regular and virtual differ", []Op{w(fa, 0, 4), vw(0, 4, 4)}, 1},
		{"virtual contiguous", []Op{vw(1, 0, 4), vw(1, 4, 4)}, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCoordinator(t, 8, 2)
			acceptOps(c, tc.ops...)
			first := c.ready.head
			require.NotNil(t, first)
			require.Equal(t, tc.want, c.coalescedRun(first))
		})
	}
}

func TestCoalescibleWrite(t *testing.T) {
	fa := os.NewFile(100, "a")
	buf := make([]byte, 4)
	tests := []struct {
		name string
		op   Op
		want bool
	}{
		{"regular write", WriteOp(fa, buf, 0), true},
		{"virtual write", VWriteOp(0, buf, 0), true},
		{"read", ReadOp(fa, buf, 0), false},
		{"fixed write", WriteFixedOp(fa, buf, 0), false},
		{"fsync", FsyncOp(fa), false},
		{"linked write", WriteOp(fa, buf, 0).Link(FsyncOp(fa)), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.op.coalescibleWrite())
		})
	}
}

func TestPlaceReadyCoalescesWrites(t *testing.T) {
	c := newTestCoordinator(t, 8, 0)
	f := testFile(t, 12)
	tickets, handles := acceptOps(c,
		WriteOp(f, make([]byte, 4), 0),
		WriteOp(f, make([]byte, 6), 4),
		WriteOp(f, make([]byte, 2), 10),
	)

	c.placeReady(true)
	require.Equal(t, 1, c.occupied, "the run was not placed as one write")
	require.Equal(t, handles, coalescedRunForTest(handles[0]), "the leader does not link the run in writev order")

	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	for i, want := range []int{4, 6, 2} {
		n, err := tickets[i].Wait()
		require.NoError(t, err)
		require.Equal(t, want, n)
	}
	for _, member := range handles {
		require.Nil(t, member.staged, "a completed member still links its run")
	}
}

func TestCoalescedShortWriteCompletion(t *testing.T) {
	c := newTestCoordinator(t, 8, 0)
	f := os.NewFile(100, "a")
	tickets, handles := acceptOps(c,
		WriteOp(f, make([]byte, 4), 0),
		WriteOp(f, make([]byte, 4), 4),
	)
	issueWriteGroupForTest(c, handles...)

	c.finishWrite(handles[0], 6, nil)
	n, err := tickets[0].Wait()
	require.NoError(t, err)
	require.Equal(t, 4, n)
	n, err = tickets[1].Wait()
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Equal(t, 2, n)
}

func TestSingleShortWriteCompletion(t *testing.T) {
	c := newTestCoordinator(t, 1, 0)
	f := os.NewFile(100, "a")
	tickets, handles := acceptOps(c, WriteOp(f, make([]byte, 4), 0))
	issueWriteGroupForTest(c, handles...)

	c.finishWrite(handles[0], 2, nil)
	n, err := tickets[0].Wait()
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Equal(t, 2, n)
}

// coalescedRunForTest lists a placed run's members, linked from its leader.
func coalescedRunForTest(leader *submission) []*submission {
	var run []*submission
	for member := leader; member != nil; member = member.staged {
		run = append(run, member)
	}
	return run
}

func TestAcceptMarksDurableWrites(t *testing.T) {
	regular := os.NewFile(100, "regular")
	durable := func(op Op) bool {
		c := newTestCoordinator(t, 8, 1)
		if op.isFixed() {
			buffers, err := c.ring.RegisterBuffers(op.buf)
			require.NoError(t, err)
			c.sched.registeredBuffers = buffers
		}
		_, handles := acceptOps(c, op)
		return handles[0].durable() != nil
	}
	require.True(t, durable(VWriteOp(0, make([]byte, 8), 0).Durable()))
	require.True(t, durable(WriteOp(regular, make([]byte, 8), 0).Durable()))
	require.True(t, durable(WriteFixedOp(regular, make([]byte, 8), 0).Durable()))
	require.True(t, durable(WritevOp(regular, [][]byte{make([]byte, 8)}, 0).Durable()))
	require.False(t, durable(VWriteOp(0, make([]byte, 8), 0)))
	require.False(t, durable(VReadOp(0, make([]byte, 8), 0).Durable()))
}

// TestDurableWriteSyncsAfterItsWrite checks that a durable write is placed
// alone, that its fdatasync is placed only once the write has completed, and
// that the ticket waits for the fdatasync.
func TestDurableWriteSyncsAfterItsWrite(t *testing.T) {
	c := newTestCoordinator(t, 2, 0)
	f := testFile(t, 8)
	tickets, work := acceptOps(c, WriteOp(f, make([]byte, 8), 0).Durable())

	c.placeReady(false)
	require.Equal(t, 1, c.occupied, "a durable write was placed with more than its write")
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	require.Equal(t, 1, c.accepted.len, "the ticket completed before its fdatasync")
	require.Len(t, c.syncPending, 1)
	slat := work[0].completion.Slat
	writesDone := time.Now()

	c.placeReady(false)
	require.Equal(t, 1, c.occupied, "the fdatasync was not placed")
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	n, err := tickets[0].Wait()
	require.NoError(t, err)
	require.Equal(t, 8, n)
	require.Zero(t, c.accepted.len)
	require.Positive(t, slat)
	require.Equal(t, slat, tickets[0].Slat, "the sync must not replace the original ring-placement time")
	require.GreaterOrEqual(t, tickets[0].Clat, writesDone.Sub(work[0].completion.started)-slat, "clat includes the sync")
}

// Durable tails share a sync with standalone writes to the same file, even
// when their chains start on a different file.
func TestLinkedDurableWritesShareSync(t *testing.T) {
	c := newTestCoordinator(t, 8, 0)
	data, index, other := testFile(t, 16), testFile(t, 3*4096), testFile(t, 8)
	tickets, work := acceptOps(c,
		WriteOp(data, make([]byte, 4), 0).Link(WriteOp(index, make([]byte, 8), 0).Durable()),
		WriteOp(data, make([]byte, 4), 8).Link(WriteOp(index, make([]byte, 8), 8192).Durable()),
		WriteOp(index, make([]byte, 8), 4096).Durable(),
		WriteOp(other, make([]byte, 8), 0).Durable(),
	)
	c.placeReady(false)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	require.Equal(t, 4, c.accepted.len, "all tickets must wait for their file's sync")
	require.Len(t, c.syncPending, 2)
	require.Zero(t, work[0].fileState(&work[0].root).active, "the data file owes no sync")
	require.EqualValues(t, 3, work[0].fileState(work[0].durable()).active)

	c.placeReady(false)
	require.Equal(t, 2, c.occupied, "one sync per file, shared across chained and standalone writes")
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	for i, ticket := range tickets {
		n, err := ticket.Wait()
		require.NoError(t, err)
		want := 8
		if i < 2 {
			want = 4 // The ticket reports the root operation's byte count.
		}
		require.Equal(t, want, n)
	}
}

func TestLinkedDurableWriteFailure(t *testing.T) {
	for _, stage := range []string{"data", "index", "hard-link"} {
		t.Run(stage, func(t *testing.T) {
			c := newTestCoordinator(t, 4, 0)
			data, index := testFile(t, 4), testFile(t, 4)
			failedFile := data
			if stage == "index" {
				failedFile = index
			}
			readOnly, err := os.Open(failedFile.Name())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, readOnly.Close()) })
			prefix, tail := WriteOp(data, []byte("data"), 0), WriteOp(index, []byte("meta"), 0).Durable()
			if stage == "index" {
				tail = WriteOp(readOnly, []byte("meta"), 0).Durable()
			} else {
				prefix = WriteOp(readOnly, []byte("data"), 0)
			}
			op := prefix.Link(tail)
			if stage == "hard-link" {
				op = prefix.HardLink(tail)
			}
			tickets, _ := acceptOps(c, op)
			c.placeReady(false)
			submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
			want := make([]byte, 4)
			if stage == "hard-link" {
				require.Len(t, c.syncPending, 1, "a successful durable tail still syncs after an earlier hard-link failure")
				c.placeReady(false)
				submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
				want = []byte("meta")
			} else {
				require.Empty(t, c.syncPending, "failed or canceled durable tails must not request a sync")
			}
			_, err = tickets[0].Wait()
			require.ErrorIs(t, err, syscall.EBADF)
			require.Zero(t, c.accepted.len)
			got, err := os.ReadFile(index.Name())
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

// A read can finish after the chain's data write but before its index sync.
// The chain still references the data file's state until its ticket completes.
func TestLinkedDurableTailRetainsFileState(t *testing.T) {
	c := newTestCoordinator(t, 4, 0)
	data, index := testFile(t, 4), testFile(t, 8)
	tickets, work := acceptOps(c,
		WriteOp(data, make([]byte, 4), 0).Link(WriteOp(index, make([]byte, 8), 0).Durable()),
		ReadOp(data, make([]byte, 4), 0),
	)
	dataState := work[0].fileState(&work[0].root)
	completeForTest(c, work[0], 0, 4, nil)
	completeForTest(c, work[0], 1, 8, nil)
	completeForTest(c, work[1], 0, 4, nil)
	require.Zero(t, dataState.active)
	require.Same(t, dataState, c.files.lookup(&work[0].root), "the pending chain must prevent file state recycling")

	c.placeReady(false)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	for _, ticket := range tickets {
		_, err := ticket.Wait()
		require.NoError(t, err)
	}
	require.Empty(t, c.files.regular, "file state is released after the last chain completes")
}

func TestLinkedDurableTailSyncError(t *testing.T) {
	c := newTestCoordinator(t, 4, 1)
	data := testFile(t, 4)
	tickets, work := acceptOps(c, WriteOp(data, make([]byte, 4), 0).Link(VWriteOp(0, make([]byte, 8), 0).Durable()))
	// Deliver successful writes, then run a real sync. Slot 0 is empty: the
	// tail's sync fails, whereas an incorrect sync of the root file would pass.
	completeForTest(c, work[0], 0, 4, nil)
	completeForTest(c, work[0], 1, 8, nil)
	c.placeReady(false)
	require.Equal(t, 1, c.occupied)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	n, err := tickets[0].Wait()
	require.Equal(t, 4, n)
	require.ErrorIs(t, err, syscall.EBADF)
	results := tickets[0].AllResults()
	require.Equal(t, Result{N: 4}, results[0])
	require.Equal(t, 8, results[1].N, "the tail's sync failure is reported with its write")
	require.ErrorIs(t, results[1].Err, syscall.EBADF)
	require.Zero(t, c.files.virtual[0].active)
}

func TestLinkedDurableTailWaitsForAllCompletions(t *testing.T) {
	c := newTestCoordinator(t, 4, 0)
	data, index := testFile(t, 4), testFile(t, 8)
	tickets, work := acceptOps(c, WriteOp(data, make([]byte, 4), 0).Link(WriteOp(index, make([]byte, 8), 0).Durable()))
	// Reap the tail and its sync before delivering the root's completion.
	completeForTest(c, work[0], 1, 8, nil)
	c.placeReady(false)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	select {
	case <-tickets[0].Done():
		t.Fatal("ticket completed while the root still owed a completion")
	default:
	}
	completeForTest(c, work[0], 0, 4, nil)
	n, err := tickets[0].Wait()
	require.NoError(t, err)
	require.Equal(t, 4, n)
}

func TestDrainWaitsForLinkedDurableTail(t *testing.T) {
	c := newTestCoordinator(t, 4, 0)
	data, index := testFile(t, 4), testFile(t, 8)
	tickets, _ := acceptOps(c,
		WriteOp(data, make([]byte, 4), 0).Link(WriteOp(index, make([]byte, 8), 0).Durable()),
		DrainOp(index),
	)
	c.placeReady(false)
	require.Equal(t, 2, c.occupied, "drain must wait for the chain")
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	c.placeReady(false)
	require.Equal(t, 1, c.occupied, "only the sync may run; drain must still wait")
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	c.placeReady(false)
	require.Equal(t, 1, c.occupied, "drain becomes runnable after the sync")
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	for _, ticket := range tickets {
		_, err := ticket.Wait()
		require.NoError(t, err)
	}
}

// TestFailedDurableWriteSkipsSync delivers a short write for a durable write:
// there is nothing to make durable, so its ticket completes without waiting
// for an fdatasync.
func TestFailedDurableWriteSkipsSync(t *testing.T) {
	c := newTestCoordinator(t, 2, 1)
	tickets, handles := acceptOps(c, VWriteOp(0, make([]byte, 4), 0).Durable())
	issueWriteGroupForTest(c, handles...)

	c.finishWrite(handles[0], 2, nil)
	n, err := tickets[0].Wait()
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Equal(t, 2, n)
	require.Empty(t, c.syncPending)
	require.Zero(t, c.files.virtual[0].active, "the fdatasync's file activity was not retired")
}

func TestPlacePlainWriteWithoutSync(t *testing.T) {
	c := newTestCoordinator(t, 2, 0)
	f := testFile(t, 8)
	tickets, _ := acceptOps(c, WriteOp(f, make([]byte, 8), 0))

	c.placeReady(true)
	require.Equal(t, 1, c.occupied)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	n, err := tickets[0].Wait()
	require.NoError(t, err)
	require.Equal(t, 8, n)
	require.Empty(t, c.syncPending, "a plain write waited for an fdatasync")
}

// TestURingConcurrentDurableWrites races durable writes to one file through
// the scheduler, so fdatasyncs are shared by whatever completes together, and
// then drains the file: every write must report success, the drain must wait
// for the fdatasyncs, and the data must be in place.
func TestURingConcurrentDurableWrites(t *testing.T) {
	const (
		writers = 8
		writes  = 200
		block   = 512
	)
	s := newURingForDoorbellTest(t, WithRingDepth(32))
	defer func() { require.NoError(t, s.Close()) }()
	f := testFile(t, writers*writes*block)

	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := bytes.Repeat([]byte{byte(w + 1)}, block)
			for i := range writes {
				ticket, err := s.Submit(WriteOp(f, buf, int64((w*writes+i)*block)).Durable())
				if err == nil {
					var n int
					if n, err = ticket.Wait(); err == nil && n != block {
						err = io.ErrShortWrite
					}
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	drain, err := s.Submit(DrainOp(f))
	require.NoError(t, err)
	_, err = waitTicket(t, drain, 5*time.Second)
	require.NoError(t, err)

	got := make([]byte, writers*writes*block)
	_, err = f.ReadAt(got, 0)
	require.NoError(t, err)
	for w := range writers {
		want := bytes.Repeat([]byte{byte(w + 1)}, writes*block)
		require.Equalf(t, want, got[w*writes*block:(w+1)*writes*block], "writer %d", w)
	}
}

// A durable write followed by a close gets its own fdatasync between them.
func TestIntermediateDurableWriteThenClose(t *testing.T) {
	s := newURingForDoorbellTest(t, WithVFiles(1))
	defer func() { require.NoError(t, s.Close()) }()
	path := filepath.Join(t.TempDir(), "data")
	data := []byte("durable")
	op := VOpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR, 0o600, AllocateVFD).Link(
		VWriteOp(AllocateVFD, data, 0).Durable(), VCloseOp(AllocateVFD))
	ticket, err := s.Submit(op)
	require.NoError(t, err)
	_, err = waitTicket(t, ticket, 5*time.Second)
	require.NoError(t, err)
	results := ticket.AllResults()
	require.Len(t, results, 3, "the synthesized fdatasync reports into its write")
	require.Equal(t, Result{N: len(data)}, results[1])
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, got)
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
