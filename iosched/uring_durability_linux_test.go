//go:build linux

package iosched

import (
	"bytes"
	"io"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAcceptMarksDurableWrites(t *testing.T) {
	regular := os.NewFile(100, "regular")
	durable := func(op Op) bool {
		c := newTestCoordinator(t, 8, 1)
		_, handles := acceptOps(c, op)
		return handles[0].durable != nil
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

// TestDurableWritesShareOneSyncPerFile places durable writes on two files that
// all complete before the next placement: the file with three writes gets one
// fdatasync for all of them, and the other file its own. The writes are not
// contiguous, so this is group commit without coalescing.
func TestDurableWritesShareOneSyncPerFile(t *testing.T) {
	c := newTestCoordinator(t, 8, 0)
	shared := testFile(t, 3*4096)
	other := testFile(t, 4096)
	tickets, _ := acceptOps(c,
		WriteOp(shared, make([]byte, 8), 0).Durable(),
		WriteOp(shared, make([]byte, 8), 8192).Durable(),
		WriteOp(other, make([]byte, 8), 0).Durable(),
		WriteOp(shared, make([]byte, 8), 4096).Durable(),
	)
	c.placeReady(false)
	require.Equal(t, 4, c.occupied)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })

	c.placeReady(false)
	require.Equal(t, 2, c.occupied, "want one fdatasync per file")
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	for i, ticket := range tickets {
		n, err := ticket.Wait()
		require.NoErrorf(t, err, "ticket %d", i)
		require.Equalf(t, 8, n, "ticket %d", i)
	}
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
	require.EqualValues(t, 3, work[0].fileState(work[0].durable).active)

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
