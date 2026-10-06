//go:build linux

package iosched

import (
	"bytes"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAcceptMarksDurableWrites(t *testing.T) {
	regular := os.NewFile(100, "regular")
	durable := func(op Op) bool {
		c := newTestCoordinator(t, 8, 1)
		_, handles := acceptOps(c, op)
		return handles[0].durable
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
	tickets, _ := acceptOps(c, WriteOp(f, make([]byte, 8), 0).Durable())

	c.placeReady(false)
	require.Equal(t, 1, c.occupied, "a durable write was placed with more than its write")
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	require.Equal(t, 1, c.accepted.len, "the ticket completed before its fdatasync")
	require.Len(t, c.syncPending, 1)

	c.placeReady(false)
	require.Equal(t, 1, c.occupied, "the fdatasync was not placed")
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	n, err := tickets[0].Wait()
	require.NoError(t, err)
	require.Equal(t, 8, n)
	require.Zero(t, c.accepted.len)
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
