package iosched

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestAllocatedVFDResults(t *testing.T) {
	s := newAllocatingScheduler(t, 1)
	path := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0600))
	for _, name := range []string{path, path + "missing"} {
		read := make([]byte, 8)
		op := VOpenatOp(unix.AT_FDCWD, name, unix.O_RDONLY, 0, AllocateVFD).Link(
			VReadOp(AllocateVFD, read, 0).HardLink(VCloseOp(AllocateVFD)))
		ticket, err := s.Submit(op)
		require.NoError(t, err)
		_, err = ticket.Wait()
		results := ticket.AllResults()
		require.Len(t, results, 3)
		if name == path {
			require.NoError(t, err)
			require.Equal(t, []Result{{N: 0}, {N: 4}, {}}, results)
			require.Equal(t, "data", string(read[:4]))
		} else {
			require.ErrorIs(t, err, syscall.ENOENT)
			require.ErrorIs(t, results[0].Err, syscall.ENOENT)
			require.ErrorIs(t, results[1].Err, syscall.ECANCELED)
			require.ErrorIs(t, results[2].Err, syscall.ECANCELED)
		}
	}
	waitVFDOp(t, s, VOpenatOp(unix.AT_FDCWD, path, unix.O_RDONLY, 0, AllocateVFD).Link(VCloseOp(AllocateVFD)))
}

func TestResultsOnShutdown(t *testing.T) {
	for _, unplaced := range []bool{false, true} {
		root := WriteOp(nil, nil, 0).Link(ReadOp(nil, nil, 0))
		work := prepareForTest(root)
		ticket := Ticket{&work.completion}
		if unplaced {
			work.fail(syscall.EIO)
		} else {
			work.recordResult(&work.root, 3, nil)
			work.abandon(syscall.EIO)
		}
		_, err := ticket.Wait()
		require.ErrorIs(t, err, syscall.EIO)
		results := ticket.AllResults()
		if unplaced {
			require.ErrorIs(t, results[0].Err, syscall.EIO)
		} else {
			require.Equal(t, Result{N: 3}, results[0])
		}
		require.ErrorIs(t, results[1].Err, syscall.EIO)
	}
}

func TestResultsIncludeDurableSyncFailure(t *testing.T) {
	c := newTestCoordinator(t, 4, 1)
	f := testFile(t, 1)
	tickets, works := acceptOps(c, ReadOp(f, make([]byte, 1), 0).Link(VWriteOp(0, []byte("x"), 0).Durable()))
	completeForTest(c, works[0], 0, 1, nil)
	completeForTest(c, works[0], 1, 1, nil)
	c.placeReady(false)
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	_, err := tickets[0].Wait()
	require.ErrorIs(t, err, syscall.EBADF)
	results := tickets[0].AllResults()
	require.Equal(t, Result{N: 1}, results[0])
	require.Equal(t, 1, results[1].N)
	require.ErrorIs(t, results[1].Err, syscall.EBADF)
}

var resultStorageTicket Ticket

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

var allResultsSink []Result

func TestAllResultsAllocatesOnlyForStandalone(t *testing.T) {
	for _, count := range []int32{1, 2} {
		op := Op{}
		if count > 1 {
			op = op.Link(Op{})
		}
		work := prepareForTest(op)
		ticket := Ticket{&work.completion}
		ticket.completion.finish()
		_, err := ticket.Wait()
		require.NoError(t, err)
		allocs := testing.AllocsPerRun(100, func() { allResultsSink = ticket.AllResults() })
		want := float64(0)
		if count == 1 {
			want = 1
		}
		require.Equal(t, want, allocs)
	}
	allResultsSink = nil
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
	require.Zero(t, c.sched.config.disk.Stats().Writes.Depth)
	require.Zero(t, c.sched.config.disk.Stats().Writes.Bytes)
	require.EqualValues(t, 0, c.allocVFD(), "failed placement returns the reserved slot")
	c.freeVFD(0)
}

func TestUnknownResultsPreserveOutOfOrderCompletions(t *testing.T) {
	root := ReadOp(nil, nil, 0).Link(ReadOp(nil, nil, 0), ReadOp(nil, nil, 0))
	work := prepareForTest(root)
	ticket := Ticket{&work.completion}
	work.recordResult(work.op(2), 7, nil)
	work.abandon(syscall.EIO)
	_, err := ticket.Wait()
	require.ErrorIs(t, err, syscall.EIO)
	results := ticket.AllResults()
	require.ErrorIs(t, results[0].Err, syscall.EIO)
	require.ErrorIs(t, results[1].Err, syscall.EIO)
	require.Equal(t, Result{N: 7}, results[2])
}
