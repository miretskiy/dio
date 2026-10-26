package iosched

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestVFDAllocation(t *testing.T) {
	for _, capacity := range []uint32{0, 1, 63, 64, 65, 129} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			var c coordinator
			c.initVFiles(capacity)
			for i := range capacity {
				require.Equal(t, i, c.allocVFD())
			}
			require.Equal(t, AllocateVFD, c.allocVFD())
			for i := range capacity {
				c.freeVFD(i)
				require.Equal(t, i, c.allocVFD())
			}
			if capacity > 64 {
				c.freeVFD(0)
				c.freeVFD(capacity - 1)
				require.Equal(t, capacity-1, c.allocVFD())
				require.Zero(t, c.allocVFD(), "search must wrap around")
			}
			require.Equal(t, AllocateVFD, c.allocVFD())
		})
	}
}

func TestVFDAllocationDoesNotAllocate(t *testing.T) {
	var c coordinator
	c.initVFiles(65)
	require.Zero(t, testing.AllocsPerRun(1000, func() {
		c.freeVFD(c.allocVFD())
	}))
}

func newAllocatingScheduler(t *testing.T, capacity uint32) *URingScheduler {
	t.Helper()
	if !IOUringAvailable {
		t.Skip("io_uring unavailable")
	}
	s, err := NewURingScheduler(WithVFiles(capacity), WithRingDepth(16))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func waitVFDOp(t *testing.T, s *URingScheduler, op Op) int {
	t.Helper()
	ticket, err := s.Submit(op)
	require.NoError(t, err)
	n, err := ticket.Wait()
	require.NoError(t, err)
	return n
}

func TestAllocatedVFDChainAndReuse(t *testing.T) {
	s := newAllocatingScheduler(t, 1)
	path := filepath.Join(t.TempDir(), "data")
	want := []byte("linked virtual I/O")
	got := make([]byte, len(want))
	for range 4 {
		chain := VOpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR, 0o600, AllocateVFD).Link(
			VFallocateOp(AllocateVFD, 4096),
			VWriteOp(AllocateVFD, want, 0),
			VReadOp(AllocateVFD, got, 0),
			VFsyncOp(AllocateVFD),
			VCloseOp(AllocateVFD),
		)
		clear(got)
		require.Zero(t, waitVFDOp(t, s, chain))
		require.Equal(t, want, got)
	}
}

func TestAllocatedVFDSamePathUsesDistinctSlots(t *testing.T) {
	s := newAllocatingScheduler(t, 32)
	path := filepath.Join(t.TempDir(), "shared")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0o600))
	open := func() Op {
		return VOpenatOp(unix.AT_FDCWD, path, unix.O_RDONLY, 0, AllocateVFD).Link(VFsyncOp(AllocateVFD))
	}
	tickets := make([]Ticket, 32)
	errs := make([]error, len(tickets))
	var group sync.WaitGroup
	for i := range tickets {
		group.Go(func() { tickets[i], errs[i] = s.Submit(open()) })
	}
	group.Wait()
	seen := make(map[uint32]bool)
	for i, ticket := range tickets {
		require.NoError(t, errs[i])
		n, err := ticket.Wait()
		require.NoError(t, err)
		vfd := uint32(n)
		require.Less(t, vfd, uint32(len(tickets)))
		require.False(t, seen[vfd], "two opens reserved the same slot")
		seen[vfd] = true
	}
	full, err := s.Submit(open())
	require.NoError(t, err)
	_, err = full.Wait()
	require.ErrorIs(t, err, syscall.ENFILE)
	for vfd := range seen {
		waitVFDOp(t, s, VCloseOp(vfd))
	}
	waitVFDOp(t, s, open().Link(VCloseOp(AllocateVFD)))
}

func TestAllocatedVFDFailedOpenReturnsSlot(t *testing.T) {
	s := newAllocatingScheduler(t, 1)
	path := filepath.Join(t.TempDir(), "missing")
	ticket, err := s.Submit(VOpenatOp(unix.AT_FDCWD, path, unix.O_RDONLY, 0, AllocateVFD).Link(
		VWriteOp(AllocateVFD, []byte("canceled"), 0), VCloseOp(AllocateVFD)))
	require.NoError(t, err)
	_, err = ticket.Wait()
	require.ErrorIs(t, err, syscall.ENOENT)
	waitVFDOp(t, s, VOpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR, 0o600, AllocateVFD).Link(VCloseOp(AllocateVFD)))
}

func TestAllocatedVFDFailedFollowerKeepsOpenSlot(t *testing.T) {
	s := newAllocatingScheduler(t, 1)
	path := filepath.Join(t.TempDir(), "readonly")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0o600))
	ticket, err := s.Submit(VOpenatOp(unix.AT_FDCWD, path, unix.O_RDONLY, 0, AllocateVFD).Link(
		VWriteOp(AllocateVFD, []byte("fails"), 0), VCloseOp(AllocateVFD)))
	require.NoError(t, err)
	vfd, err := ticket.Wait()
	require.ErrorIs(t, err, syscall.EBADF)

	unused := filepath.Join(t.TempDir(), "unused")
	full, err := s.Submit(VOpenatOp(unix.AT_FDCWD, unused, unix.O_CREAT|unix.O_RDWR, 0o600, AllocateVFD))
	require.NoError(t, err)
	_, err = full.Wait()
	require.ErrorIs(t, err, syscall.ENFILE)
	_, err = os.Stat(unused)
	require.True(t, os.IsNotExist(err))
	waitVFDOp(t, s, VCloseOp(uint32(vfd)))
	waitVFDOp(t, s, VOpenatOp(unix.AT_FDCWD, path, unix.O_RDONLY, 0, AllocateVFD).Link(VCloseOp(AllocateVFD)))
}

func TestAllocatedVFDFailedOpenRetainsSlotThroughHardLink(t *testing.T) {
	c := newTestCoordinator(t, 8, 1)
	f := testFile(t, 1)
	op := VOpenatOp(unix.AT_FDCWD, "missing", unix.O_RDONLY, 0, AllocateVFD).HardLink(WriteOp(f, []byte("x"), 0))
	ticket, submitErr := c.sched.Submit(op)
	require.NoError(t, submitErr)
	c.accept(c.sched.takeStaged())
	work := c.accepted.tail
	completeForTest(c, work, 0, 0, syscall.ENOENT)
	require.Equal(t, AllocateVFD, c.allocVFD(), "failed open released its slot while followers remained")
	completeForTest(c, work, 1, 1, nil)
	_, err := ticket.Wait()
	require.ErrorIs(t, err, syscall.ENOENT)
	require.Zero(t, c.allocVFD())
}

func TestAllocatedVFDAdmissionFailureReturnsSlot(t *testing.T) {
	c := newTestCoordinator(t, 8, 1)
	f := testFile(t, 1)
	_, works := acceptOps(c, DrainOp(f))
	op := VOpenatOp(unix.AT_FDCWD, "unused", unix.O_RDONLY, 0, AllocateVFD).Link(ReadOp(f, make([]byte, 1), 0))
	ticket, submitErr := c.sched.Submit(op)
	require.NoError(t, submitErr)
	c.accept(c.sched.takeStaged())
	_, err := ticket.Wait()
	require.ErrorContains(t, err, "lifecycle barrier")
	require.Zero(t, c.allocVFD())
	completeForTest(c, works[0], 0, 0, nil)
}

func TestCoordinatorResolvesVFDChainInPlace(t *testing.T) {
	c := newTestCoordinator(t, 8, 1)
	op := VOpenatOp(unix.AT_FDCWD, "unused", unix.O_RDONLY, 0, AllocateVFD).Link(
		VFsyncOp(AllocateVFD), VCloseOp(AllocateVFD))
	before := c.vfiles[0]
	ticket, err := c.sched.Submit(op)
	require.NoError(t, err)
	require.Equal(t, before, c.vfiles[0], "Submit reserved a slot")
	require.Equal(t, AllocateVFD, op.followers()[0].vfd, "Submit resolved a follower")

	c.accept(c.sched.takeStaged())
	work := c.accepted.head
	require.NotSame(t, &op.followers()[0], &work.linked[0], "submission owns the mutable chain")
	for i := range work.opCount() {
		node := work.op(i)
		require.Zero(t, node.vfd)
	}
	for i := range uint32(3) {
		completeForTest(c, work, i, 0, nil)
	}
	n, err := ticket.Wait()
	require.NoError(t, err)
	require.Zero(t, n)
	require.Equal(t, before, c.vfiles[0], "close did not return the slot")
}

func TestVFDResolutionDoesNotAllocate(t *testing.T) {
	var c coordinator
	c.initVFiles(1)
	op := VOpenatOp(unix.AT_FDCWD, "unused", unix.O_RDONLY, 0, AllocateVFD).Link(
		VReadOp(AllocateVFD, nil, 0), VCloseOp(AllocateVFD))
	work := prepareForTest(op)
	require.Zero(t, testing.AllocsPerRun(1000, func() {
		for i := range work.opCount() {
			op := work.op(i)
			op.vfd = AllocateVFD
		}
		if err := c.prepareVFiles(work); err != nil {
			panic(err)
		}
		c.freeVFD(work.root.vfd)
	}))
}

func TestAllocatedVFDExhaustionLinkSemantics(t *testing.T) {
	for _, test := range []struct {
		name    string
		chain   func(Op, *os.File, uint32) Op
		written bool
	}{
		{
			name: "soft link cancels",
			chain: func(open Op, f *os.File, _ uint32) Op {
				return open.Link(WriteOp(f, []byte("after!"), 0))
			},
		},
		{
			name: "soft link cancels hard-linked tail",
			chain: func(open Op, f *os.File, _ uint32) Op {
				return open.Link(VFsyncOp(AllocateVFD).HardLink(WriteOp(f, []byte("after!"), 0)))
			},
		},
		{
			name: "hard link continues",
			chain: func(open Op, f *os.File, _ uint32) Op {
				return open.HardLink(WriteOp(f, []byte("after!"), 0))
			},
			written: true,
		},
		{
			name: "hard link continues on an existing virtual file",
			chain: func(open Op, _ *os.File, held uint32) Op {
				return open.HardLink(VWriteOp(held, []byte("after!"), 0))
			},
			written: true,
		},
		{
			name: "unallocated follower cancels its soft link",
			chain: func(open Op, f *os.File, _ uint32) Op {
				return open.HardLink(VWriteOp(AllocateVFD, []byte("bad"), 0).
					Link(WriteOp(f, []byte("after!"), 0)))
			},
		},
		{
			name: "unallocated follower continues its hard link",
			chain: func(open Op, f *os.File, _ uint32) Op {
				return open.HardLink(VWriteOp(AllocateVFD, []byte("bad"), 0).
					HardLink(WriteOp(f, []byte("after!"), 0)))
			},
			written: true,
		},
		{
			name: "unallocated close continues its hard link",
			chain: func(open Op, f *os.File, _ uint32) Op {
				return open.HardLink(VCloseOp(AllocateVFD).HardLink(WriteOp(f, []byte("after!"), 0)))
			},
			written: true,
		},
		{
			name: "hard link completes durable follower",
			chain: func(open Op, f *os.File, _ uint32) Op {
				return open.HardLink(WriteOp(f, []byte("after!"), 0).Durable())
			},
			written: true,
		},
		{
			name: "soft link cancels durable follower",
			chain: func(open Op, f *os.File, _ uint32) Op {
				return open.Link(WriteOp(f, []byte("after!"), 0).Durable())
			},
		},
		{
			name: "unallocated durable follower fails",
			chain: func(open Op, _ *os.File, _ uint32) Op {
				return open.HardLink(VWriteOp(AllocateVFD, []byte("bad"), 0).Durable())
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newAllocatingScheduler(t, 1)
			f := testFile(t, 6)
			_, err := f.WriteAt([]byte("before"), 0)
			require.NoError(t, err)
			held := uint32(waitVFDOp(t, s, VOpenatOp(unix.AT_FDCWD, f.Name(), unix.O_RDWR, 0, AllocateVFD)))
			unused := filepath.Join(t.TempDir(), "unused")
			open := VOpenatOp(unix.AT_FDCWD, unused, unix.O_CREAT|unix.O_RDWR, 0o600, AllocateVFD)
			ticket, err := s.Submit(test.chain(open, f, held))
			require.NoError(t, err)
			n, err := ticket.Wait()
			require.Zero(t, n, "a follower overwrote the failed open's result")
			require.ErrorIs(t, err, syscall.ENFILE, "the root error must survive follower completion")
			got, err := os.ReadFile(f.Name())
			require.NoError(t, err)
			want := "before"
			if test.written {
				want = "after!"
			}
			require.Equal(t, want, string(got))
			_, err = os.Stat(unused)
			require.True(t, os.IsNotExist(err), "exhausted open executed")
			waitVFDOp(t, s, VCloseOp(held))
		})
	}
}

func TestAllocatedVFDExhaustionHardLinkReleasesSlot(t *testing.T) {
	s := newAllocatingScheduler(t, 1)
	f := testFile(t, 1)
	held := uint32(waitVFDOp(t, s, VOpenatOp(unix.AT_FDCWD, f.Name(), unix.O_RDONLY, 0, AllocateVFD)))
	ticket, err := s.Submit(VOpenatOp(unix.AT_FDCWD, f.Name(), unix.O_RDONLY, 0, AllocateVFD).
		HardLink(VCloseOp(held)))
	require.NoError(t, err)
	_, err = ticket.Wait()
	require.ErrorIs(t, err, syscall.ENFILE)
	waitVFDOp(t, s, VOpenatOp(unix.AT_FDCWD, f.Name(), unix.O_RDONLY, 0, AllocateVFD).Link(VCloseOp(AllocateVFD)))
}

func TestAllocatedVFDExhaustionWaitsForHardLinkedIO(t *testing.T) {
	c := newTestCoordinator(t, 4, 1)
	require.Zero(t, c.allocVFD())
	f, release := blockingRead(t)
	tickets, works := acceptOps(c, VOpenatOp(unix.AT_FDCWD, "unused", unix.O_RDONLY, 0, AllocateVFD).
		HardLink(ReadOp(f, make([]byte, 8), 0)))
	require.Len(t, works, 1)
	placed, _ := c.placeReady(false)
	require.Equal(t, 1, placed, "only the hard-linked read needs an SQE")
	require.Equal(t, workIssued, works[0].state)
	require.EqualValues(t, 1, works[0].remaining)
	require.ErrorIs(t, works[0].err, syscall.ENFILE)
	release()
	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	n, err := tickets[0].Wait()
	require.Zero(t, n)
	require.ErrorIs(t, err, syscall.ENFILE)
	require.Zero(t, c.accepted.len)
}

func TestAllocatedVFDExhaustionReturnsAdmission(t *testing.T) {
	c := withDefaultBudget(newTestCoordinator(t, 4, 1))
	require.Zero(t, c.allocVFD())
	f := testFile(t, 1)
	tickets, _ := acceptOps(c, VOpenatOp(unix.AT_FDCWD, "unused", unix.O_RDONLY, 0, AllocateVFD).
		Link(WriteOp(f, []byte("x"), 0).Durable()))
	placed, _ := c.placeReady(false)
	require.Zero(t, placed)
	require.Zero(t, c.accepted.len)
	require.Zero(t, c.ready.len)
	require.Empty(t, c.syncPending)
	require.Zero(t, c.sched.config.disk.Stats().Writes.ServiceTime)
	require.Zero(t, c.sched.config.disk.Stats().Writes.Bytes)
	n, err := tickets[0].Wait()
	require.Zero(t, n)
	require.ErrorIs(t, err, syscall.ENFILE)
}
