package iosched

import (
	"io"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func groupLen(leader *Ticket) int {
	n := 1
	for m := leader.group; m != nil; m = m.next {
		n++
	}
	return n
}

func TestCoalesceWrites(t *testing.T) {
	fa := os.NewFile(100, "a")
	fb := os.NewFile(101, "b")
	w := func(f *os.File, off int64, n int) *Ticket { return &Ticket{Op: WriteOp(f, make([]byte, n), off)} }
	vw := func(vfd uint32, off int64, n int) *Ticket { return &Ticket{Op: VWriteOp(vfd, make([]byte, n), off)} }
	rd := func(f *os.File, off int64, n int) *Ticket { return &Ticket{Op: ReadOp(f, make([]byte, n), off)} }

	t.Run("contiguous in submission order merges into one writev leader", func(t *testing.T) {
		out := coalesceWrites([]*Ticket{w(fa, 0, 4), w(fa, 4, 6), w(fa, 10, 2)})
		require.Len(t, out, 1)
		leader := out[0]
		require.Equal(t, OpWritev, leader.Op.kind())
		require.Equal(t, int64(0), leader.Op.offset) // first write is the leader
		require.Equal(t, 3, groupLen(leader))
	})

	t.Run("out-of-order input coalesces only adjacent-contiguous (no sort)", func(t *testing.T) {
		// w(10,2) can't extend nothing; w(0,4) starts a run; w(4,6) extends it. So we
		// get a singleton plus a 2-member run, not one big writev — the merge we
		// deliberately skip by not sorting.
		out := coalesceWrites([]*Ticket{w(fa, 10, 2), w(fa, 0, 4), w(fa, 4, 6)})
		require.Len(t, out, 2)
		require.Equal(t, OpWrite, out[0].Op.kind()) // w(10,2) singleton
		require.Equal(t, OpWritev, out[1].Op.kind())
		require.Equal(t, 2, groupLen(out[1]))
	})

	t.Run("gap splits into two singletons", func(t *testing.T) {
		out := coalesceWrites([]*Ticket{w(fa, 0, 4), w(fa, 100, 4)})
		require.Len(t, out, 2)
		for _, l := range out {
			require.Equal(t, OpWrite, l.Op.kind()) // untouched singletons
			require.Nil(t, l.group)
		}
	})

	t.Run("overlap does not merge", func(t *testing.T) {
		out := coalesceWrites([]*Ticket{w(fa, 0, 4), w(fa, 0, 4)})
		require.Len(t, out, 2)
	})

	t.Run("same file separated by another target does not coalesce", func(t *testing.T) {
		// fa[0,4] and fa[4,4] are contiguous but the fb write between them breaks the
		// run; without a sort we accept the missed merge. All three pass through.
		out := coalesceWrites([]*Ticket{w(fa, 0, 4), w(fb, 0, 4), w(fa, 4, 4)})
		require.Len(t, out, 3)
		for _, l := range out {
			require.Equal(t, OpWrite, l.Op.kind())
			require.Nil(t, l.group)
		}
	})

	t.Run("non-coalescible op breaks the run", func(t *testing.T) {
		out := coalesceWrites([]*Ticket{w(fa, 0, 4), rd(fa, 4, 4), w(fa, 4, 4)})
		require.Len(t, out, 3) // write, read, write — the read splits the two writes
		require.Equal(t, OpWrite, out[0].Op.kind())
		require.Equal(t, OpRead, out[1].Op.kind())
		require.Equal(t, OpWrite, out[2].Op.kind())
	})

	t.Run("regular and virtual never merge", func(t *testing.T) {
		out := coalesceWrites([]*Ticket{vw(0, 0, 4), w(fa, 0, 4)})
		require.Len(t, out, 2)
	})

	t.Run("virtual contiguous merges", func(t *testing.T) {
		out := coalesceWrites([]*Ticket{vw(1, 0, 4), vw(1, 4, 4)})
		require.Len(t, out, 1)
		require.Equal(t, OpWritev, out[0].Op.kind())
		require.True(t, out[0].Op.isVirtual()) // virtual bit preserved
		require.Equal(t, 2, groupLen(out[0]))
	})
}

func TestCoalescibleWrite(t *testing.T) {
	fa := os.NewFile(100, "a")
	buf := make([]byte, 4)
	cases := []struct {
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
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			op := tc.op
			require.Equal(t, tc.want, op.coalescibleWrite())
		})
	}
}

func TestCompleteCoalescedGroup(t *testing.T) {
	fa := os.NewFile(100, "a")
	// leader(4) + follower(6); leader keeps its own buf for its length. reap
	// records each op's result and drives leader.pending to zero before calling
	// completeCoalescedGroup, so the fixtures start the leader at pending zero.
	newLeader := func() (*Ticket, *Ticket) {
		leader := &Ticket{Op: WriteOp(fa, make([]byte, 4), 0)}
		follower := &Ticket{Op: WriteOp(fa, make([]byte, 6), 4)}
		leader.pending.Store(0)
		leader.wg.Add(1)
		follower.pending.Store(1)
		follower.wg.Add(1)
		leader.Op.opcode = OpWritev
		leader.group = follower
		return leader, follower
	}

	t.Run("full success splits by buffer length", func(t *testing.T) {
		leader, follower := newLeader()
		leader.Op.result.N = 10 // writev wrote all 10 bytes
		completeCoalescedGroup(leader)
		leader.Wait()
		follower.Wait()
		require.NoError(t, leader.Op.result.Err)
		require.NoError(t, follower.Op.result.Err)
		require.Equal(t, 4, leader.Op.result.N)
		require.Equal(t, 6, follower.Op.result.N)
	})

	t.Run("short write: leader fully covered, boundary member short", func(t *testing.T) {
		leader, follower := newLeader()
		leader.Op.result.N = 8 // writev wrote 8 of 10; leader(4) full, follower gets 4 of 6
		completeCoalescedGroup(leader)
		leader.Wait()
		follower.Wait()
		require.NoError(t, leader.Op.result.Err)
		require.Equal(t, 4, leader.Op.result.N)
		require.ErrorIs(t, follower.Op.result.Err, io.ErrShortWrite)
		require.Equal(t, 4, follower.Op.result.N)
	})

	t.Run("short write within leader zeroes followers", func(t *testing.T) {
		leader, follower := newLeader()
		leader.Op.result.N = 3 // < leader's 4: leader partial (3), follower 0
		completeCoalescedGroup(leader)
		leader.Wait()
		follower.Wait()
		require.ErrorIs(t, leader.Op.result.Err, io.ErrShortWrite)
		require.Equal(t, 3, leader.Op.result.N)
		require.ErrorIs(t, follower.Op.result.Err, io.ErrShortWrite)
		require.Equal(t, 0, follower.Op.result.N)
	})

	t.Run("write error zeroes all with errno", func(t *testing.T) {
		leader, follower := newLeader()
		leader.Op.result.Err = syscall.EIO
		completeCoalescedGroup(leader)
		leader.Wait()
		follower.Wait()
		require.ErrorIs(t, leader.Op.result.Err, syscall.EIO)
		require.Equal(t, 0, leader.Op.result.N)
		require.ErrorIs(t, follower.Op.result.Err, syscall.EIO)
		require.Equal(t, 0, follower.Op.result.N)
	})

	t.Run("fdatasync error fails all", func(t *testing.T) {
		leader, follower := newLeader()
		leader.Op.result.N = 10 // writev succeeded
		sync := VFdatasyncOp(0)
		sync.result.Err = syscall.EIO // durability failed
		leader.Op.linked = &sync
		completeCoalescedGroup(leader)
		leader.Wait()
		follower.Wait()
		require.ErrorIs(t, leader.Op.result.Err, syscall.EIO)
		require.ErrorIs(t, follower.Op.result.Err, syscall.EIO)
	})
}

func TestFailCoalescedWrite(t *testing.T) {
	fa := os.NewFile(100, "a")
	leader := &Ticket{Op: WriteOp(fa, make([]byte, 4), 0)}
	follower := &Ticket{Op: WriteOp(fa, make([]byte, 6), 4)}
	leader.pending.Store(2) // writev + linked fdatasync
	leader.wg.Add(1)
	follower.pending.Store(1)
	follower.wg.Add(1)
	leader.Op.opcode = OpWritev
	leader.group = follower

	// Idempotent: failAllInflight can reach a multi-SQE leader once per SQE, so a
	// second call must be a no-op, not a double wg.Done.
	failCoalescedWrite(leader, syscall.EIO)
	failCoalescedWrite(leader, syscall.EIO)

	leader.Wait()
	follower.Wait()
	require.ErrorIs(t, leader.Op.result.Err, syscall.EIO)
	require.ErrorIs(t, follower.Op.result.Err, syscall.EIO)
	require.Equal(t, int32(0), leader.pending.Load())
	require.Equal(t, int32(0), follower.pending.Load())
}
