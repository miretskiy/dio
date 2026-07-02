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

func TestGroupCoalescibleWrites(t *testing.T) {
	fa := os.NewFile(100, "a")
	fb := os.NewFile(101, "b")
	w := func(f *os.File, off int64, n int) *Ticket { return &Ticket{Op: WriteOp(f, make([]byte, n), off)} }
	vw := func(vfd uint32, off int64, n int) *Ticket { return &Ticket{Op: VWriteOp(vfd, make([]byte, n), off)} }

	t.Run("contiguous merges into one writev leader", func(t *testing.T) {
		// Deliberately unordered input; grouping sorts by offset.
		out := groupCoalescibleWrites([]*Ticket{w(fa, 10, 2), w(fa, 0, 4), w(fa, 4, 6)})
		require.Len(t, out, 1)
		leader := out[0]
		require.Equal(t, OpWritev, leader.Op.base())
		require.Equal(t, int64(0), leader.Op.offset) // lowest offset is the leader
		require.Equal(t, 3, groupLen(leader))
		require.Len(t, leader.Op.iovecs, 3)
	})

	t.Run("gap splits into two runs", func(t *testing.T) {
		out := groupCoalescibleWrites([]*Ticket{w(fa, 0, 4), w(fa, 100, 4)})
		require.Len(t, out, 2)
		for _, l := range out {
			require.Equal(t, OpWrite, l.Op.base()) // untouched singletons
			require.Nil(t, l.group)
		}
	})

	t.Run("overlap does not merge", func(t *testing.T) {
		out := groupCoalescibleWrites([]*Ticket{w(fa, 0, 4), w(fa, 0, 4)})
		require.Len(t, out, 2)
	})

	t.Run("different files stay separate", func(t *testing.T) {
		out := groupCoalescibleWrites([]*Ticket{w(fa, 0, 4), w(fb, 0, 4), w(fa, 4, 4)})
		require.Len(t, out, 2) // fa[0,4] merged; fb[0] singleton
		var merged, single int
		for _, l := range out {
			if l.Op.base() == OpWritev {
				merged++
				require.Equal(t, 2, groupLen(l))
			} else {
				single++
			}
		}
		require.Equal(t, 1, merged)
		require.Equal(t, 1, single)
	})

	t.Run("regular and virtual never merge", func(t *testing.T) {
		out := groupCoalescibleWrites([]*Ticket{vw(0, 0, 4), w(fa, 0, 4)})
		require.Len(t, out, 2)
	})

	t.Run("virtual contiguous merges", func(t *testing.T) {
		out := groupCoalescibleWrites([]*Ticket{vw(1, 0, 4), vw(1, 4, 4)})
		require.Len(t, out, 1)
		require.Equal(t, OpWritev, out[0].Op.base())
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

func TestCompleteCoalescedWrite(t *testing.T) {
	fa := os.NewFile(100, "a")
	// leader(4) + follower(6); leader keeps its own buf for its length.
	newLeader := func() (*Ticket, *Ticket) {
		leader := &Ticket{Op: WriteOp(fa, make([]byte, 4), 0)}
		follower := &Ticket{Op: WriteOp(fa, make([]byte, 6), 4)}
		leader.pending.Store(1)
		leader.wg.Add(1)
		follower.pending.Store(1)
		follower.wg.Add(1)
		leader.Op.opcode = OpWritev
		leader.group = follower
		return leader, follower
	}

	t.Run("full success splits by buffer length", func(t *testing.T) {
		leader, follower := newLeader()
		completeCoalescedWrite(leader, 10)
		leader.Wait()
		follower.Wait()
		require.NoError(t, leader.Op.Result.Err)
		require.NoError(t, follower.Op.Result.Err)
		require.Equal(t, 4, leader.Op.Result.N)
		require.Equal(t, 6, follower.Op.Result.N)
	})

	t.Run("short write fails all, retriable", func(t *testing.T) {
		leader, follower := newLeader()
		completeCoalescedWrite(leader, 8) // < 10
		leader.Wait()
		follower.Wait()
		require.ErrorIs(t, leader.Op.Result.Err, io.ErrShortWrite)
		require.ErrorIs(t, follower.Op.Result.Err, io.ErrShortWrite)
	})

	t.Run("error fails all with errno", func(t *testing.T) {
		leader, follower := newLeader()
		completeCoalescedWrite(leader, -int32(syscall.EIO))
		leader.Wait()
		follower.Wait()
		require.ErrorIs(t, leader.Op.Result.Err, syscall.EIO)
		require.ErrorIs(t, follower.Op.Result.Err, syscall.EIO)
	})
}
