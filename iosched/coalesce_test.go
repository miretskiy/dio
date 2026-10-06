//go:build linux

package iosched

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
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
			require.NotNil(t, c.ready.head)
			require.Equal(t, tc.want, c.coalescedRun(c.ready.head))
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
	require.Equal(t, handles, handles[0].coalesced, "the leader does not list the run in writev order")

	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	for i, want := range []int{4, 6, 2} {
		n, err := tickets[i].Wait()
		require.NoError(t, err)
		require.Equal(t, want, n)
	}
	require.Nil(t, handles[0].coalesced, "the completed leader still references its run")
}

// TestCoalescedRunCompletesEveryMember checks that finishing the leader, which
// completes its ticket first, does not cut the rest of the run short.
func TestCoalescedRunCompletesEveryMember(t *testing.T) {
	c := newTestCoordinator(t, 8, 0)
	ops := make([]Op, 6)
	f := testFile(t, len(ops)*4)
	for i := range ops {
		ops[i] = WriteOp(f, make([]byte, 4), int64(i*4))
	}
	tickets, handles := acceptOps(c, ops...)

	c.placeReady(true)
	require.Len(t, handles[0].coalesced, len(ops))

	submitAndReapUntil(t, c, func() bool { return c.occupied == 0 })
	for _, ticket := range tickets {
		n, err := ticket.Wait()
		require.NoError(t, err)
		require.Equal(t, 4, n)
	}
}

// The kernel cannot be made to write short, so the short-write tests deliver
// the write's completion themselves.

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

func TestOpBytes(t *testing.T) {
	writev := WritevOp(os.NewFile(100, "a"), [][]byte{make([]byte, 3), nil, make([]byte, 5)}, 0)
	require.Equal(t, 8, opBytes(&writev))
}
