//go:build linux

package iosched

import (
	"errors"
	"os"
	"testing"

	"github.com/miretskiy/dio/internal/intrusive"
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
			c := newTestCoordinator(8, 2, &fakeRingQueue{})
			_, handles := acceptOps(&c, tc.ops...)
			front, ok := c.ready.Front()
			require.True(t, ok)
			run := c.coalescedRun(front, []intrusive.Handle{handles[0]})
			require.Len(t, run, tc.want)

			c.stopping = true
			c.failAllWork(errors.New("test cleanup"))
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

func TestOpBytes(t *testing.T) {
	writev := WritevOp(os.NewFile(100, "a"), [][]byte{make([]byte, 3), nil, make([]byte, 5)}, 0)
	require.Equal(t, 8, opBytes(&writev))
}
