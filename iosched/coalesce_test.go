//go:build linux

package iosched

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/miretskiy/dio/giouring"
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

			c.failRemaining(nil, errors.New("test cleanup"))
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
	ring := &fakeRingQueue{}
	c := newTestCoordinator(8, 0, ring)
	f := os.NewFile(100, "a")
	tickets, handles := acceptOps(&c,
		WriteOp(f, make([]byte, 4), 0),
		WriteOp(f, make([]byte, 6), 4),
		WriteOp(f, make([]byte, 2), 10),
	)

	c.placeReady(true)
	require.Len(t, ring.sqes, 1)
	require.Equal(t, uint8(giouring.OpWritev), ring.sqes[0].OpCode)
	slot := c.slots.Value(intrusive.Handle(ring.sqes[0].UserData))
	require.Len(t, slot.iovecs, 3)
	completion := c.pending.Value(handles[0]).writeGroup
	require.Equal(t, 3, completion.count)
	last := len(handles) - 1
	if last < len(completion.inline) {
		require.Equal(t, handles[last], completion.inline[last].work)
	} else {
		require.Equal(t, handles[last], completion.overflow[last-len(completion.inline)].work)
	}

	c.failRemaining(nil, errors.New("test cleanup"))
	c.releaseAllSlots()
	for _, ticket := range tickets {
		ticket.Wait()
	}
}

func TestWriteCompletionSnapshotSurvivesLeaderRemoval(t *testing.T) {
	ring := &fakeRingQueue{}
	c := newTestCoordinator(8, 0, ring)
	f := os.NewFile(100, "a")
	inlineTargets := len((writeGroupCompletion{}).inline)
	ops := make([]Op, inlineTargets+1)
	for i := range ops {
		ops[i] = WriteOp(f, make([]byte, 4), int64(i*4))
	}
	tickets, handles := acceptOps(&c, ops...)

	c.placeReady(true)
	completion := c.pending.Value(handles[0]).writeGroup
	require.Equal(t, len(ops), completion.count)
	require.Len(t, completion.overflow, 1)
	require.Equal(t, handles[inlineTargets], completion.overflow[0].work)

	ring.complete(ring.sqes[0].UserData, int32(len(ops)*4))
	c.reap()
	for _, ticket := range tickets {
		ticket.Wait()
		require.NoError(t, ticket.Error())
		require.Equal(t, 4, ticket.N())
	}
}

func TestCoalescedShortWriteCompletion(t *testing.T) {
	ring := &fakeRingQueue{}
	c := newTestCoordinator(8, 0, ring)
	f := os.NewFile(100, "a")
	tickets, _ := acceptOps(&c,
		WriteOp(f, make([]byte, 4), 0),
		WriteOp(f, make([]byte, 4), 4),
	)
	c.placeReady(true)
	require.Len(t, ring.sqes, 1)

	ring.complete(ring.sqes[0].UserData, 6)
	c.reap()
	for _, ticket := range tickets {
		ticket.Wait()
	}
	require.NoError(t, tickets[0].Error())
	require.Equal(t, 4, tickets[0].N())
	require.ErrorIs(t, tickets[1].Error(), io.ErrShortWrite)
	require.Equal(t, 2, tickets[1].N())
}

func TestSingleShortWriteCompletion(t *testing.T) {
	ring := &fakeRingQueue{}
	c := newTestCoordinator(1, 0, ring)
	f := os.NewFile(100, "a")
	tickets, _ := acceptOps(&c, WriteOp(f, make([]byte, 4), 0))
	c.placeReady(true)
	require.Len(t, ring.sqes, 1)

	ring.complete(ring.sqes[0].UserData, 2)
	c.reap()
	tickets[0].Wait()
	require.ErrorIs(t, tickets[0].Error(), io.ErrShortWrite)
	require.Equal(t, 2, tickets[0].N())
}

func TestOpBytes(t *testing.T) {
	writev := WritevOp(os.NewFile(100, "a"), [][]byte{make([]byte, 3), nil, make([]byte, 5)}, 0)
	require.Equal(t, 8, opBytes(&writev))
}
