//go:build linux

package iosched

import (
	"errors"
	"os"
	"testing"

	"github.com/miretskiy/dio/giouring"
	"github.com/stretchr/testify/require"
)

func TestCoordinatorDetectsDurableWrite(t *testing.T) {
	regular := os.NewFile(100, "regular")
	durable := func(ops ...Op) bool {
		c := newTestCoordinator(8, 1, &fakeRingQueue{})
		_, handles := acceptOps(&c, ops...)
		got := c.durableWrite(handles)
		c.failRemaining(nil, errors.New("test cleanup"))
		return got
	}
	require.True(t, durable(VWriteOp(0, make([]byte, 8), 0).Durable()))
	require.False(t, durable(VWriteOp(0, make([]byte, 8), 0)))
	require.True(t, durable(WriteOp(regular, make([]byte, 8), 0).Durable()))
	require.True(t, durable(
		VWriteOp(0, make([]byte, 8), 0),
		VWriteOp(0, make([]byte, 8), 8).Durable(),
	))
	require.False(t, durable(VReadOp(0, make([]byte, 8), 0)))
}

func TestPlaceDurableWriteWithLinkedSync(t *testing.T) {
	ring := &fakeRingQueue{}
	c := newTestCoordinator(2, 1, ring)
	tickets, handles := acceptOps(&c, VWriteOp(0, make([]byte, 8), 0).Durable())

	c.placeReady(true)
	require.Len(t, ring.sqes, 2)
	require.Equal(t, uint8(giouring.OpWrite), ring.sqes[0].OpCode)
	require.NotZero(t, ring.sqes[0].Flags&giouring.SqeIOLink)
	require.Equal(t, uint8(giouring.OpFsync), ring.sqes[1].OpCode)
	require.Equal(t, giouring.FsyncDatasync, ring.sqes[1].OpcodeFlags)
	completion := c.pending.Value(handles[0]).writeGroup
	require.Equal(t, 1, completion.count)
	require.Empty(t, completion.overflow)
	require.Equal(t, handles[0], completion.inline[0].work)

	c.failRemaining(nil, errors.New("test cleanup"))
	c.releaseAllSlots()
	tickets[0].Wait()
}

func TestPlacePlainWriteWithoutSync(t *testing.T) {
	ring := &fakeRingQueue{}
	c := newTestCoordinator(2, 1, ring)
	tickets, _ := acceptOps(&c, VWriteOp(0, make([]byte, 8), 0))

	c.placeReady(true)
	require.Len(t, ring.sqes, 1)
	require.Equal(t, uint8(giouring.OpWrite), ring.sqes[0].OpCode)
	require.Zero(t, ring.sqes[0].Flags&giouring.SqeIOLink)

	c.failRemaining(nil, errors.New("test cleanup"))
	c.releaseAllSlots()
	tickets[0].Wait()
}
