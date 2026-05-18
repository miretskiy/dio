//go:build linux

package giouring

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareRWClearsReusedSQEFields(t *testing.T) {
	entry := &SubmissionQueueEntry{
		Flags:       0xff,
		IoPrio:      0xffff,
		OpcodeFlags: 0xdeadbeef,
		UserData:    0xfeedface,
		BufIG:       0xbeef,
		Personality: 0xcafe,
		SpliceFdIn:  123,
		Addr3:       456,
		_pad2:       [1]uint64{789},
	}

	entry.PrepareWrite(10, 100, 200, 300)

	require.EqualValues(t, OpWrite, entry.OpCode)
	require.EqualValues(t, 0, entry.Flags)
	require.EqualValues(t, 0, entry.IoPrio)
	require.EqualValues(t, 10, entry.Fd)
	require.EqualValues(t, 300, entry.Off)
	require.EqualValues(t, 100, entry.Addr)
	require.EqualValues(t, 200, entry.Len)
	require.EqualValues(t, 0, entry.OpcodeFlags)
	require.EqualValues(t, 0, entry.UserData)
	require.EqualValues(t, 0, entry.BufIG)
	require.EqualValues(t, 0, entry.Personality)
	require.EqualValues(t, 0, entry.SpliceFdIn)
	require.EqualValues(t, 0, entry.Addr3)
	require.Equal(t, [1]uint64{}, entry._pad2)
}
