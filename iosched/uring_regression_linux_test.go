//go:build linux

package iosched_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/iosched"
	"github.com/miretskiy/dio/sys"
)

func TestURing_ReusedSQEWriteAfterFdatasync(t *testing.T) {
	s := newURingSched(t)

	path := filepath.Join(t.TempDir(), "direct.dat")
	f, err := sys.CreateDirect(path, sys.FlDirectIO)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	payload := align.AllocAligned(align.BlockSize)
	t.Cleanup(func() { align.FreeAligned(payload) })
	for i := range payload {
		payload[i] = byte(i)
	}

	ticket, err := s.Submit([]iosched.Op{iosched.WriteOp(f, payload, 0)})
	require.NoError(t, err)
	var writeRes [1]iosched.Result
	_, err = s.Wait(ticket, writeRes[:])
	require.NoError(t, err)
	require.NoError(t, writeRes[0].Err)
	require.Equal(t, len(payload), writeRes[0].N)

	ticket, err = s.Submit([]iosched.Op{iosched.FdatasyncOp(f)})
	require.NoError(t, err)
	var syncRes [1]iosched.Result
	_, err = s.Wait(ticket, syncRes[:])
	require.NoError(t, err)
	require.NoError(t, syncRes[0].Err)

	ticket, err = s.Submit([]iosched.Op{iosched.WriteOp(f, payload, int64(len(payload)))})
	require.NoError(t, err)
	_, err = s.Wait(ticket, writeRes[:])
	require.NoError(t, err)
	require.NoError(t, writeRes[0].Err)
	require.Equal(t, len(payload), writeRes[0].N)
}
