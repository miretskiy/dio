package iosched_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
)

func TestPOSIXUnlink(t *testing.T) {
	s := iosched.NewPOSIXScheduler()
	defer func() { require.NoError(t, s.Close()) }()
	testUnlink(t, s)
}

func testUnlink(t *testing.T, s iosched.Scheduler) {
	dir := t.TempDir()
	d, err := os.Open(dir)
	require.NoError(t, err)
	defer d.Close()
	path := filepath.Join(dir, "victim")
	require.NoError(t, os.WriteFile(path, []byte("value"), 0600))
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	_, err = submitAndWait(t, s, iosched.UnlinkatOp(int(d.Fd()), "victim"))
	require.NoError(t, err)
	require.NoFileExists(t, path)
	// Unlink removes a name; an outstanding reader retains its descriptor.
	buf := make([]byte, 5)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, "value", string(buf))

	require.NoError(t, os.WriteFile(path, []byte("next"), 0600))
	_, err = submitAndWait(t, s, iosched.UnlinkatOp(int(d.Fd()), "missing").HardLink(iosched.UnlinkatOp(int(d.Fd()), "victim")))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoFileExists(t, path) // a failed hard-linked unlink does not cancel its peer
	for _, invalid := range []string{"", "bad\x00path"} {
		_, err := s.Submit(iosched.UnlinkatOp(int(d.Fd()), invalid))
		require.Error(t, err)
	}
}
