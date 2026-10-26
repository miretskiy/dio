package iosched_test

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/v2/iosched"
)

func writeTempFile(t *testing.T, size int) (path string, data []byte) {
	t.Helper()
	data = make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	path = filepath.Join(t.TempDir(), "test.dat")
	require.NoError(t, os.WriteFile(path, data, 0644))
	return path, data
}

func openRWFile(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	return f
}

func submitAndWait(t *testing.T, s iosched.Scheduler, op iosched.Op) (int, error) {
	t.Helper()
	ticket, err := s.Submit(op)
	require.NoError(t, err)
	return ticket.Wait()
}

func TestNewDefaultScheduler(t *testing.T) {
	s := iosched.NewDefaultScheduler()
	require.NotNil(t, s)
	require.NoError(t, s.Close())
}

func TestPOSIXScheduler(t *testing.T) {
	s := iosched.NewPOSIXScheduler()
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	path := filepath.Join(t.TempDir(), "posix.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0644))
	f := openRWFile(t, path)

	payload := make([]byte, 4096)
	_, err := rand.Read(payload)
	require.NoError(t, err)

	n, err := submitAndWait(t, s, iosched.WriteOp(f, payload, 0))
	require.NoError(t, err)
	require.Equal(t, len(payload), n)

	got := make([]byte, len(payload))
	n, err = submitAndWait(t, s, iosched.ReadOp(f, got, 0))
	require.NoError(t, err)
	require.Equal(t, len(got), n)
	require.Equal(t, payload, got)

	n, err = submitAndWait(t, s, iosched.WriteOp(f, payload, 0).Link(iosched.FdatasyncOp(f)))
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
}

func TestPOSIXSchedulerErrors(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T) error
		want string
	}{
		{
			name: "closed",
			run: func(t *testing.T) error {
				s := iosched.NewPOSIXScheduler()
				require.NoError(t, s.Close())
				path, _ := writeTempFile(t, 4096)
				f := openRWFile(t, path)
				_, err := s.Submit(iosched.ReadOp(f, make([]byte, 4096), 0))
				return err
			},
			want: "scheduler closed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, tc.run(t), tc.want)
		})
	}
}

func TestFallocateOpGrowsFile(t *testing.T) {
	s := iosched.NewPOSIXScheduler()
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	path := filepath.Join(t.TempDir(), "grow.dat")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	const size = int64(1 << 20)
	_, err = submitAndWait(t, s, iosched.FallocateOp(f, size))
	require.NoError(t, err)

	info, err := f.Stat()
	require.NoError(t, err)
	require.GreaterOrEqual(t, info.Size(), size)
}

// Without WithVFiles, virtual operations are unsupported, as on io_uring.
func TestPOSIXRejectsVirtualOperationsWithoutTable(t *testing.T) {
	s := newPOSIX(t)
	path := filepath.Join(t.TempDir(), "unused")
	for _, op := range []iosched.Op{
		iosched.VOpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR, 0o600, 0),
		iosched.VOpenatOp(unix.AT_FDCWD, path, unix.O_RDONLY, 0, iosched.AllocateVFD),
		iosched.VReadOp(0, make([]byte, 1), 0),
		iosched.VWriteOp(0, []byte("x"), 0),
		iosched.VCloseOp(0),
		iosched.OpenatOp(unix.AT_FDCWD, path, unix.O_CREAT|unix.O_RDWR, 0o600).Link(iosched.VCloseOp(0)),
	} {
		_, err := s.Submit(op)
		require.ErrorIs(t, err, syscall.EOPNOTSUPP)
	}
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "rejected chains must not execute")
}

func TestPOSIXVirtualFiles(t *testing.T) {
	s := iosched.NewPOSIXScheduler(iosched.WithVFiles(2))
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	dir := t.TempDir()
	path := filepath.Join(dir, "v")
	open := func(path string, flags int) iosched.Op {
		return iosched.VOpenatOp(unix.AT_FDCWD, path, flags, 0o600, iosched.AllocateVFD)
	}

	// Open, write, close in one chain; Wait reports the allocated slot.
	ticket, err := s.Submit(open(path, unix.O_CREAT|unix.O_RDWR).Link(
		iosched.VWriteOp(iosched.AllocateVFD, []byte("hello"), 0),
		iosched.VCloseOp(iosched.AllocateVFD)))
	require.NoError(t, err)
	vfd, err := ticket.Wait()
	require.NoError(t, err)
	require.Equal(t, 0, vfd)
	require.Equal(t, 5, ticket.AllResults()[1].N)

	// The close freed slot 0; a held open keeps it occupied across submissions.
	held, err := submitAndWait(t, s, open(path, unix.O_RDONLY))
	require.NoError(t, err)
	require.Equal(t, 0, held)
	buf := make([]byte, 5)
	n, err := submitAndWait(t, s, iosched.VReadOp(uint32(held), buf, 0))
	require.NoError(t, err)
	require.Equal(t, "hello", string(buf[:n]))

	second, err := submitAndWait(t, s, open(path, unix.O_RDONLY))
	require.NoError(t, err)
	require.Equal(t, 1, second)

	// Exhaustion fails the open with ENFILE; Link cancels, HardLink reports EBADF.
	ticket, err = s.Submit(open(path, unix.O_RDONLY).Link(iosched.VReadOp(iosched.AllocateVFD, buf, 0)))
	require.NoError(t, err)
	_, err = ticket.Wait()
	require.ErrorIs(t, err, syscall.ENFILE)
	require.ErrorIs(t, ticket.AllResults()[1].Err, syscall.ECANCELED)
	ticket, err = s.Submit(open(path, unix.O_RDONLY).HardLink(iosched.VCloseOp(iosched.AllocateVFD)))
	require.NoError(t, err)
	_, err = ticket.Wait()
	require.ErrorIs(t, err, syscall.ENFILE)
	require.ErrorIs(t, ticket.AllResults()[1].Err, syscall.EBADF)

	// A failed open releases its reservation after the chain.
	_, err = submitAndWait(t, s, iosched.VCloseOp(uint32(second)))
	require.NoError(t, err)
	ticket, err = s.Submit(open(filepath.Join(dir, "missing"), unix.O_RDONLY).HardLink(
		iosched.VCloseOp(iosched.AllocateVFD)))
	require.NoError(t, err)
	_, err = ticket.Wait()
	require.ErrorIs(t, err, syscall.ENOENT)
	require.ErrorIs(t, ticket.AllResults()[1].Err, syscall.EBADF)
	reused, err := submitAndWait(t, s, open(path, unix.O_RDONLY))
	require.NoError(t, err)
	require.Equal(t, second, reused)
}
