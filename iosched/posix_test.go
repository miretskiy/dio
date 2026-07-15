package iosched_test

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/iosched"
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

func submitAndWait(t *testing.T, s iosched.Scheduler, op iosched.Op) iosched.Ticket {
	t.Helper()
	ticket, err := s.Submit(op)
	require.NoError(t, err)
	ticket.Wait()
	return ticket
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

	ticket := submitAndWait(t, s, iosched.WriteOp(f, payload, 0))
	require.NoError(t, ticket.Error())
	require.Equal(t, len(payload), ticket.N())

	got := make([]byte, len(payload))
	ticket = submitAndWait(t, s, iosched.ReadOp(f, got, 0))
	require.NoError(t, ticket.Error())
	require.Equal(t, len(got), ticket.N())
	require.Equal(t, payload, got)

	ticket = submitAndWait(t, s, iosched.WriteOp(f, payload, 0).Link(iosched.FdatasyncOp(f)))
	require.NoError(t, ticket.Error())
	require.Equal(t, len(payload), ticket.N())
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
		{
			name: "nil file",
			run: func(t *testing.T) error {
				s := iosched.NewPOSIXScheduler()
				t.Cleanup(func() { require.NoError(t, s.Close()) })
				_, err := s.Submit(iosched.ReadOp(nil, make([]byte, 4096), 0))
				return err
			},
			want: "nil file",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, tc.run(t), tc.want)
		})
	}
}
