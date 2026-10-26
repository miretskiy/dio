package iosched_test

import (
	"os"
	"syscall"
	"testing"

	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
)

func TestTicketResults(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s iosched.Scheduler) {
		f := newEmptyFile(t)
		got := make([]byte, 3)
		ticket, err := s.Submit(iosched.WriteOp(f, []byte("value"), 0).Link(
			iosched.ReadOp(f, got, 1), iosched.FdatasyncOp(f)))
		require.NoError(t, err)
		n, err := ticket.Wait()
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, "alu", string(got))
		require.Equal(t, []iosched.Result{{N: 5}, {N: 3}, {}}, ticket.AllResults())

		readonly, err := os.Open(f.Name())
		require.NoError(t, err)
		defer readonly.Close()
		for _, hard := range []bool{false, true} {
			root := iosched.WriteOp(readonly, []byte("fails"), 0)
			tail := iosched.WriteOp(f, []byte("ok"), 0).Durable()
			if hard {
				root = root.HardLink(tail)
			} else {
				root = root.Link(tail)
			}
			ticket, err := s.Submit(root)
			require.NoError(t, err)
			_, err = ticket.Wait()
			require.ErrorIs(t, err, syscall.EBADF)
			results := ticket.AllResults()
			require.Len(t, results, 2)
			require.ErrorIs(t, results[0].Err, syscall.EBADF)
			if hard {
				require.Equal(t, iosched.Result{N: 2}, results[1])
			} else {
				require.ErrorIs(t, results[1].Err, syscall.ECANCELED)
			}
		}
	})
}

func TestTicketResultsRemainIndependent(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s iosched.Scheduler) {
		f := newEmptyFile(t)
		op := iosched.WriteOp(f, []byte("same"), 0).Link(iosched.FsyncOp(f))
		tickets := make([]iosched.Ticket, 16)
		for i := range tickets {
			var err error
			tickets[i], err = s.Submit(op)
			require.NoError(t, err)
		}
		for _, ticket := range tickets {
			_, err := ticket.Wait()
			require.NoError(t, err)
			require.Equal(t, []iosched.Result{{N: 4}, {}}, ticket.AllResults())
		}
	})
}

func TestTicketAllResults(t *testing.T) {
	forEachScheduler(t, func(t *testing.T, s iosched.Scheduler) {
		f := newEmptyFile(t)
		for _, linked := range []bool{false, true} {
			op := iosched.WriteOp(f, []byte("value"), 0)
			want := []iosched.Result{{N: 5}}
			if linked {
				op = op.Link(iosched.ReadOp(f, make([]byte, 3), 1))
				want = append(want, iosched.Result{N: 3})
			}
			ticket, err := s.Submit(op)
			require.NoError(t, err)
			n, err := ticket.Wait()
			require.NoError(t, err)
			require.Equal(t, 5, n)
			require.Equal(t, want, ticket.AllResults())
		}
	})
}
