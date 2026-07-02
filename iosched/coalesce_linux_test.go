//go:build linux

package iosched_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/iosched"
)

// TestURing_WriteCoalescing fires a burst of contiguous writes and checks both
// that the bytes land correctly and that the coordinator merged them into fewer
// SQEs than writes (Stats.OpsPlaced < n). The burst is submitted without waiting
// between ops so it piles into the staging stack and batches — with 128 rapid
// submits behind a SubmitAndWait syscall, at least some coalescing is a
// near-certainty.
func TestURing_WriteCoalescing(t *testing.T) {
	s := newURingSched(t, iosched.URingConfig{RingDepth: 256})
	f := newEmptyFile(t)

	const n = 128
	const chunk = 64
	want := make([]byte, n*chunk)
	tickets := make([]*iosched.Ticket, n)
	for i := range n {
		buf := bytes.Repeat([]byte{byte(i)}, chunk)
		copy(want[i*chunk:], buf)
		tk, err := s.Submit(iosched.WriteOp(f, buf, int64(i*chunk)))
		require.NoError(t, err)
		tickets[i] = tk
	}
	for _, tk := range tickets {
		tk.Wait()
		require.NoError(t, tk.Error())
		tk.Release()
	}

	placed := int(s.Stats().OpsPlaced) // writes only; the verify read runs after
	require.Less(t, placed, n, "expected write coalescing to place fewer SQEs than writes")

	got := make([]byte, len(want))
	require.Equal(t, len(want), runOp(t, s, iosched.ReadOp(f, got, 0)).N)
	require.Equal(t, want, got)
}

// BenchmarkWriteCoalescing measures coalescing under parallelism. Each goroutine
// repeatedly submits a batch of contiguous writes to its own file and waits for
// them. It reports writes/sqe and writes/syscall so the reduction is visible
// alongside throughput (MB/s from SetBytes).
func BenchmarkWriteCoalescing(b *testing.B) {
	for _, tc := range []struct {
		name    string
		disable bool
	}{
		{"coalesced", false},
		{"uncoalesced", true},
	} {
		b.Run(tc.name, func(b *testing.B) { benchWriteCoalescing(b, tc.disable) })
	}
}

func benchWriteCoalescing(b *testing.B, disable bool) {
	if !iosched.IOUringAvailable {
		b.Skip("io_uring not available")
	}
	s, err := iosched.NewURingScheduler(iosched.URingConfig{RingDepth: 4096, DisableCoalescing: disable})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, s.Close()) })

	dir := b.TempDir()
	const batch = 16
	const chunk = 4096
	b.SetBytes(int64(batch * chunk))
	var gid atomic.Int64

	b.RunParallel(func(pb *testing.PB) {
		f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("g%d.dat", gid.Add(1))), os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			b.Error(err)
			return
		}
		defer f.Close()
		buf := make([]byte, chunk)
		tickets := make([]*iosched.Ticket, batch)
		for pb.Next() {
			for i := range batch {
				tk, err := s.Submit(iosched.WriteOp(f, buf, int64(i*chunk)))
				if err != nil {
					b.Error(err)
					return
				}
				tickets[i] = tk
			}
			for _, tk := range tickets {
				tk.Wait()
				if err := tk.Error(); err != nil {
					b.Error(err)
				}
				tk.Release()
			}
		}
	})

	b.StopTimer()
	st := s.Stats()
	writes := float64(b.N) * batch
	b.ReportMetric(writes/float64(st.OpsPlaced), "writes/sqe")
	b.ReportMetric(writes/float64(st.Syscalls), "writes/syscall")
}
