//go:build linux

package iosched_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

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

// sealedMemfd creates an anonymous in-memory file of exactly size bytes and
// seals it against growth, so a write past the end short-writes (returns the
// bytes that fit) instead of extending the file.
func sealedMemfd(t *testing.T, size int64) *os.File {
	t.Helper()
	fd, err := unix.MemfdCreate("dio-short", unix.MFD_ALLOW_SEALING)
	require.NoError(t, err)
	require.NoError(t, unix.Fallocate(fd, 0, 0, size))
	_, err = unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, unix.F_SEAL_GROW)
	require.NoError(t, err)
	f := os.NewFile(uintptr(fd), "memfd")
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// TestURing_CoalescedShortWrite drives a real coalesced writev into a
// size-sealed memfd so it short-writes, and checks the partial count is
// distributed per ticket: the first (fully covered) write reports its full
// length with no error; the second reports only the bytes that fit, with
// io.ErrShortWrite. Coalescing is timing-dependent, so it retries with a fresh
// file until the two writes actually merge (OpsPlaced advances by one writev,
// not two writes) — no mocks; a genuine short writev through the kernel.
func TestURing_CoalescedShortWrite(t *testing.T) {
	s := newURingSched(t, iosched.URingConfig{RingDepth: 16})
	// Seal the file at one page. A sub-page write crossing the seal is refused
	// whole (EPERM, zero bytes), but a write that fills complete pages and then
	// crosses the seal short-writes at the boundary. So the first buffer fills
	// most of the page and the second straddles the page boundary: the coalesced
	// writev fills the page and is refused past it — a real byte-granular short
	// write.
	page := os.Getpagesize()
	firstLen := page - 96
	first := bytes.Repeat([]byte{0xAA}, firstLen)
	second := bytes.Repeat([]byte{0xBB}, 200) // 96 bytes fit before the seal, 104 do not

	for attempt := 0; ; attempt++ {
		require.Less(t, attempt, 200, "two contiguous writes never coalesced")
		f := sealedMemfd(t, int64(page))

		before := s.Stats().OpsPlaced
		ta, err := s.Submit(iosched.WriteOp(f, first, 0))
		require.NoError(t, err)
		tb, err := s.Submit(iosched.WriteOp(f, second, int64(firstLen)))
		require.NoError(t, err)
		ta.Wait()
		tb.Wait()

		if s.Stats().OpsPlaced-before != 1 { // not one writev; didn't coalesce, retry
			ta.Release()
			tb.Release()
			continue
		}

		// The coalesced writev filled exactly one page: the first buffer fully,
		// the second only up to the seal (96 of 200 bytes).
		require.NoError(t, ta.Error())
		require.Equal(t, firstLen, ta.Result().N)
		require.ErrorIs(t, tb.Error(), io.ErrShortWrite)
		require.Equal(t, 96, tb.Result().N)
		ta.Release()
		tb.Release()
		return
	}
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
