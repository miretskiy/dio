//go:build linux

package iosched_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/iosched"
)

// BenchmarkWriteCoalescing measures coalescing under parallelism. Each goroutine
// repeatedly submits a batch of contiguous writes to its own file and waits for
// them and reports throughput through the testing package's MB/s metric.
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
		tickets := make([]iosched.Ticket, batch)
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
			}
		}
	})

}
