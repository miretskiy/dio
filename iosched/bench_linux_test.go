//go:build linux

package iosched_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/iosched"
)

const benchStorageRoot = "/instance_storage"

func benchFile(b *testing.B, size int64) *os.File {
	b.Helper()
	if _, err := os.Stat(benchStorageRoot); os.IsNotExist(err) {
		b.Skipf("%s not found", benchStorageRoot)
	}
	dir, err := os.MkdirTemp(benchStorageRoot, "dio-bench-")
	if err != nil {
		b.Fatalf("mkdir temp under %s: %v", benchStorageRoot, err)
	}
	path := filepath.Join(dir, "data.bin")
	f, err := os.Create(path)
	if err != nil {
		b.Fatalf("create %s: %v", path, err)
	}
	if err := f.Truncate(size); err != nil {
		b.Fatalf("truncate %s: %v", path, err)
	}
	buf := align.AllocAligned(1 << 20)
	defer align.FreeAligned(buf)
	for i := range buf {
		buf[i] = byte(i)
	}
	for off := int64(0); off < size; {
		n, werr := f.WriteAt(buf, off)
		if werr != nil {
			b.Fatalf("write at %d: %v", off, werr)
		}
		off += int64(n)
	}
	if err := f.Sync(); err != nil {
		b.Fatalf("sync: %v", err)
	}
	b.Cleanup(func() {
		f.Close()
		os.RemoveAll(dir)
	})
	return f
}

func reportSchedStats(b *testing.B, sched *iosched.URingScheduler) {
	st := sched.Stats()
	if st.Syscalls == 0 {
		return
	}
	b.ReportMetric(float64(st.OpsPlaced)/float64(st.Syscalls), "ops/syscall")
}

func BenchmarkSubmission_ReadAt_Serial(b *testing.B) {
	if !iosched.IOUringAvailable {
		b.Skip("io_uring not available")
	}
	const fileSize = 64 << 20
	const blockSize = 4096

	f := benchFile(b, fileSize)
	sched, err := iosched.NewURingScheduler(iosched.URingConfig{RingDepth: 256})
	if err != nil {
		b.Fatal(err)
	}
	defer sched.Close()

	buf := make([]byte, blockSize)
	b.SetBytes(blockSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		offset := int64((i * blockSize) % (fileSize - blockSize))
		ticket, err := sched.Submit(iosched.ReadOp(f, buf, offset))
		if err != nil {
			b.Fatalf("submit: %v", err)
		}
		ticket.Wait()
		if res := ticket.Op.Result; res.Err != nil {
			ticket.Release()
			b.Fatalf("read: %v", res.Err)
		}
		ticket.Release()
	}
	b.StopTimer()
	reportSchedStats(b, sched)
}

func BenchmarkSubmission_ReadAt_Parallel(b *testing.B) {
	if !iosched.IOUringAvailable {
		b.Skip("io_uring not available")
	}
	const fileSize = 256 << 20
	const blockSize = 4096

	f := benchFile(b, fileSize)
	sched, err := iosched.NewURingScheduler(iosched.URingConfig{RingDepth: 512})
	if err != nil {
		b.Fatal(err)
	}
	defer sched.Close()

	b.SetBytes(blockSize)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, blockSize)
		var i int64
		for pb.Next() {
			offset := (i * blockSize) % (fileSize - blockSize)
			i++
			ticket, err := sched.Submit(iosched.ReadOp(f, buf, offset))
			if err != nil {
				b.Fatalf("submit: %v", err)
			}
			ticket.Wait()
			if res := ticket.Op.Result; res.Err != nil {
				ticket.Release()
				b.Fatalf("read: %v", res.Err)
			}
			ticket.Release()
		}
	})
	b.StopTimer()
	reportSchedStats(b, sched)
}

func BenchmarkSubmission_Concurrency(b *testing.B) {
	if !iosched.IOUringAvailable {
		b.Skip("io_uring not available")
	}
	const fileSize = 256 << 20
	const blockSize = 4096

	for _, n := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
		b.Run(fmt.Sprintf("goroutines=%d", n), func(b *testing.B) {
			f := benchFile(b, fileSize)
			sched, err := iosched.NewURingScheduler(iosched.URingConfig{RingDepth: 512})
			if err != nil {
				b.Fatal(err)
			}
			defer sched.Close()

			b.SetBytes(blockSize)
			b.ReportAllocs()
			b.ResetTimer()

			perG := b.N / n
			extra := b.N % n
			var wg sync.WaitGroup
			for g := range n {
				count := perG
				if g < extra {
					count++
				}
				wg.Go(func() {
					buf := make([]byte, blockSize)
					for i := range count {
						offset := int64((i * blockSize) % (fileSize - blockSize))
						ticket, err := sched.Submit(iosched.ReadOp(f, buf, offset))
						if err != nil {
							b.Errorf("submit: %v", err)
							return
						}
						ticket.Wait()
						if res := ticket.Op.Result; res.Err != nil {
							ticket.Release()
							b.Errorf("read: %v", res.Err)
							return
						}
						ticket.Release()
					}
				})
			}
			wg.Wait()
			b.StopTimer()
			reportSchedStats(b, sched)
		})
	}
}

func BenchmarkSubmission_BatchedReads(b *testing.B) {
	if !iosched.IOUringAvailable {
		b.Skip("io_uring not available")
	}
	const fileSize = 64 << 20
	const blockSize = 4096

	for _, batch := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			f := benchFile(b, fileSize)
			sched, err := iosched.NewURingScheduler(iosched.URingConfig{RingDepth: 512})
			if err != nil {
				b.Fatal(err)
			}
			defer sched.Close()

			bufs := make([][]byte, batch)
			tickets := make([]*iosched.Ticket, batch)
			for i := range bufs {
				bufs[i] = make([]byte, blockSize)
			}

			b.SetBytes(int64(batch) * blockSize)
			b.ReportAllocs()
			b.ResetTimer()

			for i := range b.N {
				for j := range tickets {
					offset := int64(((i*batch + j) * blockSize) % (fileSize - blockSize))
					ticket, err := sched.Submit(iosched.ReadOp(f, bufs[j], offset))
					if err != nil {
						b.Fatalf("submit %d: %v", j, err)
					}
					tickets[j] = ticket
				}
				for j, ticket := range tickets {
					ticket.Wait()
					if res := ticket.Op.Result; res.Err != nil {
						ticket.Release()
						b.Fatalf("read %d: %v", j, res.Err)
					}
					ticket.Release()
				}
			}
			b.StopTimer()
			reportSchedStats(b, sched)
		})
	}
}
