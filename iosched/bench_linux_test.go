//go:build linux

// Micro-benchmarks for the io_uring Submission API. These exercise the
// hot path of Submit + sub.Done() against /instance_storage (NVMe) so
// throughput, allocation behavior, and CPU utilization can be measured
// against the real storage device rather than tmpfs.
//
// Run:
//
//	go test ./iosched/... -bench=BenchmarkSubmission -benchmem -benchtime=2s
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

// benchFile creates a pre-allocated O_DIRECT file under /instance_storage of
// the given size, filled with deterministic bytes. b.Cleanup removes it.
func benchFile(b *testing.B, size int64) *os.File {
	b.Helper()
	if _, err := os.Stat(benchStorageRoot); os.IsNotExist(err) {
		b.Skipf("%s not found", benchStorageRoot)
	}
	dir := filepath.Join(benchStorageRoot, fmt.Sprintf("dio-bench-%d", os.Getpid()))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatalf("mkdir %s: %v", dir, err)
	}
	path := filepath.Join(dir, "data.bin")
	f, err := os.Create(path)
	if err != nil {
		b.Fatalf("create %s: %v", path, err)
	}
	if err := f.Truncate(size); err != nil {
		b.Fatalf("truncate %s: %v", path, err)
	}
	// Fill the file so reads hit storage.
	buf := align.AllocAligned(1 << 20)
	defer align.FreeAligned(buf)
	for i := range buf {
		buf[i] = byte(i)
	}
	off := int64(0)
	for off < size {
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

// BenchmarkSubmission_ReadAt_Serial measures one Submit + sub.Done() round
// trip on a single goroutine — the simplest path through the scheduler.
// Shows base latency + per-op allocation cost.
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

	// Use a single Ticket reused across iterations. The Ticket itself is
	// pool-managed, so even without explicit reuse the cost is amortized;
	// reusing locally exercises the steady-state allocation profile.
	ticket := iosched.NewTicket()
	defer ticket.Release()

	for i := range b.N {
		offset := int64((i * blockSize) % (fileSize - blockSize))
		ticket.Ops = []iosched.Op{iosched.ReadOp(f, buf, offset)}
		if err := sched.Submit(ticket); err != nil {
			b.Fatalf("submit: %v", err)
		}
		ticket.Wait()
		if res := ticket.Ops[0].Result(); res.Err != nil {
			b.Fatalf("read: %v", res.Err)
		}
	}
	b.StopTimer()
	reportSchedStats(b, sched)
}

// reportSchedStats emits ops/syscall as a custom benchmark metric so
// benchstat can summarize the average batch size achieved by the coordinator.
func reportSchedStats(b *testing.B, sched *iosched.URingScheduler) {
	st := sched.Stats()
	if st.Syscalls == 0 {
		return
	}
	b.ReportMetric(float64(st.OpsPlaced)/float64(st.Syscalls), "ops/syscall")
}

// BenchmarkSubmission_ReadAt_Parallel measures throughput under N concurrent
// readers. Exercises the signal-channel fast path + overflow slow path under
// contention.
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
		ticket := iosched.NewTicket()
		defer ticket.Release()
		var i int64
		for pb.Next() {
			offset := (i * blockSize) % (fileSize - blockSize)
			i++
			ticket.Ops = []iosched.Op{iosched.ReadOp(f, buf, offset)}
			if err := sched.Submit(ticket); err != nil {
				b.Fatalf("submit: %v", err)
			}
			ticket.Wait()
			if res := ticket.Ops[0].Result(); res.Err != nil {
				b.Fatalf("read: %v", res.Err)
			}
		}
	})
	b.StopTimer()
	reportSchedStats(b, sched)
}

// BenchmarkSubmission_Concurrency sweeps the number of submitter goroutines
// to see how the coordinator's opportunistic batching scales with arrival
// concurrency. Each goroutine submits 4 KiB reads in a tight loop. The
// reported ops/syscall metric is the average number of SQEs placed per
// io_uring_enter call — the directly observable "did batching happen?"
// answer.
func BenchmarkSubmission_Concurrency(b *testing.B) {
	if !iosched.IOUringAvailable {
		b.Skip("io_uring not available")
	}
	const fileSize = 256 << 20
	const blockSize = 4096

	for _, n := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
		b.Run(fmt.Sprintf("goroutines=%d", n), func(b *testing.B) {
			f := benchFile(b, fileSize)
			sched, err := iosched.NewURingScheduler(iosched.URingConfig{
				RingDepth: 512,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer sched.Close()

			b.SetBytes(blockSize)
			b.ReportAllocs()
			b.ResetTimer()

			// Split b.N evenly across n goroutines.
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
					ticket := iosched.NewTicket()
					defer ticket.Release()
					for i := range count {
						offset := int64((i * blockSize) % (fileSize - blockSize))
						ticket.Ops = []iosched.Op{iosched.ReadOp(f, buf, offset)}
						if err := sched.Submit(ticket); err != nil {
							b.Errorf("submit: %v", err)
							return
						}
						ticket.Wait()
						if res := ticket.Ops[0].Result(); res.Err != nil {
							b.Errorf("read: %v", res.Err)
							return
						}
					}
				})
			}
			wg.Wait()
			b.StopTimer()
			reportSchedStats(b, sched)
		})
	}
}

// BenchmarkSubmission_BatchedReads measures the cost of a single Submission
// that carries many ops — exercises the per-Submission overhead amortized
// across N ops. Compared against _Serial this isolates the fixed cost of
// Submit + Done vs the variable cost of placing/reaping ops.
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
			for i := range bufs {
				bufs[i] = make([]byte, blockSize)
			}

			b.SetBytes(int64(batch) * blockSize)
			b.ReportAllocs()
			b.ResetTimer()

			ticket := iosched.NewTicket()
			defer ticket.Release()
			ops := make([]iosched.Op, batch)
			for i := range b.N {
				for j := range ops {
					offset := int64(((i*batch + j) * blockSize) % (fileSize - blockSize))
					ops[j] = iosched.ReadOp(f, bufs[j], offset)
				}
				ticket.Ops = ops
				if err := sched.Submit(ticket); err != nil {
					b.Fatalf("submit: %v", err)
				}
				ticket.Wait()
				for j := range ticket.Ops {
					if res := ticket.Ops[j].Result(); res.Err != nil {
						b.Fatalf("read %d: %v", j, res.Err)
					}
				}
			}
			b.StopTimer()
			reportSchedStats(b, sched)
		})
	}
}
