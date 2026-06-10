//go:build linux

// Benchmarks for scheduler throughput and IOPS.
//
// By default they run against a file in the test temp dir, which usually sits
// on the root filesystem. To measure a specific device (e.g. a raw NVMe
// namespace or a file on it), point DIO_BENCH_FILE at a path on that device:
//
//	DIO_BENCH_FILE=/mnt/nvme/bench go test -bench . -benchtime 5s ./iosched
//
// Files are opened with O_DIRECT so results measure the device, not the page
// cache; benchmarks that cannot open O_DIRECT (e.g. tmpfs) fall back to
// buffered I/O and say so. The 4k random-read benchmarks are the IOPS
// numbers; the 128k sequential reads are the bandwidth numbers. Saturating a
// modern NVMe device (~1M IOPS) requires queue depth: use the batch≥32
// variants and -cpu to scale submitter goroutines.
package iosched

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/mempool"
	"github.com/miretskiy/dio/sys"
)

const benchFileSize = 1 << 30 // 1 GiB

// benchFile opens (creating and pre-sizing if needed) the benchmark file,
// preferring O_DIRECT.
func benchFile(b *testing.B) *os.File {
	b.Helper()
	path := os.Getenv("DIO_BENCH_FILE")
	if path == "" {
		path = filepath.Join(b.TempDir(), "bench")
	}
	// Read-write with O_DIRECT (sys.CreateDirect is write-only).
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|sys.FlDirectIO.OpenFlags(), 0o644)
	if err != nil {
		b.Logf("O_DIRECT unavailable (%v); falling back to buffered I/O", err)
		f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.Cleanup(func() { _ = f.Close() })

	st, err := f.Stat()
	if err != nil {
		b.Fatal(err)
	}
	if st.Size() < benchFileSize {
		if err := sys.Fallocate(f, benchFileSize); err != nil {
			b.Fatalf("fallocate: %v", err)
		}
		// Materialize extents so reads hit real blocks, 1 MiB at a time.
		buf := align.AllocAligned(1 << 20)
		defer align.FreeAligned(buf)
		_, _ = rand.NewChaCha8([32]byte{42}).Read(buf)
		for off := int64(0); off < benchFileSize; off += int64(len(buf)) {
			if _, err := pwriteFull(int(f.Fd()), buf, off); err != nil {
				b.Fatalf("prefill: %v", err)
			}
		}
		if err := sys.Fdatasync(f); err != nil {
			b.Fatal(err)
		}
	}
	return f
}

// benchRandRead measures random positioned reads of size bs, `batch` ops per
// Submit, across parallel goroutines (scale with -cpu).
func benchRandRead(b *testing.B, s Scheduler, f *os.File, bs, batch int, fixed bool, pool *mempool.SlabPool) {
	fd := int(f.Fd())
	blocks := int64(benchFileSize / bs)

	b.SetBytes(int64(bs * batch))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var bufs [][]byte
		if fixed {
			for i := 0; i < batch; i++ {
				slot, err := pool.Acquire()
				if err != nil {
					b.Error(err)
					return
				}
				defer slot.Release()
				bufs = append(bufs, slot.Data[:bs])
			}
		} else {
			raw := align.AllocAligned(bs * batch)
			defer align.FreeAligned(raw)
			for i := 0; i < batch; i++ {
				bufs = append(bufs, raw[i*bs:(i+1)*bs])
			}
		}
		rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
		ops := make([]Op, batch)
		for pb.Next() {
			for i := 0; i < batch; i++ {
				off := rng.Int64N(blocks) * int64(bs)
				if fixed {
					ops[i] = ReadFixedOp(fd, bufs[i], off)
				} else {
					ops[i] = ReadOp(fd, bufs[i], off)
				}
			}
			if err := s.Submit(ops); err != nil {
				b.Error(err)
				return
			}
			for i := range ops {
				if r := ops[i].Result(); r.Err != nil || r.N != bs {
					b.Errorf("op %d: n=%d err=%v", i, r.N, r.Err)
					return
				}
			}
		}
	})
	b.StopTimer()
	reportIOPS(b, batch)
}

func reportIOPS(b *testing.B, batch int) {
	if sec := b.Elapsed().Seconds(); sec > 0 {
		b.ReportMetric(float64(b.N*batch)/sec, "iops")
	}
}

func newBenchURing(b *testing.B, cfg URingConfig) *URingScheduler {
	s, err := NewURingScheduler(cfg)
	if err != nil {
		b.Skipf("io_uring unavailable: %v", err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s
}

func BenchmarkURingRead4K(b *testing.B) {
	f := benchFile(b)
	for _, batch := range []int{1, 8, 32, 64} {
		b.Run(fmt.Sprintf("batch%d", batch), func(b *testing.B) {
			s := newBenchURing(b, URingConfig{RingDepth: 512})
			benchRandRead(b, s, f, 4096, batch, false, nil)
		})
	}
}

func BenchmarkURingRead4KFixed(b *testing.B) {
	f := benchFile(b)
	for _, batch := range []int{1, 8, 32, 64} {
		b.Run(fmt.Sprintf("batch%d", batch), func(b *testing.B) {
			s := newBenchURing(b, URingConfig{RingDepth: 512})
			// One slab large enough for every parallel goroutine's batch.
			// Registration pins the slab, so it counts against
			// RLIMIT_MEMLOCK; skip when the limit is too low.
			pool, err := mempool.NewSlabPool(runtime.GOMAXPROCS(0)*batch*4096, 4096)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(pool.Close)
			if err := RegisterDMASlab(s, pool); err != nil {
				b.Skipf("cannot register DMA slab (raise ulimit -l): %v", err)
			}
			benchRandRead(b, s, f, 4096, batch, true, pool)
		})
	}
}

func BenchmarkURingRead4KSQPoll(b *testing.B) {
	f := benchFile(b)
	for _, batch := range []int{1, 32} {
		b.Run(fmt.Sprintf("batch%d", batch), func(b *testing.B) {
			s := newBenchURing(b, URingConfig{RingDepth: 512, SQPOLL: true})
			benchRandRead(b, s, f, 4096, batch, false, nil)
		})
	}
}

func BenchmarkPosixRead4K(b *testing.B) {
	f := benchFile(b)
	for _, batch := range []int{1, 32} {
		b.Run(fmt.Sprintf("batch%d", batch), func(b *testing.B) {
			s := NewPosixScheduler()
			b.Cleanup(func() { _ = s.Close() })
			benchRandRead(b, s, f, 4096, batch, false, nil)
		})
	}
}

// Sequential large reads: device bandwidth rather than IOPS.
func BenchmarkURingRead128KSeq(b *testing.B) {
	f := benchFile(b)
	const bs = 128 << 10
	const batch = 8
	s := newBenchURing(b, URingConfig{RingDepth: 512})
	fd := int(f.Fd())

	b.SetBytes(bs * batch)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		raw := align.AllocAligned(bs * batch)
		defer align.FreeAligned(raw)
		rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
		ops := make([]Op, batch)
		// Each iteration reads a contiguous batch*bs run at a random
		// batch-aligned position.
		runs := int64(benchFileSize / (bs * batch))
		for pb.Next() {
			base := rng.Int64N(runs) * bs * batch
			for i := 0; i < batch; i++ {
				ops[i] = ReadOp(fd, raw[i*bs:(i+1)*bs], base+int64(i*bs))
			}
			if err := s.Submit(ops); err != nil {
				b.Error(err)
				return
			}
			for i := range ops {
				if r := ops[i].Result(); r.Err != nil || r.N != bs {
					b.Errorf("op %d: n=%d err=%v", i, r.N, r.Err)
					return
				}
			}
		}
	})
	b.StopTimer()
	reportIOPS(b, batch)
}

// Random 4k writes through the scheduler (no fsync): measures write IOPS.
func BenchmarkURingWrite4K(b *testing.B) {
	f := benchFile(b)
	const bs = 4096
	for _, batch := range []int{1, 32} {
		b.Run(fmt.Sprintf("batch%d", batch), func(b *testing.B) {
			s := newBenchURing(b, URingConfig{RingDepth: 512})
			fd := int(f.Fd())
			blocks := int64(benchFileSize / bs)

			b.SetBytes(int64(bs * batch))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				raw := align.AllocAligned(bs * batch)
				defer align.FreeAligned(raw)
				_, _ = rand.NewChaCha8([32]byte{1}).Read(raw)
				rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
				ops := make([]Op, batch)
				for pb.Next() {
					for i := 0; i < batch; i++ {
						off := rng.Int64N(blocks) * bs
						ops[i] = WriteOp(fd, raw[i*bs:(i+1)*bs], off)
					}
					if err := s.Submit(ops); err != nil {
						b.Error(err)
						return
					}
					for i := range ops {
						if r := ops[i].Result(); r.Err != nil || r.N != bs {
							b.Errorf("op %d: n=%d err=%v", i, r.N, r.Err)
							return
						}
					}
				}
			})
			b.StopTimer()
			reportIOPS(b, batch)
		})
	}
}

// Group commit: linked write+fdatasync chains, the WAL pattern.
func BenchmarkURingWriteFdatasync(b *testing.B) {
	f := benchFile(b)
	const bs = 4096
	s := newBenchURing(b, URingConfig{RingDepth: 512})
	fd := int(f.Fd())
	blocks := int64(benchFileSize / bs)

	b.SetBytes(bs)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		raw := align.AllocAligned(bs)
		defer align.FreeAligned(raw)
		rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
		for pb.Next() {
			off := rng.Int64N(blocks) * bs
			ops := []Op{
				WriteOp(fd, raw, off).Linked(),
				FdatasyncOp(fd),
			}
			if err := s.Submit(ops); err != nil {
				b.Error(err)
				return
			}
			for i := range ops {
				if err := ops[i].Result().Err; err != nil {
					b.Errorf("op %d: %v", i, err)
					return
				}
			}
		}
	})
	b.StopTimer()
	reportIOPS(b, 1)
}
