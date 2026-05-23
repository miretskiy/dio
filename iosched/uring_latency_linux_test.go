//go:build linux

package iosched

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/sys"
)

const latencyBenchRoot = "/instance_storage"

type uringLatencyRecorder struct {
	mu              sync.Mutex
	leader          uringLatencyBucket
	follower        uringLatencyBucket
	queuedBehind    uringLatencyBucket
	total           int64
	queuedBehindOps int64
	batchSizeSum    int64
	batchCyclesSum  int64
}

type uringLatencyBucket struct {
	count            int64
	submitLatency    time.Duration
	queueLatency     time.Duration
	completeToReturn time.Duration
}

func (r *uringLatencyRecorder) recordURingTrace(e uringTraceEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.total++
	r.batchSizeSum += int64(e.batchSize)
	r.batchCyclesSum += int64(e.batchCycles)

	var bucket *uringLatencyBucket
	switch e.role {
	case uringTraceLeader:
		bucket = &r.leader
	case uringTraceFollower:
		bucket = &r.follower
	default:
		return
	}
	recordLatency(bucket, e)

	if e.queuedBehindLeader {
		r.queuedBehindOps++
		recordLatency(&r.queuedBehind, e)
	}
}

func recordLatency(bucket *uringLatencyBucket, e uringTraceEvent) {
	bucket.count++
	bucket.submitLatency += e.submitLatency
	bucket.queueLatency += e.queueLatency
	bucket.completeToReturn += e.completeToReturn
}

func (r *uringLatencyRecorder) report(b *testing.B) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.total == 0 {
		return
	}
	reportBucket := func(name string, bucket uringLatencyBucket) {
		if bucket.count == 0 {
			return
		}
		n := float64(bucket.count)
		b.ReportMetric(float64(bucket.submitLatency.Nanoseconds())/n, name+"-submit-ns")
		b.ReportMetric(float64(bucket.queueLatency.Nanoseconds())/n, name+"-queue-ns")
		b.ReportMetric(float64(bucket.completeToReturn.Nanoseconds())/n, name+"-lag-ns")
	}

	reportBucket("leader", r.leader)
	reportBucket("follower", r.follower)
	reportBucket("behind", r.queuedBehind)
	b.ReportMetric(100*float64(r.leader.count)/float64(r.total), "leader-pct")
	b.ReportMetric(100*float64(r.queuedBehindOps)/float64(r.total), "behind-pct")
	b.ReportMetric(float64(r.batchSizeSum)/float64(r.total), "batch-size")
	b.ReportMetric(float64(r.batchCyclesSum)/float64(r.total), "batch-cycles")
}

func latencyBenchFile(b *testing.B, size int64) *os.File {
	b.Helper()
	if _, err := os.Stat(latencyBenchRoot); os.IsNotExist(err) {
		b.Skipf("%s not found", latencyBenchRoot)
	}

	dir := filepath.Join(latencyBenchRoot, fmt.Sprintf("dio-latency-%d", os.Getpid()))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatalf("mkdir %s: %v", dir, err)
	}
	path := filepath.Join(dir, "data.bin")
	f, err := sys.CreateAndAllocate(path, 0, size)
	if err != nil {
		b.Fatalf("create %s: %v", path, err)
	}

	buf := align.AllocAligned(1 << 20)
	defer align.FreeAligned(buf)
	for i := range buf {
		buf[i] = byte(i)
	}
	for off := int64(0); off < size; off += int64(len(buf)) {
		n, err := f.WriteAt(buf, off)
		if err != nil {
			b.Fatalf("write at %d: %v", off, err)
		}
		if n != len(buf) {
			b.Fatalf("short write at %d: got %d want %d", off, n, len(buf))
		}
	}
	if err := f.Sync(); err != nil {
		b.Fatalf("sync: %v", err)
	}
	if err := f.Close(); err != nil {
		b.Fatalf("close filled file: %v", err)
	}

	direct, err := sys.OpenDirect(path, sys.FlDirectIO)
	if err != nil {
		b.Fatalf("open direct %s: %v", path, err)
	}
	b.Cleanup(func() {
		direct.Close()
		os.RemoveAll(dir)
	})
	return direct
}

func BenchmarkURingSubmitRoleLatency(b *testing.B) {
	if !IOUringAvailable {
		b.Skip("io_uring not available")
	}
	const fileSize = 1 << 30
	const blockSize = 4096

	for _, workers := range []int{8, 64} {
		b.Run(fmt.Sprintf("direct4k/workers=%d", workers), func(b *testing.B) {
			f := latencyBenchFile(b, fileSize)
			sched, err := NewURingScheduler(URingConfig{RingDepth: 512})
			if err != nil {
				b.Fatal(err)
			}
			defer sched.Close()

			recorder := &uringLatencyRecorder{}
			sched.mu.Lock()
			sched.trace = recorder
			sched.mu.Unlock()

			b.SetBytes(blockSize)
			b.ReportAllocs()
			b.ResetTimer()

			perG := b.N / workers
			extra := b.N % workers
			var wg sync.WaitGroup
			for g := range workers {
				count := perG
				if g < extra {
					count++
				}
				wg.Go(func() {
					buf := align.AllocAligned(blockSize)
					defer align.FreeAligned(buf)

					ops := make([]Op, 1)
					for i := range count {
						block := int64(i*workers + g)
						offset := (block * blockSize) % (fileSize - blockSize)
						ops[0] = ReadOp(f, buf, offset)
						if err := sched.Submit(ops); err != nil {
							b.Errorf("submit: %v", err)
							return
						}
						res := ops[0].Result()
						if res.Err != nil {
							b.Errorf("read: %v", res.Err)
							return
						}
						if res.N != blockSize {
							b.Errorf("short read: got %d want %d", res.N, blockSize)
							return
						}
					}
				})
			}
			wg.Wait()
			b.StopTimer()
			recorder.report(b)
			st := sched.Stats()
			if st.Syscalls > 0 {
				b.ReportMetric(float64(st.OpsPlaced)/float64(st.Syscalls), "ops/syscall")
			}
		})
	}
}
