//go:build linux

package iosched_test

// Benchmarks for comparing coordinator designs at controlled load. They use
// only the public API, so the same file runs against older trees. Reads and
// writes use O_DIRECT on files filled with real data, so every operation
// reaches the device. Run with TMPDIR on the NVMe file system.

import (
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/v2/iosched"
)

const coordBenchFileSize = 256 << 20

func coordBenchScheduler(b *testing.B) *iosched.URingScheduler {
	b.Helper()
	if !iosched.IOUringAvailable {
		b.Skip("io_uring not available")
	}
	s, err := iosched.NewURingScheduler(iosched.WithRingDepth(256))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := s.Close(); err != nil {
			b.Error(err)
		}
	})
	return s
}

// alignedBuffer returns n bytes of page-aligned memory, as O_DIRECT requires.
func alignedBuffer(b *testing.B, n int) []byte {
	b.Helper()
	mem, err := unix.Mmap(-1, 0, n, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := unix.Munmap(mem); err != nil {
			b.Error(err)
		}
	})
	return mem
}

// directFile returns an O_DIRECT file of coordBenchFileSize bytes of written
// data, so that reads reach the device rather than returning unwritten zeros.
func directFile(b *testing.B, name string) *os.File {
	b.Helper()
	f, err := os.OpenFile(filepath.Join(b.TempDir(), name), os.O_CREATE|os.O_RDWR|unix.O_DIRECT, 0o600)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := f.Close(); err != nil {
			b.Error(err)
		}
	})
	chunk := alignedBuffer(b, 1<<20)
	for i := range chunk {
		chunk[i] = byte(i * 7)
	}
	for off := int64(0); off < coordBenchFileSize; off += int64(len(chunk)) {
		if _, err := f.WriteAt(chunk, off); err != nil {
			b.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		b.Fatal(err)
	}
	return f
}

func cpuTime() time.Duration {
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
		panic(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

func submitWait(b *testing.B, s *iosched.URingScheduler, op iosched.Op, want int) {
	ticket, err := s.Submit(op)
	if err != nil {
		b.Error(err)
		return
	}
	if n, err := ticket.Wait(); err != nil || n != want {
		b.Errorf("n=%d err=%v, want n=%d", n, err, want)
	}
}

func reportLatencies(b *testing.B, latencies []time.Duration) {
	slices.Sort(latencies)
	at := func(q float64) float64 {
		return float64(latencies[int(q*float64(len(latencies)-1))].Microseconds())
	}
	b.ReportMetric(at(0.50), "p50-us")
	b.ReportMetric(at(0.99), "p99-us")
	b.ReportMetric(at(0.999), "p999-us")
}

// BenchmarkCoordReadBehindWrite times 4 KiB reads issued one at a time while
// another goroutine keeps one 1 MiB write in flight on a different file. A
// coordinator that waits for a completion before placing new work makes a
// read wait for the write it arrived behind.
func BenchmarkCoordReadBehindWrite(b *testing.B) {
	s := coordBenchScheduler(b)
	readFile := directFile(b, "read")
	writeFile := directFile(b, "write")
	writeBuf := alignedBuffer(b, 1<<20)
	readBuf := alignedBuffer(b, 4096)

	stop := make(chan struct{})
	var writes atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for off := int64(0); ; off = (off + int64(len(writeBuf))) % coordBenchFileSize {
			select {
			case <-stop:
				return
			default:
			}
			submitWait(b, s, iosched.WriteOp(writeFile, writeBuf, off), len(writeBuf))
			writes.Add(1)
		}
	}()
	time.Sleep(10 * time.Millisecond) // let the writer get going

	latencies := make([]time.Duration, 0, b.N)
	rng := rand.New(rand.NewPCG(1, 2))
	b.ResetTimer()
	start, cpu, writes0 := time.Now(), cpuTime(), writes.Load()
	for range b.N {
		off := rng.Int64N(coordBenchFileSize/4096) * 4096
		t := time.Now()
		submitWait(b, s, iosched.ReadOp(readFile, readBuf, off), len(readBuf))
		latencies = append(latencies, time.Since(t))
	}
	elapsed := time.Since(start)
	b.StopTimer()
	b.ReportMetric(float64((cpuTime()-cpu).Microseconds())/float64(b.N), "cpu-us/op")
	b.ReportMetric(float64(writes.Load()-writes0)/elapsed.Seconds(), "MiB-written/s")
	close(stop)
	wg.Wait()
	reportLatencies(b, latencies)
}

// BenchmarkCoordParallelReads runs n goroutines, each issuing reads one at a
// time, so n operations are in flight. ns/op is wall time per read across all
// of them; cpu-us/op is the process's CPU time per read, which counts the
// syscalls and wakeups each design spends.
func BenchmarkCoordParallelReads(b *testing.B) {
	for _, tc := range []struct {
		size int
		n    []int
	}{
		{4096, []int{1, 4, 16, 64}},
		{128 << 10, []int{64}},
	} {
		for _, n := range tc.n {
			b.Run(sizeName(tc.size)+"/goroutines="+strconv.Itoa(n), func(b *testing.B) {
				benchParallelReads(b, tc.size, n)
			})
		}
	}
}

func benchParallelReads(b *testing.B, size, goroutines int) {
	s := coordBenchScheduler(b)
	f := directFile(b, "read")
	bufs := make([][]byte, goroutines)
	for i := range bufs {
		bufs[i] = alignedBuffer(b, size)
	}
	var next atomic.Int64
	latencies := make([][]time.Duration, goroutines)
	b.SetBytes(int64(size))
	b.ResetTimer()
	cpu := cpuTime()
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), 3))
			for next.Add(1) <= int64(b.N) {
				off := rng.Int64N(coordBenchFileSize/int64(size)) * int64(size)
				t := time.Now()
				submitWait(b, s, iosched.ReadOp(f, bufs[g], off), size)
				latencies[g] = append(latencies[g], time.Since(t))
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64((cpuTime()-cpu).Microseconds())/float64(b.N), "cpu-us/op")
	reportLatencies(b, slices.Concat(latencies...))
}

func sizeName(n int) string {
	if n >= 1<<20 {
		return strconv.Itoa(n>>20) + "MiB"
	}
	return strconv.Itoa(n>>10) + "KiB"
}
