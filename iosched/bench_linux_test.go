package iosched_test

// Benchmarks for comparing coordinator designs at controlled load. They use
// only the public API, so the same file runs against older trees. Reads and
// writes use O_DIRECT on files filled with real data, so every operation
// reaches the device. Run with TMPDIR on the NVMe file system.

import (
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/sys"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
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
	s, err := iosched.NewURingScheduler(
		iosched.WithRingDepth(4096),
		iosched.WithCoalescing(!disable),
	)
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
				if _, err := tk.Wait(); err != nil {
					b.Error(err)
				}
			}
		}
	})

}

const (
	fanoutStageRoot = "/instance_storage"
	fanoutFileSize  = 10 << 30 // 10 GiB per file
	fanoutChunk     = 1 << 20  // 1 MiB I/O unit
	fanoutNumSinks  = 3        // 1 local + 2 remote replicas (simulated)
	fanoutNumChunks = fanoutFileSize / fanoutChunk
)

// fanoutFiles holds the open file descriptors for one benchmark run.
type fanoutFiles struct {
	src   *os.File
	sinks [fanoutNumSinks]*os.File
}

// setupFanout allocates and opens all benchmark files. The source is filled
// with dd(1) so that O_DIRECT reads actually hit the storage controller.
// b.Cleanup handles teardown regardless of how the benchmark exits.
func setupFanout(b *testing.B) *fanoutFiles {
	b.Helper()

	if _, err := os.Stat(fanoutStageRoot); os.IsNotExist(err) {
		b.Skipf("%s not found", fanoutStageRoot)
	}

	dir := filepath.Join(fanoutStageRoot, fmt.Sprintf("dio-fanout-%d", os.Getpid()))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatalf("mkdir %s: %v", dir, err)
	}

	srcPath := filepath.Join(dir, "source.bin")
	sinkPaths := [fanoutNumSinks]string{}
	for i := range fanoutNumSinks {
		sinkPaths[i] = filepath.Join(dir, fmt.Sprintf("sink%d.bin", i+1))
	}

	// Pre-allocate all files so the NVMe has real blocks assigned upfront.
	for _, p := range append([]string{srcPath}, sinkPaths[:]...) {
		f, err := sys.CreateAndAllocate(p, 0, fanoutFileSize)
		if err != nil {
			b.Fatalf("allocate %s: %v", p, err)
		}
		f.Close()
	}

	// Fill source with zeros via dd so blocks transition from "unwritten"
	// to "written" — otherwise O_DIRECT reads may bypass the NVMe entirely.
	cmd := exec.Command("dd",
		"if=/dev/zero",
		"of="+srcPath,
		"bs=1M",
		fmt.Sprintf("count=%d", fanoutFileSize>>20),
		"conv=notrunc",
		"status=none",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		b.Fatalf("dd fill source: %v: %s", err, out)
	}

	// Open source read-only with O_DIRECT.
	src, err := sys.OpenDirect(srcPath, sys.FlDirectIO)
	if err != nil {
		b.Fatalf("open source: %v", err)
	}

	// Open sinks write-only with O_DIRECT (no truncation).
	var ff fanoutFiles
	ff.src = src
	for i, p := range sinkPaths {
		fd, err := unix.Open(p, unix.O_WRONLY|unix.O_DIRECT, 0)
		if err != nil {
			b.Fatalf("open sink%d: %v", i+1, err)
		}
		ff.sinks[i] = os.NewFile(uintptr(fd), p)
	}

	b.Cleanup(func() {
		ff.src.Close()
		for _, f := range ff.sinks {
			f.Close()
		}
		os.RemoveAll(dir)
	})

	return &ff
}

// BenchmarkFanout_Naive copies one 1 MiB chunk per iteration:
// one O_DIRECT pread from source, three concurrent O_DIRECT pwrites to sinks.
func BenchmarkFanout_Naive(b *testing.B) {
	ff := setupFanout(b)

	buf := align.AllocAligned(fanoutChunk)
	defer align.FreeAligned(buf)

	errs := make([]error, fanoutNumSinks)
	b.SetBytes(fanoutChunk * (1 + fanoutNumSinks))
	b.ResetTimer()

	for i := range b.N {
		offset := int64(i%fanoutNumChunks) * fanoutChunk

		n, err := unix.Pread(int(ff.src.Fd()), buf, offset)
		runtime.KeepAlive(ff.src)
		if err != nil {
			b.Fatalf("pread at %d: %v", offset, err)
		}

		var wg sync.WaitGroup
		for si := range fanoutNumSinks {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[si] = unix.Pwrite(int(ff.sinks[si].Fd()), buf[:n], offset)
				runtime.KeepAlive(ff.sinks[si])
			}()
		}
		wg.Wait()

		for si, werr := range errs {
			if werr != nil {
				b.Fatalf("pwrite sink%d at %d: %v", si+1, offset, werr)
			}
		}
	}
}

// BenchmarkFanout_URing copies one 1 MiB chunk per iteration using io_uring:
//  1. IORING_OP_READ_FIXED into a registered staging buffer.
//  2. Three IORING_OP_WRITE_FIXED submitted as independent ops (parallel).
func BenchmarkFanout_URing(b *testing.B) {
	if !iosched.IOUringAvailable {
		b.Skip("io_uring not available")
	}

	ff := setupFanout(b)

	// The caller keeps one staging buffer registered through scheduler shutdown.
	slot := alloc.NewMemory(fanoutChunk)
	defer slot.Unpin()
	sched, err := iosched.NewURingScheduler(
		iosched.WithRingDepth(256),
		iosched.WithDMABuffers(slot.Bytes()),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := sched.Close(); err != nil {
			b.Errorf("close io_uring scheduler: %v", err)
		}
	}()

	writeTickets := make([]iosched.Ticket, fanoutNumSinks)

	b.SetBytes(fanoutChunk * (1 + fanoutNumSinks))
	b.ResetTimer()

	for i := range b.N {
		offset := int64(i%fanoutNumChunks) * fanoutChunk

		// Fixed read.
		readTicket, err := sched.Submit(iosched.ReadFixedOp(ff.src, slot.Bytes(), offset))
		if err != nil {
			b.Fatalf("submit read: %v", err)
		}
		if _, rerr := readTicket.Wait(); rerr != nil {
			b.Fatalf("read at %d: %v", offset, rerr)
		}

		// Fan-out: submit all sinks before waiting, allowing the coordinator
		// to place them in the same ring wave.
		for si := range fanoutNumSinks {
			writeTicket, err := sched.Submit(iosched.WriteFixedOp(ff.sinks[si], slot.Bytes(), offset))
			if err != nil {
				b.Fatalf("submit write sink%d: %v", si+1, err)
			}
			writeTickets[si] = writeTicket
		}
		for si := range fanoutNumSinks {
			if _, werr := writeTickets[si].Wait(); werr != nil {
				b.Fatalf("write sink%d at %d: %v", si+1, offset, werr)
			}
		}
	}
	b.StopTimer()
}
