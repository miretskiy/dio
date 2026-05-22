//go:build linux

// Fan-out benchmarks for the io_uring scheduler.
//
// Each benchmark is self-contained: it allocates its own files under
// /instance_storage, fills the source, runs, and cleans up via b.Cleanup.
// Benchmarks are skipped when /instance_storage is absent.
//
// Usage:
//
//	go test ./iosched/... -bench=BenchmarkFanout -benchtime=1000x -v
//
//	perf stat -e context-switches,cpu-migrations,instructions,cycles,\
//	    syscalls:sys_enter_io_uring_enter,syscalls:sys_enter_pread64,syscalls:sys_enter_pwrite64 \
//	    go test ./iosched/... -bench=BenchmarkFanout_URing -benchtime=1000x
package iosched_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/iosched"
	"github.com/miretskiy/dio/mempool"
	"github.com/miretskiy/dio/sys"
)

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

// ── Naive: heap buffer + goroutines ──────────────────────────────────────────

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
			wg.Go(func() {
				_, errs[si] = unix.Pwrite(int(ff.sinks[si].Fd()), buf[:n], offset)
				runtime.KeepAlive(ff.sinks[si])
			})
		}
		wg.Wait()

		for si, werr := range errs {
			if werr != nil {
				b.Fatalf("pwrite sink%d at %d: %v", si+1, offset, werr)
			}
		}
	}
}

// ── io_uring: registered fixed buffers ───────────────────────────────────────

// BenchmarkFanout_URing copies one 1 MiB chunk per iteration using io_uring:
//  1. IORING_OP_READ_FIXED into a registered slab slot.
//  2. Three IORING_OP_WRITE_FIXED submitted as independent ops (parallel).
func BenchmarkFanout_URing(b *testing.B) {
	if !iosched.IOUringAvailable {
		b.Skip("io_uring not available")
	}

	ff := setupFanout(b)

	sched, err := iosched.NewURingScheduler(iosched.URingConfig{RingDepth: 256})
	if err != nil {
		b.Fatal(err)
	}
	defer sched.Close()

	// 1 GiB slab, 1 MiB slots.  Size is a 2 MiB multiple → MAP_HUGETLB attempted.
	pool, err := mempool.NewSlabPool(1<<30, fanoutChunk)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()

	if err := iosched.RegisterDMASlab(sched, pool); err != nil {
		b.Fatal(err)
	}

	writeOps := make([]iosched.Op, fanoutNumSinks)

	b.SetBytes(fanoutChunk * (1 + fanoutNumSinks))
	b.ResetTimer()

	readTicket := iosched.NewTicket()
	defer readTicket.Release()
	writeTicket := iosched.NewTicket()
	defer writeTicket.Release()

	for i := range b.N {
		offset := int64(i%fanoutNumChunks) * fanoutChunk

		slot, err := pool.Acquire()
		if err != nil {
			b.Fatalf("acquire slot: %v", err)
		}

		// Fixed read.
		readTicket.Ops = []iosched.Op{iosched.ReadFixedOp(ff.src, slot, offset)}
		if err := sched.Submit(readTicket); err != nil {
			slot.Release()
			b.Fatalf("submit read: %v", err)
		}
		readTicket.Wait()
		if rerr := readTicket.Ops[0].Result().Err; rerr != nil {
			slot.Release()
			b.Fatalf("read at %d: %v", offset, rerr)
		}

		// Fan-out: all sinks in one batch, dispatched in parallel by the ring.
		for si := range fanoutNumSinks {
			writeOps[si] = iosched.WriteFixedOp(ff.sinks[si], slot, offset)
		}
		writeTicket.Ops = writeOps
		if err := sched.Submit(writeTicket); err != nil {
			slot.Release()
			b.Fatalf("submit writes: %v", err)
		}
		writeTicket.Wait()
		for si := range fanoutNumSinks {
			if werr := writeTicket.Ops[si].Result().Err; werr != nil {
				slot.Release()
				b.Fatalf("write sink%d at %d: %v", si+1, offset, werr)
			}
		}

		slot.Release()
	}
	b.StopTimer()
	reportSchedStats(b, sched)
}
