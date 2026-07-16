//go:build linux

package iosched_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/iosched"
)

const benchStorageRoot = "/instance_storage"

func benchFilePath(b *testing.B, size int64) string {
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
		os.RemoveAll(dir)
	})
	if err := f.Close(); err != nil {
		b.Fatalf("close %s: %v", path, err)
	}
	return path
}

func benchFile(b *testing.B, size int64) *os.File {
	b.Helper()
	path := benchFilePath(b, size)
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		b.Fatalf("open %s: %v", path, err)
	}
	b.Cleanup(func() {
		f.Close()
	})
	return f
}

const (
	readBenchFileSize  = 1 << 30 // 1 GiB
	readBenchBlockSize = 4096
	readBenchBlocks    = readBenchFileSize / readBenchBlockSize
	readBenchDepth     = 1024
)

func recordBenchErr(once *sync.Once, dst *error, err error) {
	once.Do(func() {
		*dst = err
	})
}

func BenchmarkRegularIO(b *testing.B) {
	if !iosched.IOUringAvailable {
		b.Skip("io_uring not available")
	}

	path := benchFilePath(b, readBenchFileSize)
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		b.Fatalf("open %s: %v", path, err)
	}
	b.Cleanup(func() { f.Close() })

	sched, err := iosched.NewURingScheduler(iosched.URingConfig{RingDepth: readBenchDepth})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { sched.Close() })

	var seq atomic.Uint64
	var errOnce sync.Once
	var benchErr error

	b.SetBytes(readBenchBlockSize)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, readBenchBlockSize)
		for pb.Next() {
			block := (seq.Add(1) - 1) % readBenchBlocks
			offset := int64(block * readBenchBlockSize)
			ticket, err := sched.Submit(iosched.ReadOp(f, buf, offset))
			if err != nil {
				recordBenchErr(&errOnce, &benchErr, fmt.Errorf("submit: %w", err))
				return
			}
			ticket.Wait()
			if err := ticket.Error(); err != nil {
				recordBenchErr(&errOnce, &benchErr, fmt.Errorf("read at %d: %w", offset, err))
				return
			} else if ticket.N() != readBenchBlockSize {
				recordBenchErr(&errOnce, &benchErr, fmt.Errorf("short read at %d: %d", offset, ticket.N()))
				return
			}
		}
	})
	b.StopTimer()
	if benchErr != nil {
		b.Fatal(benchErr)
	}
}

func BenchmarkDirectIO(b *testing.B) {
	if !iosched.IOUringAvailable {
		b.Skip("io_uring not available")
	}

	path := benchFilePath(b, readBenchFileSize)
	sched, err := iosched.NewURingScheduler(iosched.URingConfig{
		RingDepth: readBenchDepth,
		VFiles:    1,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if ticket, err := sched.Submit(iosched.VCloseOp(0)); err == nil {
			ticket.Wait()
		}
		sched.Close()
	})

	openTicket, err := sched.Submit(iosched.VOpenatOp(unix.AT_FDCWD, path, unix.O_RDONLY, 0, 0))
	if err != nil {
		b.Fatalf("submit vopen: %v", err)
	}
	openTicket.Wait()
	if err := openTicket.Error(); err != nil {
		b.Fatalf("vopen: %v", err)
	}

	var seq atomic.Uint64
	var errOnce sync.Once
	var benchErr error

	b.SetBytes(readBenchBlockSize)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, readBenchBlockSize)
		for pb.Next() {
			block := (seq.Add(1) - 1) % readBenchBlocks
			offset := int64(block * readBenchBlockSize)
			ticket, err := sched.Submit(iosched.VReadOp(0, buf, offset))
			if err != nil {
				recordBenchErr(&errOnce, &benchErr, fmt.Errorf("submit: %w", err))
				return
			}
			ticket.Wait()
			if err := ticket.Error(); err != nil {
				recordBenchErr(&errOnce, &benchErr, fmt.Errorf("read at %d: %w", offset, err))
				return
			} else if ticket.N() != readBenchBlockSize {
				recordBenchErr(&errOnce, &benchErr, fmt.Errorf("short read at %d: %d", offset, ticket.N()))
				return
			}
		}
	})
	b.StopTimer()
	if benchErr != nil {
		b.Fatal(benchErr)
	}
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
		if err := ticket.Error(); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
	b.StopTimer()
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
			if err := ticket.Error(); err != nil {
				b.Fatalf("read: %v", err)
			}
		}
	})
	b.StopTimer()
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
				wg.Add(1)
				go func() {
					defer wg.Done()
					buf := make([]byte, blockSize)
					for i := range count {
						offset := int64((i * blockSize) % (fileSize - blockSize))
						ticket, err := sched.Submit(iosched.ReadOp(f, buf, offset))
						if err != nil {
							b.Errorf("submit: %v", err)
							return
						}
						ticket.Wait()
						if err := ticket.Error(); err != nil {
							b.Errorf("read: %v", err)
							return
						}
					}
				}()
			}
			wg.Wait()
			b.StopTimer()
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
			tickets := make([]iosched.Ticket, batch)
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
					if err := ticket.Error(); err != nil {
						b.Fatalf("read %d: %v", j, err)
					}
				}
			}
			b.StopTimer()
		})
	}
}
