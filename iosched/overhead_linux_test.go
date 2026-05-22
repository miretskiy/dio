//go:build linux

package iosched_test

import (
	"runtime"
	"syscall"
	"testing"

	"github.com/miretskiy/dio/align"
)

// BenchmarkDirectPread_ReadAt_Serial is the direct syscall baseline for the
// scheduler's single-op Submit overhead. It uses the same benchFile helper as
// the scheduler benchmarks so file placement and warmup are comparable.
func BenchmarkDirectPread_ReadAt_Serial(b *testing.B) {
	const fileSize = 64 << 20
	const blockSize = 4096

	f := benchFile(b, fileSize)
	buf := align.AllocAligned(blockSize)
	defer align.FreeAligned(buf)

	b.SetBytes(blockSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		offset := int64((i * blockSize) % (fileSize - blockSize))
		n, err := syscall.Pread(int(f.Fd()), buf, offset)
		runtime.KeepAlive(f)
		if err != nil {
			b.Fatalf("pread: %v", err)
		}
		if n != blockSize {
			b.Fatalf("short read: got %d want %d", n, blockSize)
		}
	}
}
