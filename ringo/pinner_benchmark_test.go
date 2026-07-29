package ringo

import (
	"fmt"
	"runtime"
	"testing"
)

// pinnerBenchmarkTarget is large enough to avoid the tiny allocator. Each
// target is a distinct Go heap object, matching independent kernel-visible
// buffers or auxiliary structures.
type pinnerBenchmarkTarget [64]byte

// BenchmarkRuntimePinner records the cost of the pinning strategy Ringo
// deliberately does not use.
//
// Counts around five are intentional: runtime.Pinner currently has inline
// reference storage for five objects. Larger counts exercise its fallback
// allocation path, which matters for vectored I/O.
func BenchmarkRuntimePinner(b *testing.B) {
	for _, count := range []int{1, 2, 5, 6} {
		b.Run(fmt.Sprintf("distinct-%04d", count), func(b *testing.B) {
			targets := make([]*pinnerBenchmarkTarget, count)
			for i := range targets {
				targets[i] = new(pinnerBenchmarkTarget)
			}

			var pinner runtime.Pinner
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for _, target := range targets {
					pinner.Pin(target)
				}
				pinner.Unpin()
			}
		})
	}
}
