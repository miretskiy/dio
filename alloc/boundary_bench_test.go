package alloc

import (
	"testing"

	"github.com/miretskiy/dio/v2/internal/buildutil"
)

// Each operation satisfies the same complete capacity request with a fixed
// neighboring allocation. Reuse the Buffer's handle slice before timing.
func BenchmarkBufferBoundary(b *testing.B) {
	defer buildutil.EnableTestMode(false)()
	for _, test := range []struct {
		name     string
		occupied uint64
		pages    int
	}{
		{"Occupied1_120K", 1 << 1, 30},
		{"OccupiedEnds_120K", 1 | 1<<31, 30},
	} {
		b.Run(test.name, func(b *testing.B) {
			a := newAllocator(b, spanBytes, WithoutZeroing())
			var held [spanPages]Memory
			for i := range held {
				m, err := a.TryAcquire(4096)
				if err != nil {
					b.Fatal(err)
				}
				held[i] = m
			}
			for i, m := range held {
				if test.occupied&(1<<i) == 0 {
					m.Unpin()
				} else {
					defer m.Unpin()
				}
			}
			buffer := NewAllocatingBuffer(a)
			size := test.pages * 4096
			if err := buffer.Grow(size); err != nil {
				b.Fatal(err)
			}
			memories, capacity := len(buffer.RawAllocations()), buffer.Cap()
			buffer.Release()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := buffer.Grow(size); err != nil {
					b.Fatal(err)
				}
				buffer.Release()
			}
			b.StopTimer()
			b.ReportMetric(float64(memories), "memories/op")
			b.ReportMetric(float64(capacity), "held-B/op")
		})
	}
}
