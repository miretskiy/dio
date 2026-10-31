package alloc

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"
)

// Mixed request sizes in pages, weighted toward small buffers.
var spanBenchMix = []struct{ pages, weight int }{
	{1, 35}, {2, 10}, {3, 10}, {4, 10}, {6, 5}, {8, 10}, {16, 10}, {24, 5}, {64, 5},
}

func spanBenchPick(r *rand.Rand) int {
	n := r.IntN(100)
	for _, m := range spanBenchMix {
		if n < m.weight {
			return m.pages
		}
		n -= m.weight
	}
	panic("weights must sum to 100")
}

// spanFill holds allocations until requested pages reach target of capacity.
func spanFill(a *SpanAllocator, target float64, r *rand.Rand) {
	type live struct{ page, n, k int }
	var lives []live
	used, capacity := 0, len(a.spans)*spanPages
	for range 2_000_000 {
		if float64(used) < target*float64(capacity) {
			k := spanBenchPick(r)
			if page, n, ok := a.tryAcquire(k); ok {
				lives = append(lives, live{page, n, k})
				used += k
			}
			continue
		}
		i := r.IntN(len(lives))
		a.release(lives[i].page, lives[i].n)
		used -= lives[i].k
		lives[i] = lives[len(lives)-1]
		lives = lives[:len(lives)-1]
	}
}

func BenchmarkSpanAllocator(b *testing.B) {
	for _, workers := range []int{1, 32} {
		// One 4 KiB acquisition and release through the public API.
		b.Run(fmt.Sprintf("Memory4K/span/workers%d", workers), func(b *testing.B) {
			a := newSpanAllocator(b, 256, WithWarmup())
			b.ReportAllocs()
			benchmarkWorkers(b, workers, func(w, n int) {
				for range n {
					m, err := a.TryAcquire(4096)
					if err != nil {
						b.Error(err)
						return
					}
					m.data[0] = byte(w)
					m.Unpin()
				}
			})
		})
		b.Run(fmt.Sprintf("Memory4K/slab/workers%d", workers), func(b *testing.B) {
			a, err := NewSlabAllocator(256*spanBytes, WithWarmup())
			if err != nil {
				b.Fatal(err)
			}
			defer a.Close()
			b.ReportAllocs()
			benchmarkWorkers(b, workers, func(w, n int) {
				for range n {
					m, err := a.TryAcquire()
					if err != nil {
						b.Error(err)
						return
					}
					m.data[0] = byte(w)
					m.Unpin()
				}
			})
		})
		// The algorithm alone: no Memory handle, no zeroing.
		b.Run(fmt.Sprintf("Pages4K/workers%d", workers), func(b *testing.B) {
			a := newSpanAllocator(b, 256)
			benchmarkWorkers(b, workers, func(_, n int) {
				for range n {
					page, k, ok := a.tryAcquire(1)
					if !ok {
						b.Error("exhausted")
						return
					}
					a.release(page, k)
				}
			})
		})
		// Mixed sizes against a 1 GiB region churned to ~70% occupancy.
		b.Run(fmt.Sprintf("PagesMixed70/workers%d", workers), func(b *testing.B) {
			a := newSpanAllocator(b, 4096)
			spanFill(a, 0.7, rand.New(rand.NewPCG(1, 2)))
			var fails atomic.Int64
			benchmarkWorkers(b, workers, func(w, n int) {
				r := rand.New(rand.NewPCG(uint64(w), 3))
				f := 0
				for range n {
					page, k, ok := a.tryAcquire(spanBenchPick(r))
					if !ok {
						f++
						continue
					}
					a.release(page, k)
				}
				fails.Add(int64(f))
			})
			b.ReportMetric(float64(fails.Load())/float64(b.N), "fail/op")
		})
	}
	// Swiss cheese: every span is a 4 KiB class span with every other slot
	// free. Half the region is free, but no span is.
	for _, gib := range []int{1, 16} {
		a := newSpanAllocator(b, gib<<12)
		var pages []int
		for {
			page, _, ok := a.tryAcquire(1)
			if !ok {
				break
			}
			pages = append(pages, page)
		}
		for i, page := range pages {
			if i%2 == 0 {
				a.release(page, 1)
			}
		}
		for _, test := range []struct {
			name  string
			pages int
			fit   bool
		}{
			{"4K-hit", 1, true},
			{"32K-miss", 8, false},
			{"512K-miss", 128, false},
		} {
			b.Run(fmt.Sprintf("SwissCheese/gib%d/%s", gib, test.name), func(b *testing.B) {
				for b.Loop() {
					page, k, ok := a.tryAcquire(test.pages)
					if ok != test.fit {
						b.Fatalf("fit = %v", ok)
					}
					if ok {
						a.release(page, k)
					}
				}
			})
		}
	}
}
