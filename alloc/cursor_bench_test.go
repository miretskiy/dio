package alloc

import (
	"fmt"
	"runtime"
	"testing"
)

// Isolate the cost of checking a full preferred shard before reusing a known
// free overflow span. The warm case measures the same claim/release without
// the scan. No leases, zeroing, or concurrent workers obscure search cost.
func BenchmarkArenaOverflow(b *testing.B) {
	for _, home := range []int{16, 64, 512, 4096} {
		for _, overflow := range []bool{false, true} {
			b.Run(fmt.Sprintf("Home%d/overflow%t", home, overflow), func(b *testing.B) {
				a := newArena(home + 1)
				a.cursors = []arenaCursor{{end: home}}
				a.cursors[0].position.Store(uint64(home) << 32)
				if overflow {
					for i := range home {
						a.words[i].Store(uint64(fullSpan))
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					block, ok := a.claim(1)
					if !ok {
						b.Fatal("exhausted despite free overflow span")
					}
					a.freePages(block.start, block.n)
				}
			})
		}
	}
}

var cursorBudgets = []struct {
	name       string
	multiplier int
}{
	{"PerSpan", -1}, {"P", 1}, {"2P", 2}, {"4P", 4},
}

// Compare a 4 MiB request made of separate claims with one batch when the
// preferred shard is full. Both modes acquire and release the same 32 spans.
func BenchmarkArenaOverflowBatch(b *testing.B) {
	for _, home := range []int{64, 512, 4096} {
		for _, batched := range []bool{false, true} {
			b.Run(fmt.Sprintf("Home%d/batch%t", home, batched), func(b *testing.B) {
				a := newArena(home + 32)
				a.cursors = []arenaCursor{{end: home}}
				a.cursors[0].position.Store(uint64(home) << 32)
				for i := range home {
					a.words[i].Store(uint64(fullSpan))
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if batched {
						batch := a.startBatch()
						for range 32 {
							if _, ok := batch.claim(a, spanPages); !ok {
								b.Fatal("batch exhausted despite free overflow capacity")
							}
						}
						batch.save()
					} else {
						for range 32 {
							if _, ok := a.claim(spanPages); !ok {
								b.Fatal("claim exhausted despite free overflow capacity")
							}
						}
					}
					a.freePages(home*spanPages, 32*spanPages)
				}
			})
		}
	}
}

// Zero uses the production default; -1 retains one cursor per span.
func setBenchmarkCursorBudget(a *arena, multiplier int) {
	if multiplier < 0 {
		a.initCursors(len(a.words))
	} else if multiplier > 0 {
		a.initCursors(multiplier * runtime.GOMAXPROCS(0))
	}
}

// Reuse the allocator workloads to compare cursor budgets without diagnostic
// counters on the measured allocation path.
func BenchmarkAllocatorCursorBudget(b *testing.B) {
	for _, budget := range cursorBudgets {
		b.Run(budget.name, func(b *testing.B) {
			benchmarkAllocator(b, budget.multiplier)
		})
	}
}

// This diagnostic counts actual save outcomes using worker-local counters.
// Its timings include instrumentation and omit Memory leases and merging;
// BenchmarkAllocatorCursorBudget measures the complete allocation path.
func BenchmarkArenaCursorContention(b *testing.B) {
	for _, test := range []struct {
		name        string
		pages       int
		workerScale int
	}{
		{"4K", 1, 1},
		{"128K", spanPages, 1},
		{"4M", 32 * spanPages, 1},
		{"4M_8xWorkers", 32 * spanPages, 8},
	} {
		b.Run(test.name, func(b *testing.B) {
			for _, budget := range cursorBudgets {
				b.Run(budget.name, func(b *testing.B) {
					workers := test.workerScale * runtime.GOMAXPROCS(0)
					a := newArena(workers * 64) // 8 MiB per worker, matching the Buffer workload.
					setBenchmarkCursorBudget(a, budget.multiplier)
					type counts struct {
						claims, attempts, losses uint64
						cursors                  []uint64
					}
					results := make([]counts, workers)
					blocks := make([][]block, workers)
					for i := range results {
						results[i].cursors = make([]uint64, len(a.cursors))
						blocks[i] = make([]block, 0, max(1, test.pages/spanPages))
					}
					b.ReportAllocs()
					benchmarkWorkers(b, workers, func(worker, n int) {
						local := results[worker]
						for range n {
							claimed := blocks[worker][:0]
							for remaining := test.pages; remaining > 0; {
								index := entryIndex(len(a.cursors))
								cursor := &a.cursors[index]
								initial := cursor.position.Load()
								block, position, ok := a.claimFrom(cursor, initial, min(remaining, spanPages))
								saved := cursor.save(initial, position)
								local.claims++
								if position != initial {
									local.attempts++
								}
								if !saved {
									local.losses++
								}
								local.cursors[index]++
								if !ok {
									b.Error("exhausted despite spare capacity")
									for _, block := range claimed {
										a.freePages(block.start, block.n)
									}
									results[worker] = local
									return
								}
								claimed = append(claimed, block)
								remaining -= block.n
							}
							for _, block := range claimed {
								a.freePages(block.start, block.n)
							}
						}
						results[worker] = local
					})
					var claims, attempts, losses uint64
					users := make([]int, len(a.cursors))
					for _, result := range results {
						claims += result.claims
						attempts += result.attempts
						losses += result.losses
						for cursor, uses := range result.cursors {
							if uses != 0 {
								users[cursor]++
							}
						}
					}
					used, peak := 0, 0
					for _, count := range users {
						if count != 0 {
							used++
							peak = max(peak, count)
						}
					}
					b.ReportMetric(float64(len(a.cursors)), "cursors")
					b.ReportMetric(float64(used), "used-cursors")
					b.ReportMetric(float64(peak), "max-workers/cursor")
					b.ReportMetric(float64(workers), "workers")
					b.ReportMetric(float64(claims)/float64(b.N), "claims/op")
					b.ReportMetric(float64(attempts)/float64(b.N), "cursor-CAS/op")
					b.ReportMetric(100*float64(losses)/float64(max(1, attempts)), "cursor-CAS-lost-%")
					b.ReportMetric(float64(losses), "cursor-CAS-losses")
					b.ReportMetric(float64(attempts), "cursor-CAS-attempts")
					if free := a.available(); free != len(a.words)*spanPages {
						b.Fatalf("leaked pages: %d", len(a.words)*spanPages-free)
					}
				})
			}
		})
	}
}
