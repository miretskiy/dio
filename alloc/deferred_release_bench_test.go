package alloc

import (
	"fmt"
	"math/bits"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/miretskiy/dio/v2/collections"
)

// A test-only prototype compares the current span release with per-span queues.
// Lease retirement is unchanged and excluded here. Nodes are preallocated by
// page, so the stack prototype pays no heap-allocation cost on release.
type releaseRequest struct {
	block block
	link  collections.Link[releaseRequest]
}

type deferredReleases struct {
	arena  *arena
	mode   string
	onFull bool
	nodes  []releaseRequest
	stacks []*collections.TreiberStack[releaseRequest]
	masks  []atomic.Uint32
}

// Separate both the 8-byte span words and 4-byte pending masks by a cache line
// in private-span workloads, so they measure collection rather than false sharing.
const deferredSpanStride = 16

func newDeferredReleases(a *arena, mode string) *deferredReleases {
	d := &deferredReleases{arena: a, mode: mode}
	if mode == "StackOnFull" || mode == "BitmapOnFull" {
		d.mode, d.onFull = mode[:len(mode)-len("OnFull")], true
	}
	if d.mode == "Stack" {
		d.nodes = make([]releaseRequest, len(a.words)*spanPages)
		d.stacks = make([]*collections.TreiberStack[releaseRequest], len(a.words))
		for i := range d.stacks {
			d.stacks[i] = collections.NewTreiberStack(func(r *releaseRequest) *collections.Link[releaseRequest] { return &r.link })
		}
	} else if d.mode == "Bitmap" {
		d.masks = make([]atomic.Uint32, len(a.words))
	}
	return d
}

func (d *deferredReleases) claim(span, pages int) (block, bool) {
	if !d.onFull {
		d.collect(span)
	}
	b, _, ok := d.arena.claimNode(span, pages)
	if !ok && d.onFull {
		d.collect(span)
		b, _, ok = d.arena.claimNode(span, pages)
	}
	return b, ok
}

func (d *deferredReleases) release(span int, b block) {
	switch d.mode {
	case "Direct":
		d.arena.freeSpan(span, b.start, b.n)
	case "Stack":
		r := &d.nodes[span*spanPages+b.start]
		r.block = b
		d.stacks[span].Push(r)
	case "Bitmap":
		d.masks[span].Or(uint32((uint64(1)<<b.n - 1) << b.start))
	}
}

func (d *deferredReleases) collect(span int) {
	var mask uint32
	switch d.mode {
	case "Stack":
		d.stacks[span].ConsumeAll(func(r *releaseRequest) {
			mask |= uint32((uint64(1)<<r.block.n - 1) << r.block.start)
		})
	case "Bitmap":
		if d.masks[span].Load() != 0 {
			mask = d.masks[span].Swap(0)
		}
	}
	if mask == 0 {
		return
	}
	// Multiple collectors can publish disjoint detached batches. Reservations
	// still race them, so applying a batch requires a span CAS, not a store.
	word := &d.arena.words[span]
	for {
		old := spanWord(word.Load())
		next := old
		for left := mask; left != 0; {
			start := bits.TrailingZeros32(left)
			n := min(spanPages-start, bits.TrailingZeros32(^left>>start))
			next = next.release(block{start: start, n: n})
			left &^= uint32((uint64(1)<<n - 1) << start)
		}
		if word.CompareAndSwap(uint64(old), uint64(next)) {
			return
		}
	}
}

func BenchmarkDeferredRelease(b *testing.B) {
	for _, pages := range []int{1, 2, 4, 8, 16, spanPages} {
		for _, shared := range []bool{false, true} {
			workload := "Private"
			if shared {
				workload = "Shared"
			}
			b.Run(fmt.Sprintf("%s%dK", workload, pages*4), func(b *testing.B) {
				for _, mode := range []string{"Direct", "Stack", "Bitmap", "StackOnFull", "BitmapOnFull"} {
					b.Run(mode, func(b *testing.B) {
						workers := runtime.GOMAXPROCS(0)
						a := newArena(workers * deferredSpanStride)
						d := newDeferredReleases(a, mode)
						b.ReportAllocs()
						benchmarkWorkers(b, workers, func(worker, n int) {
							span := worker * deferredSpanStride
							if shared {
								span = 0
							}
							for range n {
								for {
									claim, ok := d.claim(span, pages)
									if ok {
										d.release(span, claim)
										break
									}
									runtime.Gosched()
								}
							}
						})
						for i := range a.words {
							d.collect(i)
						}
						if a.available() != len(a.words)*spanPages {
							b.Fatal("deferred releases lost capacity")
						}
					})
				}
			})
		}
	}
}

func TestDeferredReleasePrototype(t *testing.T) {
	for _, mode := range []string{"Stack", "Bitmap"} {
		t.Run(mode, func(t *testing.T) {
			a := newArena(1)
			d := newDeferredReleases(a, mode)
			for page := range spanPages {
				claim, _, ok := a.claimNode(0, 1)
				if !ok || claim.start != page {
					t.Fatal("claim failed before exhaustion")
				}
				d.release(0, claim)
			}
			if a.available() != 0 {
				t.Fatal("queued pages should remain reserved until collection")
			}
			d.collect(0)
			if a.available() != spanPages {
				t.Fatal("collection failed to release the complete batch")
			}
		})
	}
}

// The producer claims pages while a separate goroutine releases them, as with
// I/O completions. Each producer uses its own span; queueing can batch several
// completions before that producer next checks its span. Channel cost is shared
// by all modes and included in timing.
func BenchmarkDeferredRemoteRelease(b *testing.B) {
	for _, mode := range []string{"Direct", "Stack", "Bitmap", "StackOnFull", "BitmapOnFull"} {
		b.Run(mode, func(b *testing.B) {
			workers := runtime.GOMAXPROCS(0)
			a := newArena(workers * deferredSpanStride)
			d := newDeferredReleases(a, mode)
			queues := make([]chan block, workers)
			done := make([]chan struct{}, workers)
			for worker := range workers {
				queues[worker], done[worker] = make(chan block, spanPages), make(chan struct{})
				go func() {
					defer close(done[worker])
					for claim := range queues[worker] {
						d.release(worker*deferredSpanStride, claim)
					}
				}()
			}
			b.ReportAllocs()
			benchmarkWorkers(b, workers, func(worker, n int) {
				span := worker * deferredSpanStride
				for range n {
					for {
						claim, ok := d.claim(span, 1)
						if ok {
							queues[worker] <- claim
							break
						}
						runtime.Gosched()
					}
				}
				close(queues[worker])
				<-done[worker]
				d.collect(span)
			})
			if a.available() != len(a.words)*spanPages {
				b.Fatal("remote releases lost capacity")
			}
		})
	}
}
