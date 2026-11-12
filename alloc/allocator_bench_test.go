package alloc

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/internal/buildutil"
)

// Start workers before timing. Each worker owns its counters and samples.
func benchmarkWorkers(b *testing.B, workers int, work func(worker, n int)) {
	b.Helper()
	var ready, done sync.WaitGroup
	ready.Add(workers)
	start := make(chan struct{})
	for worker := range workers {
		n := b.N / workers
		if worker < b.N%workers {
			n++
		}
		done.Go(func() { ready.Done(); <-start; work(worker, n) })
	}
	ready.Wait()
	b.ResetTimer()
	close(start)
	done.Wait()
	b.StopTimer()
}

// Mixed request sizes in pages, weighted toward small buffers.
var benchMix = []struct{ pages, weight int }{
	{1, 35}, {2, 10}, {3, 10}, {4, 10}, {6, 5}, {8, 10}, {16, 10}, {24, 5}, {32, 5},
}

func benchPick(r *rand.Rand) int {
	n := r.IntN(100)
	for _, m := range benchMix {
		if n < m.weight {
			return m.pages
		}
		n -= m.weight
	}
	panic("weights must sum to 100")
}

// CPU parallelism controls the worker count, avoiding a worker/CPU cross-product.
// Hot paths isolate metadata; mixed workloads also measure normal zeroing.
func BenchmarkAllocator(b *testing.B) {
	benchmarkAllocator(b, 0)
}

func benchmarkAllocator(b *testing.B, cursorBudget int) {
	defer buildutil.EnableTestMode(false)()
	makeAllocator := func(b *testing.B, size int64, opts ...Option) *Allocator {
		a := newAllocator(b, size, opts...)
		setBenchmarkCursorBudget(a.arena, cursorBudget)
		return a
	}
	const arenaBytes = 64 << 20
	for _, size := range []int64{4 << 10, 8 << 10, 16 << 10, 32 << 10, 64 << 10, 128 << 10} {
		b.Run(fmt.Sprintf("Memory%dK", size>>10), func(b *testing.B) {
			workers := runtime.GOMAXPROCS(0)
			a := makeAllocator(b, arenaBytes, WithoutZeroing())
			b.ReportAllocs()
			benchmarkWorkers(b, workers, func(_, n int) {
				for range n {
					m, err := a.TryAcquire(size)
					if err != nil {
						b.Error(err)
						return
					}
					m.Unpin()
				}
			})
		})
	}
	for _, zero := range []bool{false, true} {
		b.Run(fmt.Sprintf("BufferMixed/zero%t", zero), func(b *testing.B) {
			workers := runtime.GOMAXPROCS(0)
			opts := []Option{WithWarmup()}
			if !zero {
				opts = append(opts, WithoutZeroing())
			}
			a := makeAllocator(b, arenaBytes, opts...)
			var requested, served, handles atomic.Int64
			live := make([][16]*Buffer, workers)
			for worker := range live {
				for i := range live[worker] {
					live[worker][i] = NewAllocatingBuffer(a)
				}
			}
			b.ReportAllocs()
			benchmarkWorkers(b, workers, func(w, n int) {
				r := rand.New(rand.NewPCG(uint64(w), 3))
				defer func() {
					for _, buffer := range live[w] {
						buffer.Release()
					}
				}()
				var demand, bytes, memories int64
				for range n {
					buffer := live[w][r.IntN(len(live[w]))]
					buffer.Release()
					size := benchPick(r) * 4096
					if err := buffer.Grow(size); err != nil {
						b.Error(err)
						return
					}
					demand += int64(size)
					bytes += int64(buffer.Cap())
					memories += int64(len(buffer.mem))
				}
				served.Add(bytes)
				requested.Add(demand)
				handles.Add(memories)
			})
			b.SetBytes(requested.Load() / int64(b.N))
			b.ReportMetric(float64(requested.Load())/float64(b.N), "requested-B/op")
			b.ReportMetric(float64(served.Load())/float64(b.N), "served-B/op")
			b.ReportMetric(float64(handles.Load())/float64(b.N), "memories/op")
		})
	}
	// Exhaustion scans a full arena, not just one full span.
	b.Run("Exhausted", func(b *testing.B) {
		workers := runtime.GOMAXPROCS(0)
		a := makeAllocator(b, arenaBytes, WithoutZeroing())
		for i := range a.arena.words {
			a.arena.setSpanForTest(i, uint64(fullSpan))
		}
		b.ReportAllocs()
		benchmarkWorkers(b, workers, func(_, n int) {
			for range n {
				if _, err := a.TryAcquire(MaxAllocationSize); err != ErrExhausted {
					b.Errorf("expected exhaustion, got %v", err)
					return
				}
			}
		})
	})
	// A complete fill exposes scan growth as more spans become full.
	b.Run("Fill64M", func(b *testing.B) {
		workers := runtime.GOMAXPROCS(0)
		ar := makeAllocator(b, arenaBytes).arena
		pages := len(ar.words) * spanPages
		var elapsed time.Duration
		b.ReportAllocs()
		for b.Loop() {
			start := time.Now()
			var wg sync.WaitGroup
			for worker := range workers {
				n := pages / workers
				if worker < pages%workers {
					n++
				}
				wg.Go(func() {
					for range n {
						if _, ok := ar.claim(1); !ok {
							b.Error("exhausted")
							return
						}
					}
				})
			}
			wg.Wait()
			elapsed += time.Since(start)
			for i := range ar.words {
				ar.freeSpan(i, 0, spanPages)
			}
		}
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N*pages), "ns/claim")
	})
	// Half the pages are free, but no two free pages are adjacent.
	b.Run("Fragmented", func(b *testing.B) {
		workers := runtime.GOMAXPROCS(0)
		a := makeAllocator(b, arenaBytes, WithoutZeroing())
		word := uint64(refWord(0xaaaaaaaa))
		for i := range a.arena.words {
			a.arena.setSpanForTest(i, word)
		}
		b.ReportAllocs()
		benchmarkWorkers(b, workers, func(_, n int) {
			for range n {
				m, err := a.TryAcquire(MaxAllocationSize)
				if err != nil {
					b.Error(err)
					return
				}
				if len(m.Bytes()) != 4096 {
					b.Errorf("got %d bytes, want one page", len(m.Bytes()))
					m.Unpin()
					return
				}
				m.Unpin()
			}
		})
	})
	// Release on a different goroutine, as when an I/O completion returns memory.
	// Timing includes the channel handoff and draining the outstanding releases.
	b.Run("RemoteRelease4K", func(b *testing.B) {
		workers := runtime.GOMAXPROCS(0)
		a := makeAllocator(b, arenaBytes, WithoutZeroing())
		type worker struct {
			memory chan Memory
			done   chan struct{}
		}
		releasers := make([]worker, workers)
		for i := range releasers {
			w := worker{make(chan Memory, 64), make(chan struct{})}
			releasers[i] = w
			go func() {
				defer close(w.done)
				for m := range w.memory {
					m.Unpin()
				}
			}()
		}
		b.ReportAllocs()
		benchmarkWorkers(b, workers, func(i, n int) {
			w := releasers[i]
			defer func() { close(w.memory); <-w.done }()
			for range n {
				m, err := a.TryAcquire(4096)
				if err != nil {
					b.Error(err)
					return
				}
				w.memory <- m
			}
		})
	})
	// Cover every buddy size, non-power-of-two requests, and growth across spans.
	// Zeroing cost is covered by BufferMixed; these isolate allocation overhead.
	for _, size := range []int{
		4 << 10, 8 << 10, 16 << 10, 24 << 10, 32 << 10, 64 << 10,
		96 << 10, 128 << 10, (128 << 10) + 17, 256 << 10, 1 << 20, 4 << 20,
	} {
		b.Run(fmt.Sprintf("Buffer%dB", size), func(b *testing.B) {
			workers := runtime.GOMAXPROCS(0)
			a := makeAllocator(b, int64(workers)*(8<<20), WithoutZeroing())
			buffers := make([]*Buffer, workers)
			for i := range buffers {
				buffers[i] = NewAllocatingBuffer(a)
				if err := buffers[i].Grow(size); err != nil {
					b.Fatal(err)
				}
				buffers[i].Release()
			}
			var held, handles atomic.Int64
			b.ReportAllocs()
			benchmarkWorkers(b, workers, func(worker, n int) {
				buf := buffers[worker]
				var capacity, memories int64
				defer buf.Release()
				for range n {
					if err := buf.Grow(size); err != nil {
						b.Error(err)
						return
					}
					capacity += int64(buf.Cap())
					memories += int64(len(buf.mem))
					buf.Release()
				}
				held.Add(capacity)
				handles.Add(memories)
			})
			b.ReportMetric(float64(held.Load())/float64(b.N), "held-B/op")
			b.ReportMetric(float64(handles.Load())/float64(b.N), "memories/op")
		})
	}
}
