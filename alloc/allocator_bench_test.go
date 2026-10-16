package alloc

import (
	"context"
	"flag"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"runtime/metrics"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var benchmarkSlabBytes = flag.Int64("alloc-slab-bytes", 0, "Total capacity in bytes for opt-in large allocator benchmarks")

// No instrumentation is added to production allocation. This metric includes
// runtime locks and sums goroutine wait time; use profiles for attribution.
func mutexWaitSeconds() float64 {
	samples := []metrics.Sample{{Name: "/sync/mutex/wait/total:seconds"}}
	metrics.Read(samples)
	return samples[0].Value.Float64()
}

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
	before := mutexWaitSeconds()
	b.ResetTimer()
	close(start)
	done.Wait()
	b.StopTimer()
	b.ReportMetric((mutexWaitSeconds()-before)*1e9/float64(b.N), "mutex-wait-ns/op")
}

// The atomic reference supports TryAcquire only; production implementations
// also satisfy Allocator and participate in the blocking and latency cases.
type benchmarkAllocator interface {
	TryAcquire(size int64) (*Buffer, error)
	Close()
}

type allocatorWorkload struct {
	name                         string
	slabSize, slabs, chunk, free int
}

func (w allocatorWorkload) capacity() int { return w.slabSize / w.chunk * w.slabs }

type allocatorVariant struct {
	name string
	new  func(allocatorWorkload) (benchmarkAllocator, error)
}

func allocatorVariants() []allocatorVariant {
	return []allocatorVariant{
		{"extent", func(w allocatorWorkload) (benchmarkAllocator, error) {
			// Match object size and capacity: each object is one whole extent.
			return NewExtentAllocator(int64(w.chunk), w.capacity())
		}},
		{"slab", func(w allocatorWorkload) (benchmarkAllocator, error) {
			return NewSlabAllocator(int64(w.slabSize), WithChunkSize(int64(w.chunk)))
		}},
		{"atomic", func(w allocatorWorkload) (benchmarkAllocator, error) {
			return newAtomicBitmapReference(w.slabSize, w.chunk), nil
		}},
	}
}

// BenchmarkAllocator is the single entry point for allocator measurements.
// Parameters are operation, implementation, layout/occupancy, and worker count.
// Unsupported combinations (e.g. multiple slabs for SlabAllocator) are omitted.
func BenchmarkAllocator(b *testing.B) {
	if size := *benchmarkSlabBytes; size > 0 {
		b.Run(fmt.Sprintf("SlabScale/bytes%d", size), func(b *testing.B) {
			benchmarkAllocatorScale(b, func() (benchmarkAllocator, error) {
				return NewSlabAllocator(size, WithWarmup())
			}, int(size/4096))
		})
		for _, warmup := range []bool{false, true} {
			name := "cold"
			var opts []Option
			if warmup {
				name, opts = "warm", []Option{WithWarmup()}
			}
			newAllocator := func() (benchmarkAllocator, error) { return NewSlabAllocator(size, opts...) }
			b.Run(fmt.Sprintf("SlabInit/bytes%d/%s", size, name), func(b *testing.B) {
				benchmarkAllocatorInit(b, newAllocator, 0)
			})
			b.Run(fmt.Sprintf("SlabGrow/bytes%d/%s", size, name), func(b *testing.B) {
				benchmarkAllocatorInit(b, newAllocator, int(size/4096))
			})
		}
	}
	for _, n := range []int{1, 4, 64, 65, 128} {
		for _, workers := range []int{1, 64} {
			b.Run(fmt.Sprintf("Chunks/n%d/workers%d", n, workers), func(b *testing.B) {
				p, err := NewSlabAllocator(2<<20, WithWarmup())
				if err != nil {
					b.Fatal(err)
				}
				benchmarkAllocatorOperations(b, p, int64(n*4096), 0, workers, "TryAcquire")
			})
			b.Run(fmt.Sprintf("ExtentChunks/n%d/workers%d", n, workers), func(b *testing.B) {
				p, err := NewExtentAllocator(4096, max(512, 2*n*workers), WithWarmup())
				if err != nil {
					b.Fatal(err)
				}
				benchmarkAllocatorOperations(b, p, int64(n*4096), 0, workers, "TryAcquire")
			})
		}
	}
	for _, n := range []int{1, 4, 16, 64, 65, 128} {
		for _, workers := range []int{1, 64} {
			for _, mode := range []string{"batch", "singles"} {
				b.Run(fmt.Sprintf("Batch/n%d/workers%d/%s", n, workers, mode), func(b *testing.B) {
					benchmarkAllocatorBatch(b, n, workers, mode)
				})
			}
		}
	}
	for _, workers := range []int{1, 64} {
		for _, path := range []string{"single", "general"} {
			b.Run(fmt.Sprintf("SinglePath/workers%d/%s", workers, path), func(b *testing.B) {
				benchmarkSlabSinglePath(b, workers, path)
			})
		}
	}
	variants := allocatorVariants()
	for _, mode := range []struct {
		name      string
		workloads []allocatorWorkload
		workers   []int
	}{
		{"TryAcquire", []allocatorWorkload{
			{"one_open", 2 << 20, 1, 4096, 512},
			{"one_free8", 2 << 20, 1, 4096, 8},
			{"whole_extents", 4096, 64, 4096, 64},
		}, []int{1, 16, 64}},
		{"Acquire", []allocatorWorkload{
			{"chunks1", 4096, 1, 4096, 1},
			{"chunks8", 8 * 4096, 1, 4096, 8},
		}, []int{16, 64}},
		{"Latency", []allocatorWorkload{
			{"chunks8", 8 * 4096, 1, 4096, 8},
			{"chunks512", 2 << 20, 1, 4096, 512},
		}, []int{64}},
	} {
		for _, variant := range variants {
			if variant.name == "atomic" && mode.name != "TryAcquire" {
				continue
			}
			for _, workload := range mode.workloads {
				if workload.slabs > 1 && (variant.name == "slab" || variant.name == "atomic") {
					continue
				}
				for _, workers := range mode.workers {
					b.Run(fmt.Sprintf("%s/%s/%s/workers%d", mode.name, variant.name, workload.name, workers), func(b *testing.B) {
						p, err := variant.new(workload)
						if err != nil {
							b.Fatal(err)
						}
						benchmarkAllocatorOperations(b, p, int64(workload.chunk), workload.capacity()-workload.free, workers, mode.name)
					})
				}
			}
		}
	}
	for _, variant := range variants {
		if variant.name == "atomic" {
			continue
		}
		for _, size := range []int{256 << 10, 2 << 20} {
			for _, count := range []int{1, 4} {
				if variant.name == "slab" && count != 1 {
					continue
				}
				b.Run(fmt.Sprintf("Cold/%s/bytes%d/extents%d", variant.name, size, count), func(b *testing.B) {
					benchmarkAllocatorCold(b, variant, allocatorWorkload{slabSize: size, slabs: count, chunk: size, free: count})
				})
			}
		}
	}
}

// One operation acquires N chunks, touches each, then releases all of them.
// Keep capacity above the largest combined working set (64 workers * 128
// chunks) so both modes measure successful allocations without exhaustion.
func benchmarkAllocatorBatch(b *testing.B, n, workers int, mode string) {
	p, err := NewSlabAllocator(64<<20, WithWarmup())
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	requests, size := n, int64(4096)
	if mode == "batch" {
		requests, size = 1, int64(n*4096)
	}
	held := make([][]*Buffer, workers)
	for i := range held {
		held[i] = make([]*Buffer, requests)
	}
	b.ReportAllocs()
	benchmarkWorkers(b, workers, func(worker, iterations int) {
		buffers := held[worker]
		for range iterations {
			for i := range buffers {
				buffer, err := p.TryAcquire(size)
				if err != nil {
					b.Error(err)
					for _, held := range buffers[:i] {
						held.Unpin()
					}
					return
				}
				buffers[i] = buffer
			}
			for _, buffer := range buffers {
				for _, chunk := range buffer.Chunks() {
					chunk[0] = byte(worker)
				}
			}
			for _, buffer := range buffers {
				buffer.Unpin()
			}
		}
	})
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/chunk")
}

// Compare the two bitmap paths directly, with the same random starting shard
// and Buffer lifecycle. This bypasses the public API's n == 1 selection.
func benchmarkSlabSinglePath(b *testing.B, workers int, path string) {
	p, err := NewSlabAllocator(64<<20, WithWarmup())
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	acquire := p.slab.tryAcquire
	if path == "general" {
		acquire = func(start int) *Buffer { return p.slab.tryAcquireN(start, 1) }
	}
	b.ReportAllocs()
	benchmarkWorkers(b, workers, func(worker, iterations int) {
		for range iterations {
			buffer := acquire(rand.IntN(len(p.slab.shards)))
			if buffer == nil {
				b.Error("unexpected exhaustion")
				return
			}
			buffer.Chunks()[0][0] = byte(worker)
			buffer.Unpin()
		}
	})
}

func benchmarkAllocatorOperations(b *testing.B, p benchmarkAllocator, size int64, reserve, workers int, mode string) {
	b.Helper()
	defer p.Close()
	held := make([]*Buffer, reserve)
	for i := range held {
		buf, err := p.TryAcquire(size)
		if err != nil {
			b.Fatal(err)
		}
		held[i] = buf
	}
	defer func() {
		for _, buf := range held {
			buf.Unpin()
		}
	}()
	benchmarkAllocatorCycles(b, p, size, workers, mode)
}

// Share one mapping and occupancy setup across calibrated benchmark runs.
func benchmarkAllocatorScale(b *testing.B, newAllocator func() (benchmarkAllocator, error), capacity int) {
	p, err := newAllocator()
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	for _, occupancy := range []struct {
		name string
		held int
	}{
		{"open", 0},
		{"half", capacity / 2},
		{"99pct", capacity * 99 / 100},
		{"free8", capacity - 8},
		{"full", capacity},
	} {
		b.Run(occupancy.name, func(b *testing.B) {
			held := make([]*Buffer, occupancy.held)
			for i := range held {
				held[i], err = p.TryAcquire(4096)
				if err != nil {
					b.Fatal(err)
				}
			}
			defer func() {
				for _, buffer := range held {
					buffer.Unpin()
				}
			}()
			for _, workers := range []int{1, 64} {
				b.Run(fmt.Sprintf("workers%d", workers), func(b *testing.B) {
					benchmarkAllocatorCycles(b, p, 4096, workers, "TryAcquire")
				})
			}
		})
	}
}

// Keep construction and the first (possibly mapping) acquisition distinct.
// A nonzero fill measures a burst that borrows the entire configured capacity.
func benchmarkAllocatorInit(b *testing.B, newAllocator func() (benchmarkAllocator, error), fill int) {
	b.ReportAllocs()
	var initTime, firstTime, fillTime, closeTime time.Duration
	for b.Loop() {
		start := time.Now()
		p, err := newAllocator()
		if err != nil {
			b.Fatal(err)
		}
		initTime += time.Since(start)
		start = time.Now()
		first, err := p.TryAcquire(4096)
		if err != nil {
			b.Fatal(err)
		}
		firstTime += time.Since(start)
		start = time.Now()
		held := make([]*Buffer, max(1, fill))
		held[0] = first
		for i := 1; i < len(held); i++ {
			held[i], err = p.TryAcquire(4096)
			if err != nil {
				b.Fatal(err)
			}
		}
		fillTime += time.Since(start)
		for _, buffer := range held {
			buffer.Unpin()
		}
		start = time.Now()
		p.Close()
		closeTime += time.Since(start)
	}
	for _, metric := range []struct {
		name     string
		duration time.Duration
	}{
		{"init-ns/op", initTime},
		{"first-acquire-ns/op", firstTime},
		{"close-ns/op", closeTime},
	} {
		b.ReportMetric(float64(metric.duration.Nanoseconds())/float64(b.N), metric.name)
	}
	if fill > 0 {
		b.ReportMetric(float64(fillTime.Nanoseconds())/float64(b.N), "fill-ns/op")
	}
}

func benchmarkAllocatorCycles(b *testing.B, p benchmarkAllocator, size int64, workers int, mode string) {
	b.Helper()
	acquire := p.TryAcquire
	if mode != "TryAcquire" {
		a := p.(Allocator)
		acquire = func(size int64) (*Buffer, error) { return a.Acquire(context.Background(), size) }
	}
	exhausted := make([]int, workers)
	var samples [][]int64
	if mode == "Latency" {
		samples = make([][]int64, workers)
		for i := range samples {
			samples[i] = make([]int64, 0, 2049)
		}
	}
	b.ReportAllocs()
	benchmarkWorkers(b, workers, func(worker, n int) {
		failures := 0
		stride := max(1, (n+2047)/2048)
		for i := range n {
			var start time.Time
			if samples != nil && i%stride == 0 {
				start = time.Now()
			}
			buf, err := acquire(size)
			if !start.IsZero() {
				samples[worker] = append(samples[worker], time.Since(start).Nanoseconds())
			}
			if err == ErrExhausted {
				failures++
				continue
			}
			if err != nil {
				b.Error(err)
				return
			}
			for _, data := range buf.Chunks() {
				data[0] = byte(worker)
			}
			buf.Unpin()
		}
		exhausted[worker] = failures
	})
	failures := 0
	for _, n := range exhausted {
		failures += n
	}
	b.ReportMetric(float64(failures)/float64(b.N), "exhausted/op")
	if successes := b.N - failures; successes > 0 {
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(successes), "ns/success")
	}
	if samples != nil {
		var all []int64
		for _, worker := range samples {
			all = append(all, worker...)
		}
		slices.Sort(all)
		for _, percentile := range []int{50, 95, 99} {
			b.ReportMetric(float64(all[(len(all)-1)*percentile/100]), fmt.Sprintf("acquire-p%d-ns", percentile))
		}
	}
}

func benchmarkAllocatorCold(b *testing.B, variant allocatorVariant, w allocatorWorkload) {
	b.ReportAllocs()
	// Time construction, mapping, borrowing all backing
	// allocations, release, and shutdown. Mmap bytes are not Go heap bytes.
	for b.Loop() {
		p, err := variant.new(w)
		if err != nil {
			b.Fatal(err)
		}
		var held [4]*Buffer
		for i := range w.slabs {
			held[i], err = p.TryAcquire(int64(w.chunk))
			if err != nil {
				b.Fatal(err)
			}
		}
		for _, buf := range held[:w.slabs] {
			buf.Unpin()
		}
		p.Close()
	}
}

// Benchmark reference: the pre-unification SlabPool's padded, atomic bitmap
// algorithm. It only supports one eagerly mapped slab and TryAcquire. Keep it
// in tests as a comparison for the mutex-based implementations.
// Buffer ownership uses the same release closures as the current allocators.
type atomicReferenceShard struct {
	mask atomic.Uint64
	_    [56]byte
}

type atomicBitmapReference struct {
	backing   *Buffer
	chunkSize int
	shards    []atomicReferenceShard
}

func newAtomicBitmapReference(size, chunkSize int) *atomicBitmapReference {
	chunks := size / chunkSize
	p := &atomicBitmapReference{backing: NewBuffer(int64(size)), chunkSize: chunkSize,
		shards: make([]atomicReferenceShard, (chunks+63)/64)}
	if tail := chunks % 64; tail != 0 {
		p.shards[len(p.shards)-1].mask.Store(^uint64(0) << tail)
	}
	return p
}

func (p *atomicBitmapReference) TryAcquire(int64) (*Buffer, error) {
	n := len(p.shards)
	start := rand.IntN(n)
	for j := range n {
		i := (start + j) % n
		for {
			mask := p.shards[i].mask.Load()
			if mask == ^uint64(0) {
				break
			}
			bit := bits.TrailingZeros64(^mask)
			claimMask := uint64(1) << bit
			if p.shards[i].mask.CompareAndSwap(mask, mask|claimMask) {
				slot := i*64 + bit
				offset := slot * p.chunkSize
				data := p.backing.Chunks()[0][offset : offset+p.chunkSize : offset+p.chunkSize]
				return newBuffer(data, func() { p.release(slot) }), nil
			}
		}
	}
	return nil, ErrExhausted
}

func (p *atomicBitmapReference) release(slot int) {
	shard := &p.shards[slot/64]
	bit := uint64(1) << (slot % 64)
	shard.mask.And(^bit)
}

func (p *atomicBitmapReference) Close() { p.backing.Unpin() }
