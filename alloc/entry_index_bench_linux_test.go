package alloc

import (
	"fmt"
	"runtime"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

// This is a distribution diagnostic, not an allocator throughput benchmark.
// The marker tracks movement of the caller's stack; entryIndex is called at
// one fixed site. getcpu observes the Linux CPU, not a Go scheduler P.
func BenchmarkEntryIndex(b *testing.B) {
	const spans = 512
	for _, mode := range []string{"steady", "yield", "stack-growth"} {
		for _, multiple := range []int{1, 8} {
			workers := multiple * runtime.GOMAXPROCS(0)
			b.Run(fmt.Sprintf("%s/workers%d", mode, workers), func(b *testing.B) {
				type sample struct {
					n, first, indexChanges, stackMoves, cpuMoves, stableMigrations int
				}
				results := make([]sample, workers)
				benchmarkWorkers(b, workers, func(worker, n int) {
					var result sample
					var marker byte
					var previousAddress uintptr
					var previousIndex int
					var previousCPU uint32
					for i := range n {
						if mode == "yield" && i%256 == 0 {
							runtime.Gosched()
						}
						if mode == "stack-growth" {
							if i == n/3 {
								marker = entryIndexGrowStack(32)
							}
							if i == 2*n/3 {
								marker = entryIndexGrowStack(128)
							}
						}
						var cpu uint32
						_, _, errno := unix.RawSyscall(unix.SYS_GETCPU, uintptr(unsafe.Pointer(&cpu)), 0, 0)
						if errno != 0 {
							b.Error(errno)
							return
						}
						index := entryIndex(spans)
						address := uintptr(unsafe.Pointer(&marker))
						if i == 0 {
							result.first = index
						} else {
							if index != previousIndex {
								result.indexChanges++
							}
							if address != previousAddress {
								result.stackMoves++
							}
							if cpu != previousCPU {
								result.cpuMoves++
								if index == previousIndex {
									result.stableMigrations++
								}
							}
						}
						previousAddress, previousIndex, previousCPU = address, index, cpu
						result.n++
					}
					runtime.KeepAlive(&marker)
					results[worker] = result
				})
				var starts [spans]int
				var total sample
				active, pairs, overlaps := 0, 0, 0
				for i, s := range results {
					if s.n == 0 {
						continue
					}
					active++
					starts[s.first]++
					total.n += s.n - 1
					total.indexChanges += s.indexChanges
					total.stackMoves += s.stackMoves
					total.cpuMoves += s.cpuMoves
					total.stableMigrations += s.stableMigrations
					for _, other := range results[:i] {
						if other.n == 0 {
							continue
						}
						pairs++
						distance := (s.first - other.first + spans) % spans
						// Two 4 MiB requests each scan 32 consecutive spans.
						if distance < 32 || distance > spans-32 {
							overlaps++
						}
					}
				}
				unique, peak := 0, 0
				for _, count := range starts {
					if count > 0 {
						unique++
					}
					peak = max(peak, count)
				}
				b.ReportMetric(float64(unique), "unique-starts")
				b.ReportMetric(float64(peak), "max-workers/start")
				b.ReportMetric(100*float64(total.n-total.indexChanges)/float64(max(1, total.n)), "same-index-%")
				b.ReportMetric(float64(total.stackMoves)/float64(max(1, active)), "stack-moves/worker")
				b.ReportMetric(float64(total.indexChanges)/float64(max(1, active)), "index-changes/worker")
				b.ReportMetric(float64(total.cpuMoves)/float64(max(1, active)), "cpu-moves/worker")
				b.ReportMetric(100*float64(total.stableMigrations)/float64(max(1, total.cpuMoves)), "migrations-same-index-%")
				b.ReportMetric(100*float64(overlaps)/float64(max(1, pairs)), "overlap-pairs-%")
			})
		}
	}
}

//go:noinline
func entryIndexGrowStack(depth int) byte {
	var frame [2048]byte
	frame[0] = byte(depth)
	if depth > 0 {
		frame[1] = entryIndexGrowStack(depth - 1)
	}
	runtime.KeepAlive(&frame)
	return frame[0] ^ frame[1]
}
