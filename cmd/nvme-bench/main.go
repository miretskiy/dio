// nvme-bench drives one iosched io_uring scheduler with 128 KiB NVMe commands
// at a fixed queue depth, like fio's io_uring_cmd engine, and prints one table
// row: IOPS, MB/s, completion latency p50/p99/p99.9/p99.99 in µs, and process
// user, system, and total CPU µs and context switches per command. It writes
// the range it is given; use a dedicated scratch range. The binary needs
// CAP_SYS_ADMIN for NVMe passthrough on hosts without a Command Effects log.
//
//	nvme-bench -device /dev/ng1n1 -offset OFFSET -size SIZE -write -qd 16 write-qd16
package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/nvme"
)

func main() {
	device := flag.String("device", "", "NVMe character device")
	offset := flag.Uint64("offset", 0, "range byte offset")
	size := flag.Uint64("size", 0, "range bytes")
	write := flag.Bool("write", false, "write instead of read")
	depth := flag.Int("qd", 16, "commands in flight")
	seconds := flag.Float64("t", 15, "measured seconds")
	budget := flag.Bool("budget", false, "keep the default in-flight budget")
	waiters := flag.Bool("wait", false, "one goroutine per command waiting on its ticket, instead of completion callbacks")
	registered := flag.Bool("registered", false, "register the command buffers with the ring (WithDMABuffers)")
	flag.Parse()
	const bs = 128 << 10
	d, err := nvme.Open(*device, nvme.WithRange(*offset, *size))
	check(err)
	policy := iosched.NewDiskPolicy(iosched.WithoutIOBudget())
	if *budget {
		policy = iosched.NewDiskPolicy()
	}
	region := align.AllocAligned(int64(*depth * bs))
	opts := []iosched.Option{iosched.WithNVMeCommands(d), iosched.WithRingDepth(256), iosched.WithDiskPolicy(policy)}
	if *registered {
		// One entry per command buffer: before Linux 6.15, larger entries
		// make passthrough commands fail with EINVAL.
		var entries [][]byte
		for i := range *depth {
			entries = append(entries, region[i*bs:(i+1)*bs])
		}
		opts = append(opts, iosched.WithDMABuffers(entries...))
	}
	s, err := iosched.NewURingScheduler(opts...)
	check(err)
	blocks := int64(d.Size() / bs)
	op := func(buf []byte) iosched.Op {
		off := rand.Int64N(blocks) * bs
		if *write {
			return iosched.NVMeWriteOp(d, buf, off)
		}
		return iosched.NVMeReadOp(d, buf, off)
	}
	var (
		mu        sync.Mutex
		latencies []time.Duration
		measuring atomic.Bool
		stop      atomic.Bool
		ios       atomic.Int64
		done      sync.WaitGroup
	)
	record := func(start time.Time) {
		if measuring.Load() {
			lat := time.Since(start)
			mu.Lock()
			latencies = append(latencies, lat)
			mu.Unlock()
			ios.Add(1)
		}
	}
	for i := range *depth {
		buf := region[i*bs : (i+1)*bs]
		done.Add(1)
		if *waiters {
			go func() {
				defer done.Done()
				for !stop.Load() {
					start := time.Now()
					t, err := s.Submit(op(buf))
					check(err)
					_, err = t.Wait()
					check(err)
					record(start)
				}
			}()
			continue
		}
		var issue func()
		issue = func() {
			start := time.Now()
			_, err := iosched.SubmitNotify(s, op(buf), func(_ int, err error) {
				check(err)
				record(start)
				if stop.Load() {
					done.Done()
					return
				}
				issue()
			})
			check(err)
		}
		issue()
	}
	time.Sleep(3 * time.Second) // ramp
	var before, after syscall.Rusage
	check(syscall.Getrusage(syscall.RUSAGE_SELF, &before))
	measuring.Store(true)
	time.Sleep(time.Duration(*seconds * float64(time.Second)))
	measuring.Store(false)
	check(syscall.Getrusage(syscall.RUSAGE_SELF, &after))
	stop.Store(true)
	done.Wait()
	check(s.Close())
	n := ios.Load()
	cpu := func(a, b syscall.Timeval) float64 { return float64(b.Nano()-a.Nano()) / 1e3 }
	usr, sys := cpu(before.Utime, after.Utime), cpu(before.Stime, after.Stime)
	slices.Sort(latencies)
	q := func(f float64) float64 { return float64(latencies[int(f*float64(len(latencies)-1))]) / 1e3 }
	fmt.Printf("%-22s %9.0f %8.0f %8.0f %8.0f %8.0f %8.0f %8.2f %8.2f %8.2f %8.2f\n", flag.Arg(0),
		float64(n)/(*seconds), float64(n)*bs/(*seconds)/1e6, q(.5), q(.99), q(.999), q(.9999),
		usr/float64(n), sys/float64(n), (usr+sys)/float64(n),
		float64((after.Nvcsw+after.Nivcsw)-(before.Nvcsw+before.Nivcsw))/float64(n))
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
