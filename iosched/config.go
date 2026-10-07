package iosched

import (
	"fmt"
	"time"

	"github.com/miretskiy/dio/v2/mempool"
)

const defaultRingDepth uint32 = 256

type schedulerConfig struct {
	ringDepth  uint32
	sqPoll     bool
	vfiles     uint32
	coalescing bool
	dmaPool    *mempool.SlabPool
	dmaPoolSet bool
	budget     ioBudget
}

func makeSchedulerConfig(opts []Option) schedulerConfig {
	c := schedulerConfig{
		ringDepth:  defaultRingDepth,
		coalescing: true,
		budget:     defaultBudget(),
	}
	for _, opt := range opts {
		opt.apply(&c)
	}
	if c.ringDepth == 0 {
		c.ringDepth = defaultRingDepth
	}
	return c
}

// Option configures the io_uring backend used by [NewDefaultScheduler] or
// [NewURingScheduler]. The POSIX fallback ignores these options.
type Option interface {
	apply(*schedulerConfig)
}

type optionFunc func(*schedulerConfig)

func (f optionFunc) apply(c *schedulerConfig) { f(c) }

// WithVFiles registers a sparse virtual-file table of size n on the io_uring
// backend. A non-zero table is the prerequisite for virtual-file ops (VOpenatOp
// and friends); without it they fail. The POSIX backend ignores it (its
// emulated table is unbounded).
func WithVFiles(n uint32) Option {
	return optionFunc(func(c *schedulerConfig) { c.vfiles = n })
}

// WithRingDepth sets the io_uring SQ/CQ depth in entries. Zero (the default)
// uses the backend's default depth. The coordinator keeps one entry for its
// doorbell, so the depth must be at least two, and on a ring of fewer than nine
// entries a linked chain may use at most the depth less one.
func WithRingDepth(n uint32) Option {
	return optionFunc(func(c *schedulerConfig) { c.ringDepth = n })
}

// WithSQPOLL enables IORING_SETUP_SQPOLL, dedicating a kernel thread to poll the
// submission queue.
func WithSQPOLL() Option {
	return optionFunc(func(c *schedulerConfig) { c.sqPoll = true })
}

// WithCoalescing controls whether contiguous same-file writes are merged into
// one writev. Coalescing is enabled by default. The POSIX backend ignores it.
func WithCoalescing(enabled bool) Option {
	return optionFunc(func(c *schedulerConfig) { c.coalescing = enabled })
}

// WithDMASlab registers pool as one fixed buffer before the io_uring
// coordinator starts. The scheduler retains pool until Close returns; callers
// must release all pool slots and close the scheduler before closing pool. The
// POSIX fallback ignores this option.
func WithDMASlab(pool *mempool.SlabPool) Option {
	return optionFunc(func(c *schedulerConfig) {
		c.dmaPool = pool
		c.dmaPoolSet = true
	})
}

// The options below set the io_uring backend's in-flight budget. Reads and
// writes each keep at most the latency goal's worth of device time in flight,
// where an operation costs max(bytes/bandwidth, 1/IOPS) at its class's limits;
// work beyond that waits in the scheduler, so the device queue stays short and
// a read never waits behind a backlog of writes. The defaults (the Default*
// constants) were measured on one disk; scripts/disk-model.sh measures another.
// The POSIX backend ignores them.

// WithReadBandwidth sets the device's read bandwidth in bytes per second.
func WithReadBandwidth(bytesPerSecond int64) Option {
	return optionFunc(func(c *schedulerConfig) { c.budget.limits[classRead].bandwidth = bytesPerSecond })
}

// WithReadIOPS sets the device's read operations per second at small sizes.
func WithReadIOPS(iops int64) Option {
	return optionFunc(func(c *schedulerConfig) { c.budget.limits[classRead].iops = iops })
}

// WithWriteBandwidth sets the device's write bandwidth in bytes per second.
func WithWriteBandwidth(bytesPerSecond int64) Option {
	return optionFunc(func(c *schedulerConfig) { c.budget.limits[classWrite].bandwidth = bytesPerSecond })
}

// WithWriteIOPS sets the device's write operations per second at small sizes.
func WithWriteIOPS(iops int64) Option {
	return optionFunc(func(c *schedulerConfig) { c.budget.limits[classWrite].iops = iops })
}

// WithLatencyGoal sets how much device time each class may keep in flight,
// which is about the queueing latency the device adds. It should be at least
// the in-flight time a class needs to reach its ceiling; below that, the
// budget costs throughput. Default: DefaultLatencyGoal.
func WithLatencyGoal(goal time.Duration) Option {
	return optionFunc(func(c *schedulerConfig) { c.budget.goal = goal })
}

// WithoutIOBudget places work as soon as it fits in the ring, with no
// in-flight budget.
func WithoutIOBudget() Option {
	return optionFunc(func(c *schedulerConfig) { c.budget.goal, c.budget.off = 0, true })
}

func (b ioBudget) validate() error {
	if b.off {
		return nil
	}
	if b.goal <= 0 {
		return fmt.Errorf("iosched: latency goal %v must be positive; use WithoutIOBudget for no budget", b.goal)
	}
	for class, limits := range b.limits {
		if limits.bandwidth <= 0 || limits.iops <= 0 {
			return fmt.Errorf("iosched: %s bandwidth %d and IOPS %d must be positive",
				[]string{"read", "write"}[class], limits.bandwidth, limits.iops)
		}
	}
	return nil
}
