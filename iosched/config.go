package iosched

const defaultRingDepth uint32 = 256

type schedulerConfig struct {
	ringDepth  uint32
	sqPoll     bool
	vfiles     uint32
	coalescing bool
}

func makeSchedulerConfig(opts []Option) schedulerConfig {
	c := schedulerConfig{
		ringDepth:  defaultRingDepth,
		coalescing: true,
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
// uses the backend's default depth.
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
