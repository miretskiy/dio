package iosched

// NewDefaultScheduler returns the best available Scheduler for the host.
//
// On Linux with io_uring support, it returns a [URingScheduler]; otherwise (or
// if io_uring setup fails) a [POSIXScheduler]. Options configure the io_uring
// backend — e.g. WithVFiles(n) to register a virtual-file table, the
// prerequisite for virtual-file ops. The POSIX backend emulates virtual files
// with an unbounded userspace table and ignores these knobs, so virtual-file
// consumers get a working scheduler on either host.
//
// Callers that want full, explicit control over the io_uring backend (and to
// require it) construct NewURingScheduler(URingConfig{...}) directly.
func NewDefaultScheduler(opts ...Option) Scheduler {
	var c URingConfig
	for _, o := range opts {
		o(&c)
	}
	if IOUringAvailable {
		if s, err := NewURingScheduler(c); err == nil {
			return s
		}
	}
	return NewPOSIXScheduler()
}

// Option configures the Scheduler built by NewDefaultScheduler. The knobs are
// the io_uring backend's; the POSIX backend ignores them.
type Option func(*URingConfig)

// WithVFiles registers a sparse virtual-file table of size n on the io_uring
// backend. A non-zero table is the prerequisite for virtual-file ops (VOpenatOp
// and friends); without it they fail. The POSIX backend ignores it (its
// emulated table is unbounded).
func WithVFiles(n uint32) Option { return func(c *URingConfig) { c.VFiles = n } }

// WithRingDepth sets the io_uring SQ/CQ depth in entries. Zero (the default)
// uses the backend's default depth.
func WithRingDepth(n uint32) Option { return func(c *URingConfig) { c.RingDepth = n } }

// WithSQPOLL enables IORING_SETUP_SQPOLL, dedicating a kernel thread to poll the
// submission queue.
func WithSQPOLL() Option { return func(c *URingConfig) { c.SQPOLL = true } }

// WithEnableCoalesce toggles write coalescing (contiguous same-file plain writes
// in a batch merged into one writev). It is enabled by default; pass false to
// turn it off. The POSIX backend ignores it.
func WithEnableCoalesce(enable bool) Option {
	return func(c *URingConfig) { c.DisableCoalescing = !enable }
}
