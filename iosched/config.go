package iosched

import (
	"slices"
	"time"

	"github.com/miretskiy/dio/v2/alloc"
)

const defaultRingDepth uint32 = 256

// doorbellSlots is the ring entry the io_uring coordinator keeps for its doorbell read.
const doorbellSlots = 1

// minRingDepth holds the longest chain beside the coordinator's doorbell entry.
const minRingDepth = maxChainLength + doorbellSlots

type schedulerConfig struct {
	coordinatorCPU int // negative leaves affinity unchanged
	ringDepth      uint32
	sqPoll         bool
	vfiles         uint32
	coalescing     bool
	dmaBuffers     []*alloc.Buffer
	disk           *DiskPolicy
}

func makeSchedulerConfig(opts []Option) schedulerConfig {
	c := schedulerConfig{
		coordinatorCPU: -1,
		ringDepth:      defaultRingDepth,
		coalescing:     true,
	}
	for _, opt := range opts {
		opt.apply(&c)
	}
	if c.ringDepth == 0 {
		c.ringDepth = defaultRingDepth
	}
	c.ringDepth = max(c.ringDepth, minRingDepth)
	if c.disk == nil {
		c.disk = NewDiskPolicy()
	}
	return c
}

// Option configures a scheduler. Device rates and admission limits belong to
// the DiskPolicy passed with WithDiskPolicy. Out-of-range values select the
// default or disable the feature; options never fail. The POSIX fallback honors
// only WithVFiles.
type Option interface {
	apply(*schedulerConfig)
}

type optionFunc func(*schedulerConfig)

func (f optionFunc) apply(c *schedulerConfig) { f(c) }

// WithDiskPolicy sets the device model and admission limits. Share one policy
// among the schedulers of a device so they account against the same limits.
// Without it, or with nil, each scheduler uses its own NewDiskPolicy().
func WithDiskPolicy(policy *DiskPolicy) Option {
	return optionFunc(func(c *schedulerConfig) { c.disk = policy })
}

// WithVFiles registers a sparse virtual-file table of size n on the io_uring
// backend. A non-zero table is the prerequisite for virtual-file ops (VOpenatOp
// and friends); without it they fail with EOPNOTSUPP. The POSIX backend
// emulates the table in userspace.
func WithVFiles(n uint32) Option {
	return optionFunc(func(c *schedulerConfig) { c.vfiles = n })
}

// WithCoordinatorCPU requests best-effort affinity for the io_uring coordinator.
// A negative CPU (the default) leaves affinity unchanged. Affinity failures are logged; I/O
// continues on the dedicated OS thread without the requested affinity.
// This pins the userspace coordinator, not kernel workers or device IRQs.
// The POSIX backend ignores this option.
func WithCoordinatorCPU(cpu int) Option {
	return optionFunc(func(c *schedulerConfig) { c.coordinatorCPU = cpu })
}

// WithRingDepth sets the io_uring SQ/CQ depth in entries. Zero (the default)
// uses the backend's default depth. Smaller depths are raised to nine: the
// longest chain, eight operations, plus the coordinator's doorbell entry.
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

// WithDMABuffers registers each supplied single-chunk Buffer as one fixed buffer
// before the io_uring coordinator starts. The caller obtains the buffers and
// must keep a reference to each until the scheduler has closed successfully,
// including between I/O operations. The scheduler does not pin, unpin, or acquire buffers.
//
// The buffer list is copied. Repeated options replace the list; an empty list
// disables registration. NewURingScheduler returns an error for multi-chunk buffers.
// The POSIX fallback ignores this option.
func WithDMABuffers(buffers ...*alloc.Buffer) Option {
	buffers = slices.Clone(buffers)
	return optionFunc(func(c *schedulerConfig) {
		c.dmaBuffers = buffers
	})
}

// DiskPolicyOption configures a DiskPolicy's device model or admission limits.
type DiskPolicyOption func(*DiskPolicy)

// NewDiskPolicy creates a device model and the admission accounting shared by
// the schedulers it is passed to.
func NewDiskPolicy(opts ...DiskPolicyOption) *DiskPolicy {
	p := &DiskPolicy{
		readBudget:  ioBudget{lowWatermark: -1},
		writeBudget: ioBudget{lowWatermark: -1},
		device: deviceConfig{
			reads:  deviceIO{bytesPerSecond: DefaultReadBandwidth, iops: DefaultReadIOPS, latency: DefaultLatencyGoal},
			writes: deviceIO{bytesPerSecond: DefaultWriteBandwidth, iops: DefaultWriteIOPS, latency: DefaultLatencyGoal},
		},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// The options below set the device model and admission limits. Service time
// is computed per data operation as max(bytes/bytesPerSecond, 1/iops), then
// summed for linked chains. Coordinators decide dispatch and rejection; Submit
// only observes the rejection state and never queues a rejected request.
// A non-positive rate keeps its default; a non-positive limit or latency goal
// disables it. MustAdmit submissions bypass all of them. POSIX ignores them.

// WithReadBandwidth sets the device's read bandwidth in bytes per second.
func WithReadBandwidth(bytesPerSecond int64) DiskPolicyOption {
	return func(p *DiskPolicy) { setRate(&p.device.reads.bytesPerSecond, bytesPerSecond) }
}

// WithReadIOPS sets the device's read operations per second at small sizes.
func WithReadIOPS(iops int64) DiskPolicyOption {
	return func(p *DiskPolicy) { setRate(&p.device.reads.iops, iops) }
}

// WithWriteBandwidth sets the device's write bandwidth in bytes per second.
func WithWriteBandwidth(bytesPerSecond int64) DiskPolicyOption {
	return func(p *DiskPolicy) { setRate(&p.device.writes.bytesPerSecond, bytesPerSecond) }
}

// WithWriteIOPS sets the device's write operations per second at small sizes.
func WithWriteIOPS(iops int64) DiskPolicyOption {
	return func(p *DiskPolicy) { setRate(&p.device.writes.iops, iops) }
}

func setRate(rate *int64, value int64) {
	if value > 0 {
		*rate = value
	}
}

// WithLatencyGoal caps the modeled service time of dispatched reads and of
// dispatched writes, each class separately, across all rings sharing the
// policy. A coordinator queues work that does not fit, except that it always
// dispatches when it has none of that class in flight itself. A blocked ring
// therefore always has its own completion to wake it, its wait is bounded by
// its own I/O, and the cap is exceeded by at most one chain per ring.
// Default: DefaultLatencyGoal.
func WithLatencyGoal(goal time.Duration) DiskPolicyOption {
	return func(p *DiskPolicy) {
		p.device.reads.latency = max(0, goal)
		p.device.writes.latency = max(0, goal)
	}
}

// WithoutReadIOBudget admits reads without charging the I/O budget. Writes
// retain their configured budget. Ring capacity and operation ordering still apply.
func WithoutReadIOBudget() DiskPolicyOption {
	return func(p *DiskPolicy) { p.device.reads.latency = 0 }
}

// WithoutIOBudget disables in-flight service-time limits.
func WithoutIOBudget() DiskPolicyOption {
	return WithLatencyGoal(0)
}

// WithRejectWriteQueueDepth closes write admission when coordinators observe n
// outstanding writes. Each write in a chain counts through ticket completion,
// including Durable sync. Some staged work can overshoot. Zero disables it.
func WithRejectWriteQueueDepth(n int64) DiskPolicyOption {
	return func(p *DiskPolicy) { p.writeBudget.maxOutstandingOps = max(0, n) }
}

// WithRejectWriteBacklog closes write admission when coordinators observe this
// many outstanding buffer bytes. Staged work may overshoot before the circuit
// closes. Zero disables this threshold. POSIX ignores it.
func WithRejectWriteBacklog(bytes int64) DiskPolicyOption {
	return func(p *DiskPolicy) { p.writeBudget.maxOutstandingBytes = max(0, bytes) }
}

// WithWriteBacklogLowWatermark enables a timed pause at the write backlog
// threshold. The coordinator estimates how long draining to this watermark takes
// and closes write admission for that duration. Zero targets a full drain; a
// negative value (the default) disables timed recovery. It requires a positive
// write backlog threshold. Without timed recovery the circuit reopens when
// counts fall below their limits. POSIX ignores this option.
func WithWriteBacklogLowWatermark(bytes int64) DiskPolicyOption {
	return func(p *DiskPolicy) { p.writeBudget.lowWatermark = max(-1, bytes) }
}

// WithRejectReadQueueDepth is the read counterpart of WithRejectWriteQueueDepth.
func WithRejectReadQueueDepth(n int64) DiskPolicyOption {
	return func(p *DiskPolicy) { p.readBudget.maxOutstandingOps = max(0, n) }
}

// WithRejectReadBacklog is the read counterpart of WithRejectWriteBacklog.
func WithRejectReadBacklog(bytes int64) DiskPolicyOption {
	return func(p *DiskPolicy) { p.readBudget.maxOutstandingBytes = max(0, bytes) }
}
