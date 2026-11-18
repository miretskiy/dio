package iosched

import (
	"errors"
	"sync/atomic"
	"time"
)

// Device rates estimate service time; they do not replenish admission capacity.
const (
	DefaultReadBandwidth  int64 = 2_560_000_000
	DefaultReadIOPS       int64 = 500_000
	DefaultWriteBandwidth int64 = 1_220_000_000
	DefaultWriteIOPS      int64 = 83_000
	DefaultLatencyGoal          = 1500 * time.Microsecond
)

// ioCharge is one class's modeled device service time, in nanoseconds, for a
// submission. backlog is held from acceptance to ticket completion when a
// backlog limit is set; inFlight from dispatch to each I/O's completion when a
// latency goal is set. Both start as the request total.
type ioCharge struct {
	backlog  int64
	inFlight int64
}

// deviceConfig holds the configured device rates and service-time allowances.
// It is immutable once the policy is used by a scheduler.
type deviceConfig struct {
	reads, writes deviceIO
}

type deviceIO struct {
	bytesPerSecond int64
	iops           int64
	latency        time.Duration // dispatched service-time cap; zero disables it
}

// serviceTime models one operation as max(bytes/bandwidth, 1/IOPS).
func (d *deviceIO) serviceTime(bytes int64) int64 {
	return max(int64(float64(bytes)*1e9/float64(d.bytesPerSecond)), int64(time.Second)/d.iops)
}

// operationTime charges every physical command used by a logical NVMe transfer.
func (d *deviceIO) operationTime(op *operation) int64 {
	bytes := opBytes(op)
	if op.isNVMe() {
		limit := nvmeCommandLimit(op.nvme)
		if limit > 0 {
			if len(op.allocations) == 0 {
				return d.commandTime(bytes, limit)
			}
			elapsed := d.commandTime(len(op.buf), limit)
			left := bytes - len(op.buf)
			for _, memory := range op.allocations {
				if left == 0 {
					break
				}
				n := min(left, len(memory.Bytes()))
				elapsed += d.commandTime(n, limit)
				left -= n
			}
			return elapsed
		}
	}
	return d.serviceTime(int64(bytes))
}

func (d *deviceIO) commandTime(bytes, limit int) int64 {
	if bytes <= limit {
		return d.serviceTime(int64(bytes))
	}
	elapsed := int64(bytes/limit) * d.serviceTime(int64(limit))
	if tail := bytes % limit; tail != 0 {
		elapsed += d.serviceTime(int64(tail))
	}
	return elapsed
}

// ioBudget is one class's shared accounting and rejection state.
type ioBudget struct {
	backlog  atomic.Int64 // modeled time of accepted, unfinished I/O
	inFlight atomic.Int64 // modeled time of dispatched, unfinished I/O
	limit    int64        // backlog that closes admission; zero disables it
	recovery int64        // timed pause drains to this backlog; negative disables it
	// rejecting closes admission. With timed recovery it lasts until reopenAt,
	// a monotonicNow deadline; Submit reads the clock only while it is set.
	rejecting atomic.Bool
	reopenAt  atomic.Int64
}

// ErrRejected means the whole submission failed admission before enqueueing.
// The caller retains its operands and decides whether to retry.
var ErrRejected = errors.New("iosched: admission limit exceeded")

// RejectedError reports a whole-submission admission rejection. RetryAfter is
// a suggested delay before offering more work, not a reservation or guarantee.
// It uses the recovery deadline when configured; otherwise it estimates the
// service time needed to make room for this submission.
type RejectedError struct {
	RetryAfter time.Duration
}

func (e *RejectedError) Error() string { return ErrRejected.Error() }
func (e *RejectedError) Unwrap() error { return ErrRejected }

// DiskPolicy combines device configuration and shared atomic accounting.
// Configure it with NewDiskPolicy, then share its pointer across a device's
// schedulers. Capacity is returned by completion, not replenished by elapsed time.
type DiskPolicy struct {
	device      deviceConfig
	readBudget  ioBudget
	writeBudget ioBudget
}

// DiskPolicyOption configures a DiskPolicy's device model or admission limits.
type DiskPolicyOption func(*DiskPolicy)

// NewDiskPolicy creates a device model and the admission accounting shared by
// the schedulers it is passed to.
func NewDiskPolicy(opts ...DiskPolicyOption) *DiskPolicy {
	p := &DiskPolicy{
		readBudget:  ioBudget{recovery: -1},
		writeBudget: ioBudget{recovery: -1},
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
// its own I/O, and the cap is exceeded by at most one chain per ring. A ring
// places work in acceptance order, so work behind a waiting request waits too;
// give reads and writes separate schedulers to keep them independent.
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

// WithWriteBacklogLimit closes write admission while the modeled service
// time of accepted, unfinished writes is at least limit. It bounds how long an
// admitted write can queue: about limit at the modeled bandwidth. Each write in
// a chain counts through ticket completion, including Durable sync. Staged work
// can overshoot before a coordinator observes it. Default: disabled.
func WithWriteBacklogLimit(limit time.Duration) DiskPolicyOption {
	return func(p *DiskPolicy) { p.writeBudget.limit = int64(max(0, limit)) }
}

// WithWriteBacklogRecovery makes reaching the write backlog limit close write
// admission for a timed pause, backlog minus target: the modeled time to drain
// to target. Zero targets a full drain; a negative target (the default)
// disables timed recovery, and the circuit reopens as soon as the backlog falls
// below the limit. It requires a write backlog limit.
func WithWriteBacklogRecovery(target time.Duration) DiskPolicyOption {
	return func(p *DiskPolicy) { p.writeBudget.recovery = int64(max(-1, target)) }
}

// WithReadBacklogLimit is the read counterpart of WithWriteBacklogLimit.
func WithReadBacklogLimit(limit time.Duration) DiskPolicyOption {
	return func(p *DiskPolicy) { p.readBudget.limit = int64(max(0, limit)) }
}

// ringInFlight is one coordinator's dispatched, unfinished service time per
// class. Only that coordinator touches it.
type ringInFlight struct {
	reads, writes int64
}

var monotonicBase = time.Now()

func monotonicNow() int64 { return int64(time.Since(monotonicBase)) }

// LatencyGoal reports the configured in-flight service-time limit for opcode.
// Zero means that class is unbudgeted.
func (p *DiskPolicy) LatencyGoal(opcode uint8) time.Duration {
	switch opcode &^ opModeMask {
	case OpRead, OpReadv:
		return p.device.reads.latency
	case OpWrite, OpWritev:
		return p.device.writes.latency
	}
	return 0
}

// CanAdmit reports the current admission state for an I/O opcode. Reads and
// writes use their respective backlog limits; other operations are unrestricted.
// It returns ErrRejected when that class is rejecting work. The check neither
// reserves capacity nor guarantees a later Submit will succeed. Callers can use
// it before acquiring buffers; Submit still checks the complete operation chain.
func (p *DiskPolicy) CanAdmit(opcode uint8) error {
	switch opcode &^ opModeMask {
	case OpRead, OpReadv:
		return p.readBudget.admissionError(p.device.reads.serviceTime(0))
	case OpWrite, OpWritev:
		return p.writeBudget.admissionError(p.device.writes.serviceTime(0))
	}
	return nil
}

// admit only observes the coordinator-controlled rejection state.
func (p *DiskPolicy) admit(work *submission) error {
	if work.root.opFlags&(opMustAdmit|opAllowBacklog) != 0 {
		return nil
	}
	if work.reads.backlog > 0 {
		if err := p.readBudget.admissionError(work.reads.backlog); err != nil {
			return err
		}
	}
	if work.writes.backlog > 0 {
		return p.writeBudget.admissionError(work.writes.backlog)
	}
	return nil
}

func (b *ioBudget) admissionError(charge int64) error {
	if !b.rejecting.Load() {
		return nil
	}
	var delay int64
	if reopenAt := b.reopenAt.Load(); reopenAt != 0 {
		delay = reopenAt - monotonicNow()
		if delay <= 0 {
			return nil
		}
	} else {
		delay = max(1, b.backlog.Load()-b.limit+charge)
	}
	return &RejectedError{RetryAfter: time.Duration(delay)}
}

func (b *ioBudget) rejects() bool {
	if !b.rejecting.Load() {
		return false
	}
	reopenAt := b.reopenAt.Load()
	return reopenAt == 0 || monotonicNow() < reopenAt
}

// account runs when a coordinator accepts staged work. Submit owns no charges.
// MustAdmit work carries no charge, so nothing throttles or counts it.
func (p *DiskPolicy) account(work *submission) {
	if work.root.opFlags&opMustAdmit != 0 {
		work.reads, work.writes = ioCharge{}, ioCharge{}
		return
	}
	if p.device.reads.latency == 0 {
		work.reads.inFlight = 0
	}
	if p.device.writes.latency == 0 {
		work.writes.inFlight = 0
	}
	p.readBudget.account(&work.reads)
	p.writeBudget.account(&work.writes)
}

func (b *ioBudget) account(charge *ioCharge) {
	if b.limit == 0 {
		charge.backlog = 0
	}
	if charge.backlog > 0 {
		b.backlog.Add(charge.backlog)
	}
}

// dispatch reserves the shared in-flight allowance for the whole chain. Queued
// work retains its admission charge while it waits in its coordinator's ready queue.
func (p *DiskPolicy) dispatch(work *submission, ring *ringInFlight) bool {
	if !p.readBudget.dispatch(work.reads.inFlight, int64(p.device.reads.latency), ring.reads) {
		return false
	}
	if !p.writeBudget.dispatch(work.writes.inFlight, int64(p.device.writes.latency), ring.writes) {
		if work.reads.inFlight > 0 {
			p.readBudget.inFlight.Add(-work.reads.inFlight)
		}
		return false
	}
	ring.reads += work.reads.inFlight
	ring.writes += work.writes.inFlight
	return true
}

// dispatch admits serviceTime if it fits in allowance, or if this ring has
// none of the class in flight. The latter bounds every wait by the ring's own
// completions, which also wake it, and lets an oversized chain run.
func (b *ioBudget) dispatch(serviceTime, allowance, ringInFlight int64) bool {
	if serviceTime == 0 {
		return true
	}
	if b.inFlight.Add(serviceTime) > allowance && ringInFlight != 0 {
		b.inFlight.Add(-serviceTime)
		return false
	}
	return true
}

// completeIO returns only this operation's in-flight allowance. Its backlog
// charge remains held until the entire ticket completes, including durable sync.
func (p *DiskPolicy) completeIO(work *submission, op *operation, ring *ringInFlight) {
	switch op.kind() {
	case OpRead, OpReadv:
		if work.reads.inFlight > 0 {
			elapsed := work.reads.inFlight // placeNVMe consumes its op's buffer.
			if len(work.linked) != 0 {
				elapsed = p.device.reads.operationTime(op)
			}
			work.reads.inFlight -= elapsed
			ring.reads -= elapsed
			p.readBudget.inFlight.Add(-elapsed)
		}
	case OpWrite, OpWritev:
		if work.writes.inFlight > 0 {
			elapsed := work.writes.inFlight
			if len(work.linked) != 0 {
				elapsed = p.device.writes.operationTime(op)
			}
			work.writes.inFlight -= elapsed
			ring.writes -= elapsed
			p.writeBudget.inFlight.Add(-elapsed)
		}
	}
}

func (p *DiskPolicy) release(work *submission, ring *ringInFlight) {
	// Forced shutdown can leave issued operations without completions.
	if work.state == workIssued {
		if work.reads.inFlight > 0 {
			ring.reads -= work.reads.inFlight
			p.readBudget.inFlight.Add(-work.reads.inFlight)
		}
		if work.writes.inFlight > 0 {
			ring.writes -= work.writes.inFlight
			p.writeBudget.inFlight.Add(-work.writes.inFlight)
		}
	}
	if work.reads.backlog > 0 {
		p.readBudget.backlog.Add(-work.reads.backlog)
	}
	if work.writes.backlog > 0 {
		p.writeBudget.backlog.Add(-work.writes.backlog)
	}
	p.updateAdmission()
}

// updateAdmission is called only by coordinators. Counters remain shared across
// rings; the rejection decision never runs in Submit.
func (p *DiskPolicy) updateAdmission() {
	p.readBudget.updateAdmission()
	p.writeBudget.updateAdmission()
}

func (b *ioBudget) full() bool { return b.backlog.Load() >= b.limit }

func (b *ioBudget) updateAdmission() {
	if b.limit == 0 {
		return
	}
	if b.recovery >= 0 {
		// Timed recovery: close until the modeled backlog would drain to the
		// recovery level. Only an expired pause is renewed; observations
		// during a pause cannot extend it.
		if !b.rejects() {
			if backlog := b.backlog.Load(); backlog >= b.limit {
				b.reopenAt.Store(monotonicNow() + max(1, backlog-b.recovery))
				b.rejecting.Store(true)
			}
		}
		return
	}
	if b.full() {
		b.rejecting.Store(true)
		// Recheck after publishing: another coordinator may have completed the
		// last request meanwhile. A stale observation must not leave it closed.
		if b.full() {
			return
		}
	}
	b.rejecting.Store(false)
}

// DiskStats observes each class's modeled service time across all rings.
// Staged submissions are not yet counted, and MustAdmit work never is.
// Fields are read separately.
type DiskStats struct {
	Reads  DiskUsage
	Writes DiskUsage
}

type DiskUsage struct {
	// Backlog is the modeled service time of accepted, unfinished I/O,
	// counted only while a backlog limit is set.
	Backlog time.Duration
	// InFlight is the modeled service time of dispatched, unfinished I/O,
	// counted only while a latency goal is set.
	InFlight time.Duration
}

func (p *DiskPolicy) Stats() DiskStats {
	return DiskStats{
		Reads:  p.readBudget.snapshot(),
		Writes: p.writeBudget.snapshot(),
	}
}

func (b *ioBudget) snapshot() DiskUsage {
	return DiskUsage{Backlog: time.Duration(b.backlog.Load()), InFlight: time.Duration(b.inFlight.Load())}
}
