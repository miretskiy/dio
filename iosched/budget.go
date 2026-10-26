package iosched

import (
	"errors"
	"sync/atomic"
	"time"

	"golang.org/x/sys/cpu"
)

// Device rates estimate service time; they do not replenish admission capacity.
const (
	DefaultReadBandwidth  int64 = 2_560_000_000
	DefaultReadIOPS       int64 = 500_000
	DefaultWriteBandwidth int64 = 1_220_000_000
	DefaultWriteIOPS      int64 = 83_000
	DefaultLatencyGoal          = 1500 * time.Microsecond
)

// ioCharge holds request totals until the coordinator selects enabled charges.
// Dispatch reserves serviceTime until each I/O completes.
type ioCharge struct {
	ops         int64
	bytes       int64
	serviceTime int64 // modeled device service time in nanoseconds
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

func (d *deviceIO) serviceTime(bytes int64) int64 {
	if d.latency == 0 {
		return 0
	}
	return max(int64(float64(bytes)*1e9/float64(d.bytesPerSecond)), int64(time.Second)/d.iops)
}

// ioBudget is one class's shared accounting and rejection state.
type ioBudget struct {
	outstanding         ioCharge // atomic: coordinator-accounted ops/bytes, dispatched serviceTime
	maxOutstandingOps   int64
	maxOutstandingBytes int64
	lowWatermark        int64 // negative disables timed recovery
	// rejecting closes admission. With timed recovery it lasts until reopenAt,
	// a monotonicNow deadline; Submit reads the clock only while it is set.
	rejecting atomic.Bool
	reopenAt  atomic.Int64
}

// ErrRejected means the whole submission failed admission before enqueueing.
// The caller retains its operands and decides whether to retry.
var ErrRejected = errors.New("iosched: admission limit exceeded")

// DiskPolicy combines device configuration and shared atomic accounting.
// Configure it with NewDiskPolicy, then share its pointer across a device's
// schedulers. Capacity is returned by completion, not replenished by elapsed
// time. Each class's counters sit on their own cache lines: every ring updates them.
type DiskPolicy struct {
	device      deviceConfig
	_           cpu.CacheLinePad
	readBudget  ioBudget
	_           cpu.CacheLinePad
	writeBudget ioBudget
	_           cpu.CacheLinePad
}

// ringInFlight is one coordinator's dispatched, unfinished service time per
// class. Only that coordinator touches it.
type ringInFlight struct {
	reads, writes int64
}

var monotonicBase = time.Now()

func monotonicNow() int64 { return int64(time.Since(monotonicBase)) }

// admit only observes the coordinator-controlled rejection state.
func (p *DiskPolicy) admit(work *submission) bool {
	if work.root.opFlags&opMustAdmit != 0 {
		return true
	}
	if work.reads.ops > 0 && p.readBudget.rejects() {
		return false
	}
	if work.writes.ops > 0 && p.writeBudget.rejects() {
		return false
	}
	return true
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
	p.readBudget.account(&work.reads)
	p.writeBudget.account(&work.writes)
}

// dispatch reserves the shared in-flight allowance for the whole chain. Queued
// work retains its admission charge while it waits in its coordinator's ready queue.
func (p *DiskPolicy) dispatch(work *submission, ring *ringInFlight) bool {
	if !p.readBudget.dispatch(work.reads.serviceTime, int64(p.device.reads.latency), ring.reads) {
		return false
	}
	if !p.writeBudget.dispatch(work.writes.serviceTime, int64(p.device.writes.latency), ring.writes) {
		if work.reads.serviceTime > 0 {
			atomic.AddInt64(&p.readBudget.outstanding.serviceTime, -work.reads.serviceTime)
		}
		return false
	}
	ring.reads += work.reads.serviceTime
	ring.writes += work.writes.serviceTime
	return true
}

// dispatch admits serviceTime if it fits in allowance, or if this ring has
// none of the class in flight. The latter bounds every wait by the ring's own
// completions, which also wake it, and lets an oversized chain run.
func (b *ioBudget) dispatch(serviceTime, allowance, ringInFlight int64) bool {
	if serviceTime == 0 {
		return true
	}
	inFlight := atomic.AddInt64(&b.outstanding.serviceTime, serviceTime)
	if inFlight > allowance && ringInFlight != 0 {
		atomic.AddInt64(&b.outstanding.serviceTime, -serviceTime)
		return false
	}
	return true
}

// completeIO returns only this operation's in-flight allowance. Its admission
// charge remains held until the entire ticket completes, including durable sync.
func (p *DiskPolicy) completeIO(work *submission, op *operation, ring *ringInFlight) {
	switch op.kind() {
	case OpRead, OpReadv:
		if work.reads.serviceTime > 0 {
			elapsed := p.device.reads.serviceTime(int64(opBytes(op)))
			work.reads.serviceTime -= elapsed
			ring.reads -= elapsed
			atomic.AddInt64(&p.readBudget.outstanding.serviceTime, -elapsed)
		}
	case OpWrite, OpWritev:
		if work.writes.serviceTime > 0 {
			elapsed := p.device.writes.serviceTime(int64(opBytes(op)))
			work.writes.serviceTime -= elapsed
			ring.writes -= elapsed
			atomic.AddInt64(&p.writeBudget.outstanding.serviceTime, -elapsed)
		}
	}
}

func (p *DiskPolicy) release(work *submission, ring *ringInFlight) {
	// Forced shutdown can leave issued operations without completions.
	if work.state == workIssued {
		if work.reads.serviceTime > 0 {
			ring.reads -= work.reads.serviceTime
			atomic.AddInt64(&p.readBudget.outstanding.serviceTime, -work.reads.serviceTime)
		}
		if work.writes.serviceTime > 0 {
			ring.writes -= work.writes.serviceTime
			atomic.AddInt64(&p.writeBudget.outstanding.serviceTime, -work.writes.serviceTime)
		}
	}
	p.readBudget.release(work.reads)
	p.writeBudget.release(work.writes)
	p.updateAdmission()
}

func (b *ioBudget) account(charge *ioCharge) {
	if b.maxOutstandingOps == 0 {
		charge.ops = 0
	}
	if b.maxOutstandingBytes == 0 {
		charge.bytes = 0
	}
	if charge.ops > 0 {
		atomic.AddInt64(&b.outstanding.ops, charge.ops)
	}
	if charge.bytes > 0 {
		atomic.AddInt64(&b.outstanding.bytes, charge.bytes)
	}
}

// updateAdmission is called only by coordinators. Counters remain shared across
// rings; the rejection decision never runs in Submit.
func (p *DiskPolicy) updateAdmission() {
	p.readBudget.updateAdmission(&p.device.reads)
	p.writeBudget.updateAdmission(&p.device.writes)
}

func (b *ioBudget) full() bool {
	return (b.maxOutstandingOps > 0 && atomic.LoadInt64(&b.outstanding.ops) >= b.maxOutstandingOps) ||
		(b.maxOutstandingBytes > 0 && atomic.LoadInt64(&b.outstanding.bytes) >= b.maxOutstandingBytes)
}

func (b *ioBudget) updateAdmission(device *deviceIO) {
	if b.maxOutstandingOps == 0 && b.maxOutstandingBytes == 0 {
		return
	}
	if b.lowWatermark >= 0 && b.maxOutstandingBytes > 0 {
		// Timed recovery: close for the estimated drain time. Only an expired
		// pause is renewed; observations during a pause cannot extend it.
		if !b.rejects() && b.full() {
			ops := atomic.LoadInt64(&b.outstanding.ops)
			bytes := atomic.LoadInt64(&b.outstanding.bytes)
			drain := max(1, int64(float64(bytes-b.lowWatermark)*1e9/float64(device.bytesPerSecond)),
				ops*int64(time.Second)/device.iops)
			b.reopenAt.Store(monotonicNow() + drain)
			b.rejecting.Store(true)
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

func (b *ioBudget) release(charge ioCharge) {
	if charge.ops > 0 {
		atomic.AddInt64(&b.outstanding.ops, -charge.ops)
	}
	if charge.bytes > 0 {
		atomic.AddInt64(&b.outstanding.bytes, -charge.bytes)
	}
}

// DiskStats observes coordinator-accounted ops/bytes and dispatched service time
// across all rings. Staged submissions are not yet counted. Disabled limits
// report zero; MustAdmit work is excluded. Fields are read separately.
type DiskStats struct {
	Reads  DiskUsage
	Writes DiskUsage
}

type DiskUsage struct {
	Depth       int64
	Bytes       int64
	ServiceTime time.Duration // modeled service time of dispatched, unfinished I/O
}

func (p *DiskPolicy) Stats() DiskStats {
	return DiskStats{
		Reads:  p.readBudget.snapshot(),
		Writes: p.writeBudget.snapshot(),
	}
}

func (b *ioBudget) snapshot() DiskUsage {
	return DiskUsage{
		Depth:       atomic.LoadInt64(&b.outstanding.ops),
		Bytes:       atomic.LoadInt64(&b.outstanding.bytes),
		ServiceTime: time.Duration(atomic.LoadInt64(&b.outstanding.serviceTime)),
	}
}
