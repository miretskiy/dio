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

// DiskPolicy combines device configuration and shared atomic accounting.
// Configure it with NewDiskPolicy, then share its pointer across a device's
// schedulers. Capacity is returned by completion, not replenished by elapsed time.
type DiskPolicy struct {
	device      deviceConfig
	readBudget  ioBudget
	writeBudget ioBudget
}

// ringInFlight is one coordinator's dispatched, unfinished service time per
// class. Only that coordinator touches it.
type ringInFlight struct {
	reads, writes int64
}

var monotonicBase = time.Now()

func monotonicNow() int64 { return int64(time.Since(monotonicBase)) }

// CanAdmit reports the current admission state for an I/O opcode. Reads and
// writes use their respective backlog limits; other operations are unrestricted.
// It returns ErrRejected when that class is rejecting work. The check neither
// reserves capacity nor guarantees a later Submit will succeed. Callers can use
// it before acquiring buffers; Submit still checks the complete operation chain.
func (p *DiskPolicy) CanAdmit(opcode uint8) error {
	switch opcode &^ (opVirtual | opFixed) {
	case OpRead, OpReadv:
		if p.readBudget.rejects() {
			return ErrRejected
		}
	case OpWrite, OpWritev:
		if p.writeBudget.rejects() {
			return ErrRejected
		}
	}
	return nil
}

// admit only observes the coordinator-controlled rejection state.
func (p *DiskPolicy) admit(work *submission) bool {
	if work.root.opFlags&opMustAdmit != 0 {
		return true
	}
	if work.reads.backlog > 0 && p.CanAdmit(OpRead) != nil {
		return false
	}
	if work.writes.backlog > 0 && p.CanAdmit(OpWrite) != nil {
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
			elapsed := p.device.reads.serviceTime(int64(opBytes(op)))
			work.reads.inFlight -= elapsed
			ring.reads -= elapsed
			p.readBudget.inFlight.Add(-elapsed)
		}
	case OpWrite, OpWritev:
		if work.writes.inFlight > 0 {
			elapsed := p.device.writes.serviceTime(int64(opBytes(op)))
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
