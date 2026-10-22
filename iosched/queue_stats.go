package iosched

import "sync/atomic"

// QueueDepth counts submissions, not operations, bytes, or ring entries. Peak
// is the high-water mark over the scheduler's lifetime, including warmup.
type QueueDepth struct {
	Current int64
	Peak    int64
}

// QueueStats describes work inside an asynchronous scheduler. A linked chain
// counts as one submission, and coalesced submissions retain separate counts.
// Fields are observed independently, not as one atomic snapshot. Their peaks
// can occur at different times and must not be added into a total peak.
type QueueStats struct {
	// Staged is the submitters' stack, before the coordinator detaches it.
	Staged QueueDepth
	// Waiting is accepted work held by file-lifecycle barriers.
	Waiting QueueDepth
	// Ready is eligible work awaiting placement, including budget/ring waits.
	// This is not a count of submissions blocked specifically by the budget.
	Ready QueueDepth
	// InFlight is placed work whose ticket has not completed, including
	// durable writes awaiting sync. It is not kernel/device queue depth.
	InFlight QueueDepth
}

type queueDepth struct {
	current atomic.Int64
	peak    atomic.Int64
}

func (d *queueDepth) add(delta int64) {
	observeQueuePeak(&d.peak, d.current.Add(delta))
}

func (d *queueDepth) snapshot() QueueDepth {
	current := d.current.Load()
	return QueueDepth{Current: current, Peak: max(current, d.peak.Load())}
}

func observeQueuePeak(peak *atomic.Int64, value int64) {
	for old := peak.Load(); value > old; old = peak.Load() {
		if peak.CompareAndSwap(old, value) {
			return
		}
	}
}
