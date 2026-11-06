package alloc

import "sync/atomic"

// Stats holds cumulative byte totals for successful acquisitions. RequestedBytes
// counts the original request before capping or rounding; AllocatedBytes counts
// the actual returned length. Attempts that acquire nothing contribute nothing.
// Buffer growth contributes its individual TryAcquire requests. A partial
// allocation can cause the remaining bytes to be requested again.
type Stats struct {
	RequestedBytes uint64
	AllocatedBytes uint64
}

type allocationCounters struct {
	requested atomic.Uint64
	allocated atomic.Uint64
}

func (s *allocationCounters) record(requested, allocated uint64) {
	s.requested.Add(requested)
	s.allocated.Add(allocated)
}

// Stats sums allocation totals across spans, retained after Close. The counters
// are read independently, so concurrent acquisitions can appear in one total
// before the other. Capacity and Available report current usage separately.
// Collection is disabled by default, returning zero totals. Build with the
// allocstats tag to enable collection for diagnostics.
func (a *Allocator) Stats() Stats {
	var total Stats
	for i := range a.stats {
		total.RequestedBytes += a.stats[i].requested.Load()
		total.AllocatedBytes += a.stats[i].allocated.Load()
	}
	return total
}
