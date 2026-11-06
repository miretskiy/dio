package alloc

import "github.com/miretskiy/dio/v2/align"

// rawMemory owns an arena-relative range of pages without a lease. It may span
// several spans after joining adjacent claims. Its allocator supplies the owner;
// the range must be leased or released exactly once, and must not escape publicly.
type rawMemory block

func (m rawMemory) bytes(a *arena) []byte {
	end := (m.start + m.n) * align.BlockSize
	return a.region[m.start*align.BlockSize : end : end]
}

func (m rawMemory) lease(a *arena) Memory {
	return a.leases.lease(m.start, m.bytes(a))
}

// join combines new, unwritten claims in either address order. It must not be
// used to prepend storage to a range whose bytes already have a stream order.
func (m *rawMemory) join(other rawMemory) bool {
	if m.start+m.n == other.start {
		m.n += other.n
		return true
	}
	if other.start+other.n == m.start {
		m.start = other.start
		m.n += other.n
		return true
	}
	return false
}

// acquireRaw claims and prepares storage without publishing a Memory handle.
func (a *Allocator) acquireRaw(size int64) (rawMemory, error) {
	b, ok := a.arena.claim(allocationPages(size))
	if !ok {
		return rawMemory{}, ErrExhausted
	}
	m := rawMemory(b)
	a.prepareRaw(m, size)
	return m, nil
}

// prepareRaw zeroes only newly claimed bytes and accounts for each successful
// request before coalescing. Neither operation requires a lease.
func (a *Allocator) prepareRaw(m rawMemory, requested int64) {
	if !a.cfg.noZero {
		clear(m.bytes(a.arena))
	}
	if allocationStatsEnabled {
		a.stats[m.start/spanPages].record(uint64(requested), uint64(m.n*align.BlockSize))
	}
}
