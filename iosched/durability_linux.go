//go:build linux

package iosched

// syncOp returns the fdatasync to link after a durable write, targeting the same
// file — virtual slot or regular descriptor. This is the one place the
// virtual/regular distinction is made for durability; callers stay agnostic.
func (o *Op) syncOp() Op {
	if o.isVirtual() {
		return VFdatasyncOp(o.vfd)
	}
	return FdatasyncOp(o.f)
}

// linkDurableSyncs appends one linked sync after every batch write that requests
// durability (Op.Durable or Op.Sync) and does not already carry a linked op. A
// coalesced run is a single writev whose members' durability flags were merged
// onto the leader (see coalesceRun), so a whole run costs one sync — the
// amortization write coalescing buys. The sync is one extra SQE, so the ticket's
// pending count grows by one.
func (c *coordinator) linkDurableSyncs(batch []*Ticket) []*Ticket {
	for _, t := range batch {
		op := &t.Op
		if op.linked != nil {
			continue // a caller-built chain; leave its durability to the caller
		}
		switch op.kind() {
		case OpWrite, OpWritev:
		default:
			continue
		}
		if !op.durable() {
			continue
		}
		sync := op.syncOp()
		op.sqeFlags |= sqeLink
		op.linked = &sync
		t.pending.Add(1)
	}
	return batch
}
