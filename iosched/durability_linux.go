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

// linkDurableSyncs appends one linked fdatasync after each batch write that asked
// for durability (Op.Durable) and does not already carry a linked op, so its data
// reaches stable storage before the ticket completes. The sync is one extra SQE,
// so the ticket's pending count grows by one.
//
// It is deliberately simple. A coalesced run is already a single writev carrying
// the merged durability flag (see coalesceWrites), so the whole run costs one
// sync — the amortization coalescing buys. Separate, non-contiguous writes to the
// same file each get their own sync; we do not try to find same-file writes and
// fold them under one trailing sync (link them all, then a single fdatasync),
// which is more complex and unlikely to be faster.
func linkDurableSyncs(batch []*Ticket) []*Ticket {
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
