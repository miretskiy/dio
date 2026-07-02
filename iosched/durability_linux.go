//go:build linux

package iosched

import "syscall"

// syncMode records the durability requested for a virtual-file slot, derived
// from its open flags. A durable slot gets one fdatasync/fsync linked after each
// write (see linkDurableSyncs); the amortization comes from write coalescing —
// a contiguous run becomes one writev, so the whole run needs only one sync.
type syncMode uint8

const (
	syncNone syncMode = iota
	syncData          // O_DSYNC: flush data (fdatasync)
	syncFull          // O_SYNC:  flush data and metadata (fsync)
)

// openatSyncMode maps openat(2) flags to a syncMode. On Linux O_SYNC implies
// O_DSYNC (O_SYNC == __O_SYNC|O_DSYNC), so O_SYNC must be tested first.
func openatSyncMode(flags int) syncMode {
	switch {
	case flags&syscall.O_SYNC == syscall.O_SYNC:
		return syncFull
	case flags&syscall.O_DSYNC != 0:
		return syncData
	default:
		return syncNone
	}
}

// stripSyncFlags clears O_SYNC/O_DSYNC. The io_uring backend opens durable slots
// without them so the kernel does not sync on every write; durability is instead
// applied once per coalesced run via a linked sync (see linkDurableSyncs). The
// POSIX backend keeps the flags and lets the kernel do the syncing.
func stripSyncFlags(flags int) int {
	return flags &^ (syscall.O_SYNC | syscall.O_DSYNC)
}

// noteSyncMode records (or, with syncNone, clears) a slot's durability, keeping
// syncSlots — the number of durable slots — in step so linkDurableSyncs can be
// skipped entirely when no slot is durable.
func (c *coordinator) noteSyncMode(slot uint32, m syncMode) {
	if c.syncMode == nil {
		return
	}
	switch was := c.syncMode[slot]; {
	case was == syncNone && m != syncNone:
		c.syncSlots++
	case was != syncNone && m == syncNone:
		c.syncSlots--
	}
	c.syncMode[slot] = m
}

// linkDurableSyncs appends one linked sync (fdatasync for O_DSYNC, fsync for
// O_SYNC) after every batch write that targets a durable slot, so the write is
// durable before its ticket completes. A coalesced run is a single writev, so a
// whole run costs one sync. Writes that already carry a linked op (a
// caller-built chain) are left untouched. The sync is one extra SQE, so the
// ticket's pending count grows by one.
func (c *coordinator) linkDurableSyncs(batch []*Ticket) []*Ticket {
	for _, t := range batch {
		op := &t.Op
		if op.linked != nil || !op.isVirtual() {
			continue
		}
		switch op.base() {
		case OpWrite, OpWritev, OpWriteFixed:
		default:
			continue
		}
		mode := c.syncMode[op.vfd]
		if mode == syncNone {
			continue
		}
		sync := VFdatasyncOp(op.vfd)
		if mode == syncFull {
			sync = VFsyncOp(op.vfd)
		}
		op.sqeFlags |= sqeLink
		op.linked = &sync
		t.pending.Add(1)
	}
	return batch
}
