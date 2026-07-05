//go:build linux

package iosched

import (
	"fmt"

	"github.com/miretskiy/dio/internal/buildutil"
)

// The close-drain makes a close wait for its file's in-flight ops to complete
// before it runs. io_uring won't order a close after already-submitted writes
// (it resolves an async op's fixed file at issue, not submission, so a close can
// clear the file out from under a queued write — verified on Linux). Rather than
// push a completion-join onto every caller, the coordinator tracks in-flight ops
// per file and holds a close until the file quiesces.
//
// Virtual slots and regular descriptors share one integer key space — a slot is
// its index, a regular fd is VFiles+fd — so the drain is written once, keyed,
// with the virtual/regular distinction confined to drainKey. State is stored
// densely for keys near zero (all slots, low fds) and in a lazily-allocated map
// for the sparse tail; only files with in-flight ops or a pending close hold an
// entry, so both are bounded by in-flight concurrency (≤ ring depth), not by the
// number of files ever touched.

// drainDenseMargin sizes the dense region past the slot range, so regular fds up
// to this many get array (not map) storage.
const drainDenseMargin = 1024

// fdDrain tracks one file: inflight counts placed-but-unreaped data ops (the ops
// a close must wait for), and close is a close held until they drain. The zero
// value means "not tracked".
type fdDrain struct {
	inflight int32
	close    *Ticket
}

// drainTable maps a unified file key to its fdDrain: a dense slice for keys in
// [0, len(dense)) and a lazily-allocated map for the rest. Values are stored by
// value; the zero value is "absent", so setting a key to the zero value removes
// it and the map holds only live entries.
type drainTable struct {
	dense  []fdDrain
	sparse map[int]fdDrain
}

func newDrainTable(denseCap int) drainTable {
	return drainTable{dense: make([]fdDrain, denseCap)}
}

// get returns key's entry, or the zero value if absent. It never inserts.
func (d *drainTable) get(key int) fdDrain {
	if key >= 0 && key < len(d.dense) {
		return d.dense[key]
	}
	return d.sparse[key]
}

// set stores v under key; storing the zero value removes it.
func (d *drainTable) set(key int, v fdDrain) {
	if key >= 0 && key < len(d.dense) {
		d.dense[key] = v
		return
	}
	if v == (fdDrain{}) {
		delete(d.sparse, key)
		return
	}
	if d.sparse == nil {
		d.sparse = make(map[int]fdDrain)
	}
	d.sparse[key] = v
}

// rangeHeld calls fn for every held close (used at shutdown).
func (d *drainTable) rangeHeld(fn func(*Ticket)) {
	for i := range d.dense {
		if c := d.dense[i].close; c != nil {
			fn(c)
		}
	}
	for _, v := range d.sparse {
		if v.close != nil {
			fn(v.close)
		}
	}
}

// drainKey maps an op to its unified file key: a virtual op's slot index, or
// VFiles+fd for a regular op. This is the only virtual/regular branch in the
// drain.
func (c *coordinator) drainKey(op *Op) int {
	if op.isVirtual() {
		return int(op.vfd)
	}
	return int(c.sched.VFiles) + int(op.f.Fd())
}

// drainDataOp reports whether op counts toward its file's drain — any op that
// names a file and is not a barrier (open/close).
func drainDataOp(op *Op) bool {
	switch op.kind() {
	case OpOpenat, OpClose:
		return false
	default:
		return true
	}
}

// drainInc records a placed data op against its file.
func (c *coordinator) drainInc(op *Op) {
	if !drainDataOp(op) {
		return
	}
	key := c.drainKey(op)
	e := c.drain.get(key)
	e.inflight++
	c.drain.set(key, e)
}

// drainDec records a completed data op. When its file reaches zero in-flight ops
// the entry is dropped and any close waiting on it is returned to be placed.
func (c *coordinator) drainDec(op *Op) *Ticket {
	if !drainDataOp(op) {
		return nil
	}
	key := c.drainKey(op)
	e := c.drain.get(key)
	// Every placed data op was counted by drainInc, so a live entry must exist
	// here. inflight == 0 means a drainInc/drainDec imbalance — a bug that would
	// otherwise underflow to -1 silently and mis-time a close.
	if err := buildutil.Assert(e.inflight > 0); err != nil {
		c.sched.stop(fmt.Errorf("iosched: drain underflow for key %d: %w", key, err))
		return nil
	}
	e.inflight--
	if e.inflight > 0 {
		c.drain.set(key, e)
		return nil
	}
	held := e.close
	c.drain.set(key, fdDrain{}) // quiescent: forget it
	return held
}

// isDrainClose reports whether t is a close the drain governs. Both virtual and
// regular closes qualify.
func (c *coordinator) isDrainClose(t *Ticket) bool {
	return t.Op.kind() == OpClose
}

// holdClose defers t as its file's pending close if the file still has in-flight
// ops, returning true; it returns false if the file is quiescent and the close
// may be placed now. A file has one open→close lifecycle at a time, so at most
// one close is ever held on it.
func (c *coordinator) holdClose(t *Ticket) bool {
	key := c.drainKey(&t.Op)
	e := c.drain.get(key)
	if e.inflight == 0 {
		return false
	}
	e.close = t
	c.drain.set(key, e)
	return true
}

// failDrainHeld fails every close still held at shutdown so no caller waits
// forever.
func (c *coordinator) failDrainHeld(err error) {
	c.drain.rangeHeld(func(t *Ticket) { c.failTicket(t, err) })
}
