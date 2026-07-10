package iosched

import (
	"io"
)

// coalescibleWrite reports whether o may be merged into a writev: a standalone
// (unlinked) plain positioned write. Fixed-buffer, linked, read, sync, openat,
// and close ops are never coalesced.
func (o *Op) coalescibleWrite() bool {
	return o.kind() == OpWrite && !o.isFixed() && !o.isLinked()
}

// sameWriteTarget reports whether two writes address the same file: the same
// registered slot (virtual) or the same *os.File (regular). A regular and a
// virtual write never share a target, even at equal descriptor numbers.
func sameWriteTarget(a, b *Op) bool {
	if a.isVirtual() != b.isVirtual() {
		return false
	}
	if a.isVirtual() {
		return a.vfd == b.vfd
	}
	return a.f == b.f
}

// coalesceWrites merges runs of contiguous same-file plain writes in the batch
// into single writev leaders, in place, and returns the compacted prefix. It is
// one forward pass: a write extends the current run when it targets the same file
// at the run's next offset (offset == prior end), otherwise it starts a new run;
// any non-coalescible op ends the run. A merged follower leaves the batch — its
// bytes fold into the leader, whose one completion fans out to the whole group
// (see completeCoalescedGroup) — while leaders and singletons stay in submission
// order.
//
// It deliberately does no sorting or same-file lookahead: two writes to one file
// separated in the batch by another op simply don't coalesce. Missing a merge
// only costs an extra SQE, never correctness, and the caller submitting writes it
// wants merged in contiguous order is the common case.
func coalesceWrites(batch []*Ticket) []*Ticket {
	w := 0                    // compaction write cursor
	var leader, tail *Ticket  // current run; leader == nil means no open run
	var runEnd int64          // file offset just past the run's last byte
	for _, t := range batch {
		op := &t.Op
		if leader != nil && op.coalescibleWrite() &&
			sameWriteTarget(&leader.Op, op) && op.offset == runEnd {
			// Fold t into the run. On the first follower, promote the leader from a
			// plain write to a writev; its iovecs are built at placement from the
			// leader's own buf plus each follower's (see slotIovecs), and the leader
			// keeps its Op.buf so its own byte count stays len(Op.buf).
			if leader.group == nil {
				leader.Op.opcode = OpWritev | (leader.Op.opcode & opVirtual)
				leader.group = t
			} else {
				tail.next = t
			}
			t.next = nil
			tail = t
			leader.Op.opFlags |= op.opFlags & opDurable // run is durable if any member is
			runEnd += int64(len(op.buf))
			continue
		}
		// t is not folded: it starts a new run if it's a coalescible write, else it
		// breaks the run. Either way it stays in the batch.
		if op.coalescibleWrite() {
			leader, tail, runEnd = t, nil, op.offset+int64(len(op.buf))
		} else {
			leader = nil
		}
		batch[w] = t
		w++
	}
	return batch[:w]
}

// completeCoalescedGroup fans a finished coalesced leader's result out to its
// followers. reap calls it once the leader's whole chain has completed — the
// writev, plus a linked fdatasync when the slot is durable — having recorded
// each op's CQE result and driven leader.pending to zero. A write error, a
// durability (fdatasync) error, or a short write fails the whole group; all are
// retriable, since positioned writes are idempotent, so the caller re-issues.
func completeCoalescedGroup(leader *Ticket) {
	written := leader.Op.result.N // total bytes the writev reported
	err := leader.Op.result.Err
	if err == nil && leader.Op.linked != nil {
		err = leader.Op.linked.result.Err // the linked fdatasync's result
	}
	// Distribute the written bytes across members in writev order — leader first,
	// then followers by ascending offset. A fully covered member succeeds; the
	// boundary member gets its partial count and io.ErrShortWrite; members past
	// the end get zero and io.ErrShortWrite. A hard write or fdatasync error gives
	// every member zero and that error. Positioned writes are idempotent, so a
	// short or failed member is safely retriable.
	remaining := written
	distributeWrite(&leader.Op, &remaining, err)
	for m := leader.group; m != nil; m = m.next {
		distributeWrite(&m.Op, &remaining, err)
	}
	// Complete followers first, capturing next before each completion (the owner
	// may Release the ticket the moment it is woken); leader last so the group
	// chain stays intact throughout.
	for m := leader.group; m != nil; {
		next := m.next
		completeTicket(m)
		m = next
	}
	leader.wg.Done() // leader.pending already reached zero in reap
}

// distributeWrite records how much of op.buf the coalesced writev covered and
// advances remaining. A hard error zeroes N and records err; otherwise the op
// gets min(remaining, len) bytes, and a short count records io.ErrShortWrite.
func distributeWrite(op *Op, remaining *int, err error) {
	if err != nil {
		op.result.N = 0
		op.result.Err = err
		return
	}
	n := min(len(op.buf), *remaining)
	*remaining -= n
	op.result.N = n
	if n < len(op.buf) {
		op.result.Err = io.ErrShortWrite
	}
}

// failCoalescedWrite fails a whole coalesced group with err. It is used by the
// shutdown/error paths, where the leader may be un-placed or have SQEs still in
// flight, so it forces each ticket's pending to zero in one step — making it
// idempotent, since failAllInflight can reach a multi-SQE leader once per SQE.
// The leader is done last so the group chain stays intact.
func failCoalescedWrite(leader *Ticket, err error) {
	for m := leader.group; m != nil; {
		next := m.next
		if m.Op.result.Err == nil {
			m.Op.result.Err = err
		}
		if n := m.pending.Swap(0); n > 0 {
			m.wg.Done()
		}
		m = next
	}
	if leader.Op.result.Err == nil {
		leader.Op.result.Err = err
	}
	if n := leader.pending.Swap(0); n > 0 {
		leader.wg.Done()
	}
}
