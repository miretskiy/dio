package iosched

import (
	"cmp"
	"io"
	"slices"
	"syscall"
	"unsafe"
)

// coalescibleWrite reports whether o may be merged into a writev: a standalone
// (unlinked) plain positioned write. Fixed-buffer, linked, read, sync, openat,
// and close ops are never coalesced.
func (o *Op) coalescibleWrite() bool {
	return o.base() == OpWrite && o.linked == nil && o.sqeFlags&sqeLink == 0
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

// targetOrd is a stable value used only to cluster same-target writes adjacently
// before the linear run scan; identity is always decided by sameWriteTarget, not
// this. The sort separates regular from virtual first, so the two uintptr spaces
// never mix.
func targetOrd(o *Op) uintptr {
	if o.isVirtual() {
		return uintptr(o.vfd)
	}
	return uintptr(unsafe.Pointer(o.f))
}

// groupCoalescibleWrites reorders cands by target then offset and merges maximal
// contiguous same-file runs (offset[i] == offset[i-1] + len(buf[i-1])). Each
// merged run's lowest-offset ticket becomes the leader: its op is rewritten to a
// writev over the run's buffers and the other tickets are attached as
// leader.group. It returns the tickets to place — leaders and un-merged
// singletons; merged followers are not returned, since the leader's completion
// fans out to them. cands is scratch; the returned slice aliases its backing.
func groupCoalescibleWrites(cands []*Ticket) []*Ticket {
	slices.SortFunc(cands, func(a, b *Ticket) int {
		oa, ob := &a.Op, &b.Op
		if av, bv := oa.isVirtual(), ob.isVirtual(); av != bv {
			if av {
				return 1 // regular before virtual
			}
			return -1
		}
		if c := cmp.Compare(targetOrd(oa), targetOrd(ob)); c != 0 {
			return c
		}
		return cmp.Compare(oa.offset, ob.offset)
	})

	out := cands[:0]
	for i := 0; i < len(cands); {
		hi := cands[i].Op.offset + int64(len(cands[i].Op.buf))
		j := i + 1
		for j < len(cands) &&
			sameWriteTarget(&cands[i].Op, &cands[j].Op) &&
			cands[j].Op.offset == hi {
			hi += int64(len(cands[j].Op.buf))
			j++
		}
		if j-i >= 2 {
			coalesceRun(cands[i:j])
		}
		out = append(out, cands[i]) // leader (or lone singleton)
		i = j
	}
	return out
}

// coalesceRun rewrites run[0] into a writev over every member's buffer and chains
// run[1:] onto it as its group. run is in ascending, contiguous offset order, so
// run[0] carries the run's base offset. The leader keeps its original Op.buf, so
// its own byte count is still len(Op.buf) at completion.
func coalesceRun(run []*Ticket) {
	leader := run[0]
	// Back the iovecs inline on the leader when the run fits (no heap alloc); the
	// leader Op is stable in its ticket, so iovecs may alias leader.Op.iovecsBuf.
	iovecs := leader.Op.iovecsBacking(len(run))
	for _, m := range run {
		b := m.Op.buf
		if len(b) == 0 {
			continue
		}
		iovecs = append(iovecs, syscall.Iovec{Base: &b[0], Len: uint64(len(b))})
	}
	leader.Op.opcode = OpWritev | (leader.Op.opcode & opVirtual)
	leader.Op.iovecs = iovecs

	var tail *Ticket
	for _, f := range run[1:] {
		f.next = nil
		if tail == nil {
			leader.group = f
		} else {
			tail.next = f
		}
		tail = f
	}
}

// completeCoalescedGroup fans a finished coalesced leader's result out to its
// followers. reap calls it once the leader's whole chain has completed — the
// writev, plus a linked fdatasync when the slot is durable — having recorded
// each op's CQE result and driven leader.pending to zero. A write error, a
// durability (fdatasync) error, or a short write fails the whole group; all are
// retriable, since positioned writes are idempotent, so the caller re-issues.
func completeCoalescedGroup(leader *Ticket) {
	written := leader.Op.Result.N // total bytes the writev reported
	err := leader.Op.Result.Err
	if err == nil && leader.Op.linked != nil {
		err = leader.Op.linked.Result.Err // the linked fdatasync's result
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
		op.Result.N = 0
		op.Result.Err = err
		return
	}
	n := min(len(op.buf), *remaining)
	*remaining -= n
	op.Result.N = n
	if n < len(op.buf) {
		op.Result.Err = io.ErrShortWrite
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
		if m.Op.Result.Err == nil {
			m.Op.Result.Err = err
		}
		if n := m.pending.Swap(0); n > 0 {
			m.wg.Done()
		}
		m = next
	}
	if leader.Op.Result.Err == nil {
		leader.Op.Result.Err = err
	}
	if n := leader.pending.Swap(0); n > 0 {
		leader.wg.Done()
	}
}
