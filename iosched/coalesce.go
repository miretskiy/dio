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
	iovecs := make([]syscall.Iovec, 0, len(run))
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

// completeCoalescedWrite fans a coalesced writev's single completion out to the
// leader and its followers. On full success each member's Result.N is its own
// buffer length; on a short write or error every member fails — retriable, since
// positioned writes are idempotent, so the caller simply re-issues the write.
func completeCoalescedWrite(leader *Ticket, res int32) {
	if res < 0 {
		failCoalescedWrite(leader, syscall.Errno(-res))
		return
	}
	total := len(leader.Op.buf)
	for m := leader.group; m != nil; m = m.next {
		total += len(m.Op.buf)
	}
	if int(res) < total {
		failCoalescedWrite(leader, io.ErrShortWrite)
		return
	}
	// Complete followers first, capturing next before each completion (the owner
	// may Release the ticket as soon as it is woken); complete the leader last so
	// its group chain stays intact throughout.
	for m := leader.group; m != nil; {
		next := m.next
		m.Op.Result.N = len(m.Op.buf)
		completeTicket(m)
		m = next
	}
	leader.Op.Result.N = len(leader.Op.buf)
	completeTicket(leader)
}

// failCoalescedWrite completes the whole group with err (leader last).
func failCoalescedWrite(leader *Ticket, err error) {
	for m := leader.group; m != nil; {
		next := m.next
		m.Op.Result.Err = err
		completeTicket(m)
		m = next
	}
	leader.Op.Result.Err = err
	completeTicket(leader)
}
