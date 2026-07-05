//go:build linux

package iosched

import (
	"errors"
	"syscall"
	"testing"
)

func TestCoordinatorFailAllInflightCompletesEachLinkedOp(t *testing.T) {
	err := errors.New("ring failed")
	ticket := getTicket(Op{linked: &Op{}}, 2)
	c := coordinator{
		inflight:  make([]inflightEntry, 2),
		freeSlots: make([]int, 0, 2),
		nInflight: 2,
	}
	c.inflight[0] = inflightEntry{t: ticket, op: &ticket.Op, valid: true}
	c.inflight[1] = inflightEntry{t: ticket, op: ticket.Op.linked, valid: true}

	c.failAllInflight(err)
	ticket.Wait()
	defer ticket.Release()

	if got := ticket.pending.Load(); got != 0 {
		t.Fatalf("pending after failing linked inflight ops: got %d want 0", got)
	}
	if got := ticket.Error(); !errors.Is(got, err) {
		t.Fatalf("ticket error: got %v want %v", got, err)
	}
	if c.nInflight != 0 {
		t.Fatalf("nInflight after failAllInflight: got %d want 0", c.nInflight)
	}
}

func TestCoordinatorFailAllInflightCompletesOnlyRemainingLinkedOps(t *testing.T) {
	err := errors.New("ring failed")
	ticket := getTicket(Op{linked: &Op{}}, 2)
	completeTicket(ticket)
	c := coordinator{
		inflight:  make([]inflightEntry, 1),
		freeSlots: make([]int, 0, 1),
		nInflight: 1,
	}
	c.inflight[0] = inflightEntry{t: ticket, op: ticket.Op.linked, valid: true}

	c.failAllInflight(err)
	ticket.Wait()
	defer ticket.Release()

	if got := ticket.pending.Load(); got != 0 {
		t.Fatalf("pending after failing remaining linked op: got %d want 0", got)
	}
	if ticket.Op.result.Err != nil {
		t.Fatalf("completed root op got error: %v", ticket.Op.result.Err)
	}
	if got := ticket.Op.linked.result.Err; !errors.Is(got, err) {
		t.Fatalf("linked op error: got %v want %v", got, err)
	}
}

func TestDrainTable(t *testing.T) {
	d := newDrainTable(4) // dense keys [0,4); the rest are sparse

	if got := d.get(1); got != (fdDrain{}) {
		t.Fatalf("absent dense key: got %+v want zero", got)
	}
	if got := d.get(100); got != (fdDrain{}) {
		t.Fatalf("absent sparse key: got %+v want zero", got)
	}

	d.set(1, fdDrain{inflight: 2})   // dense
	d.set(100, fdDrain{inflight: 3}) // sparse (>= denseCap)
	d.set(-5, fdDrain{inflight: 4})  // negative → sparse
	if got := d.get(1).inflight; got != 2 {
		t.Fatalf("dense: got %d want 2", got)
	}
	if got := d.get(100).inflight; got != 3 {
		t.Fatalf("sparse: got %d want 3", got)
	}
	if got := d.get(-5).inflight; got != 4 {
		t.Fatalf("negative: got %d want 4", got)
	}

	// Setting the zero value removes: a dense slot zeroes, a sparse key is deleted.
	d.set(1, fdDrain{})
	d.set(100, fdDrain{})
	d.set(-5, fdDrain{})
	if got := d.get(1); got != (fdDrain{}) {
		t.Fatalf("dense after zero: got %+v want zero", got)
	}
	if got := d.get(100); got != (fdDrain{}) {
		t.Fatalf("sparse after zero: got %+v want zero", got)
	}
	if len(d.sparse) != 0 {
		t.Fatalf("sparse not emptied: %d entries", len(d.sparse))
	}
}

func TestDrainTableRangeHeld(t *testing.T) {
	d := newDrainTable(4)
	tA, tB := &Ticket{}, &Ticket{}
	d.set(0, fdDrain{inflight: 1, close: tA})  // dense, held
	d.set(50, fdDrain{inflight: 1, close: tB}) // sparse, held
	d.set(2, fdDrain{inflight: 1})             // in-flight, no close held

	held := map[*Ticket]bool{}
	d.rangeHeld(func(tk *Ticket) { held[tk] = true })
	if len(held) != 2 || !held[tA] || !held[tB] {
		t.Fatalf("rangeHeld: got %v want {tA, tB}", held)
	}
}

func TestSlotIovecs(t *testing.T) {
	c := coordinator{iovecBufs: make([][]syscall.Iovec, 4)}

	// A user vectored op: one iovec per non-empty buffer; empties are skipped.
	uv := &Ticket{Op: WritevOp(nil, [][]byte{make([]byte, 4), nil, make([]byte, 6)}, 0)}
	if iv := c.slotIovecs(0, uv, &uv.Op); len(iv) != 2 || iv[0].Len != 4 || iv[1].Len != 6 {
		t.Fatalf("user vectored iovecs: got %+v want lens [4 6]", iv)
	}

	// A coalesced writev leader (no bufs): the leader's own buf plus each
	// follower's, walked via the group.
	leader := &Ticket{Op: WriteOp(nil, make([]byte, 4), 0)}
	leader.Op.opcode = OpWritev
	f1 := &Ticket{Op: WriteOp(nil, make([]byte, 6), 4)}
	f2 := &Ticket{Op: WriteOp(nil, make([]byte, 2), 10)}
	leader.group = f1
	f1.next = f2
	iv := c.slotIovecs(1, leader, &leader.Op)
	if len(iv) != 3 || iv[0].Len != 4 || iv[1].Len != 6 || iv[2].Len != 2 {
		t.Fatalf("coalesced iovecs: got %+v want lens [4 6 2]", iv)
	}
}
