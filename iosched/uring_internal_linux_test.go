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
	if ticket.Op.Result.Err != nil {
		t.Fatalf("completed root op got error: %v", ticket.Op.Result.Err)
	}
	if got := ticket.Op.linked.Result.Err; !errors.Is(got, err) {
		t.Fatalf("linked op error: got %v want %v", got, err)
	}
}

func newVCoordinator(n uint32) coordinator {
	s := &URingScheduler{URingConfig: URingConfig{RingDepth: 16, VFiles: n}}
	return coordinator{sched: s, parkedOps: make([]*Ticket, n)}
}

func TestCoordinatorFilterParksVUseBehindOpeningSlot(t *testing.T) {
	c := newVCoordinator(8)
	c.parkedOps[7] = &barrierOp

	ticket := getTicket(VWriteOp(7, []byte("x"), 0), 1)
	runnable := c.filterRunnable(nil, ticket)
	if len(runnable) != 0 {
		t.Fatalf("filter returned runnable ticket behind opening slot")
	}
	if c.parkedOps[7] != ticket {
		t.Fatalf("parked ticket: got %p want %p", c.parkedOps[7], ticket)
	}
}

func TestCoordinatorFilterParksBehindRunnableVOpenBarrier(t *testing.T) {
	c := newVCoordinator(8)
	fd := uint32(7)
	open := getTicket(VOpenatOp(-100, "first.dat", 0, 0o600, fd), 1)
	write := getTicket(VWriteOp(fd, []byte("x"), 0), 1)
	open.next = write

	runnable := c.filterRunnable(nil, open)
	if len(runnable) != 1 || runnable[0] != open {
		t.Fatalf("runnable: got %v, want only open %p", runnable, open)
	}
	if c.parkedOps[7] != write {
		t.Fatalf("parked write: got %p want %p", c.parkedOps[7], write)
	}
}

func TestCoordinatorVOpenActivatesParkedTickets(t *testing.T) {
	c := newVCoordinator(8)
	ticket := getTicket(VWriteOp(7, []byte("x"), 0), 1)
	c.parkedOps[7] = ticket

	op := VOpenatOp(-100, "opened.dat", 0, 0o600, 7)
	ready := c.completeVOp(&op)
	if ready != ticket {
		t.Fatalf("ready ticket: got %p want %p", ready, ticket)
	}
}

func TestCoordinatorVCloseFailsParkedOps(t *testing.T) {
	c := newVCoordinator(8)
	fd := uint32(7)
	read := getTicket(VReadOp(fd, []byte("x"), 0), 1)
	open := getTicket(VOpenatOp(-100, "next.dat", 0, 0o600, fd), 1)
	after := getTicket(VWriteOp(fd, []byte("y"), 0), 1)
	read.next = open
	open.next = after
	c.parkedOps[7] = read

	op := VCloseOp(fd)
	ready := c.completeVOp(&op)
	read.Wait()
	if ready != nil {
		t.Fatalf("ready list after close: got %p want nil", ready)
	}
	for _, tkt := range []*Ticket{read, open, after} {
		if !errors.Is(tkt.Error(), syscall.EBADF) {
			t.Fatalf("ticket error: got %v want EBADF", tkt.Error())
		}
	}
}
