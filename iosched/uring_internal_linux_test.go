//go:build linux

package iosched

import (
	"errors"
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
