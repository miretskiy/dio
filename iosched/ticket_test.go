package iosched

import (
	"errors"
	"testing"
)

func TestTicketReleaseNilSafeIdempotentAndPendingSafe(t *testing.T) {
	var nilTicket *Ticket
	nilTicket.Release()

	completed := &Ticket{
		sched: &URingScheduler{},
		Op: Op{
			buf:    []byte("root"),
			linked: &Op{buf: []byte("linked")},
		},
	}
	completed.Release()
	completed.Release()
	if completed.sched != nil {
		t.Fatal("completed ticket kept scheduler after Release")
	}
	if completed.Op.linked != nil || completed.Op.buf != nil {
		t.Fatal("completed ticket retained operation references")
	}

	pending := &Ticket{
		sched: &URingScheduler{},
		Op: Op{
			buf:    []byte("root"),
			linked: &Op{buf: []byte("linked")},
		},
	}
	pending.pending.Store(1)
	pending.Release()
	pending.Release()
	if pending.sched != nil {
		t.Fatal("pending ticket kept scheduler after Release")
	}
	if pending.Op.linked == nil || pending.Op.buf == nil {
		t.Fatal("pending ticket cleared operation references while still in flight")
	}
}

func TestTicketErrorReturnsFirstLinkedError(t *testing.T) {
	first := errors.New("first")
	second := errors.New("second")

	ticket := &Ticket{
		Op: Op{
			linked: &Op{},
		},
	}
	ticket.Op.linked.Result.Err = second
	if got := ticket.Error(); got != second {
		t.Fatalf("root success: got %v want %v", got, second)
	}

	ticket.Op.Result.Err = first
	if got := ticket.Error(); got != first {
		t.Fatalf("root failure: got %v want %v", got, first)
	}

	if got := ((*Ticket)(nil)).Error(); got != nil {
		t.Fatalf("nil ticket error: got %v want nil", got)
	}
}
