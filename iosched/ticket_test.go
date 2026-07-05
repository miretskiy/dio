package iosched

import (
	"errors"
	"testing"
	"time"
)

func TestTicketReleaseNilSafeIdempotentAndPendingSafe(t *testing.T) {
	var nilTicket *Ticket
	nilTicket.Release()

	completed := getTicket(Op{
		buf:    []byte("root"),
		linked: &Op{buf: []byte("linked")},
	}, 1)
	completeTicket(completed)
	completed.Release()
	completed.Release()
	if completed.Op.linked != nil || completed.Op.buf != nil {
		t.Fatal("completed ticket retained operation references")
	}

	inflight := getTicket(Op{
		buf:    []byte("root"),
		linked: &Op{buf: []byte("linked")},
	}, 1)
	inflight.Release()
	if inflight.Op.linked == nil || inflight.Op.buf == nil {
		t.Fatal("in-flight ticket cleared operation references")
	}
	completeTicket(inflight)
	inflight.Release()
	if inflight.Op.linked == nil || inflight.Op.buf == nil {
		t.Fatal("early-released ticket was pooled after completion")
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
	ticket.Op.linked.result.Err = second
	if got := ticket.Error(); got != second {
		t.Fatalf("root success: got %v want %v", got, second)
	}

	ticket.Op.result.Err = first
	if got := ticket.Error(); got != first {
		t.Fatalf("root failure: got %v want %v", got, first)
	}

	if got := ((*Ticket)(nil)).Error(); got != nil {
		t.Fatalf("nil ticket error: got %v want nil", got)
	}
}

func TestOpLinkAppendsToExistingChain(t *testing.T) {
	first := errors.New("first")
	second := errors.New("second")

	op := Op{}.Link(Op{}).Link(Op{})
	op.linked.result.Err = first
	op.linked.linked.result.Err = second

	ticket := &Ticket{Op: op}
	if got := ticket.Error(); got != first {
		t.Fatalf("first linked error: got %v want %v", got, first)
	}
	op.linked.result.Err = nil
	if got := ticket.Error(); got != second {
		t.Fatalf("second linked error: got %v want %v", got, second)
	}
}

func TestTicketPendingCompletionCountsDown(t *testing.T) {
	ticket := getTicket(Op{}, 2)
	done := make(chan struct{})
	go func() {
		ticket.Wait()
		close(done)
	}()

	completeTicket(ticket)
	if got := ticket.pending.Load(); got != 1 {
		t.Fatalf("pending after first completion: got %d want 1", got)
	}
	select {
	case <-done:
		t.Fatal("Wait returned before all pending ops completed")
	default:
	}

	completeTicket(ticket)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after pending reached zero")
	}
	if got := ticket.pending.Load(); got != 0 {
		t.Fatalf("pending after second completion: got %d want 0", got)
	}
	ticket.Release()
}

func TestTicketPendingUnderflowPanics(t *testing.T) {
	ticket := getTicket(Op{}, 1)
	completeTicket(ticket)
	defer func() {
		if recover() == nil {
			t.Fatal("completeTicket did not panic on pending underflow")
		}
	}()
	completeTicket(ticket)
}

func BenchmarkTicketGetCompleteRelease(b *testing.B) {
	ticket := getTicket(Op{}, 1)
	completeTicket(ticket)
	ticket.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		ticket := getTicket(Op{}, 1)
		completeTicket(ticket)
		ticket.Release()
	}
}
