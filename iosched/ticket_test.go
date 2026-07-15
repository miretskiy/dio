package iosched

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestTicketErrorReportsLinkedError(t *testing.T) {
	err := errors.New("linked failure")
	root := Op{}.Link(Op{})
	ticket := root.prepareSubmission()
	recordResult(&root, root.linked, 0, err)
	if got := ticket.Error(); got != err {
		t.Fatalf("ticket error: got %v want %v", got, err)
	}
	completeSubmission(&root)
}

func TestOpLinkBuildsFlatChain(t *testing.T) {
	op := Op{}.Link(Op{}).HardLink(Op{})
	if op.opCount() != 3 {
		t.Fatalf("operation count: got %d want 3", op.opCount())
	}
	if op.sqeFlags&sqeLink == 0 {
		t.Fatal("root is not linked to first follower")
	}
	if linkedOpAt(&op, 1).sqeFlags&sqeHardLink == 0 {
		t.Fatal("first follower is not hard-linked to second follower")
	}
	if linkedOpAt(&op, 2).isLinked() {
		t.Fatal("last follower unexpectedly links onward")
	}
}

func TestOpLinkCopyDoesNotMutateSource(t *testing.T) {
	short := Op{}.Link(Op{})
	extendedShort := short.HardLink(Op{})
	if linkedOpAt(&short, short.opCount()-1).isLinked() {
		t.Fatal("Link on copied Op mutated source chain")
	}
	if linkedOpAt(&extendedShort, extendedShort.opCount()-2).sqeFlags&sqeHardLink == 0 {
		t.Fatal("extended copy did not link its previous tail")
	}

	base := Op{}.Link(Op{}, Op{}, Op{})
	extended := base.HardLink(Op{})
	if linkedOpAt(&base, base.opCount()-1).isLinked() {
		t.Fatal("Link on copied Op mutated source chain")
	}
	if linkedOpAt(&extended, extended.opCount()-2).sqeFlags&sqeHardLink == 0 {
		t.Fatal("extended copy did not link its previous tail")
	}
}

func TestOpLinkFlattensLinkedInputAndPreservesFlags(t *testing.T) {
	tail := Op{}.HardLink(Op{})
	op := Op{}.Link(tail)
	if linkedOpAt(&op, 0).sqeFlags&sqeLink == 0 {
		t.Fatal("root is not linked to nested chain")
	}
	if linkedOpAt(&op, 1).sqeFlags&sqeHardLink == 0 {
		t.Fatal("nested chain lost its hard-link flag")
	}
	if linkedOpAt(&op, 1).sqeFlags&sqeLink != 0 {
		t.Fatal("outer link flag leaked into nested chain")
	}
}

func linkedOpAt(op *Op, index int) *Op {
	for p := op; p != nil; p = p.linked {
		if index == 0 {
			return p
		}
		index--
	}
	panic("operation index outside linked chain")
}

func TestSubmissionOwnsOpCopy(t *testing.T) {
	op := Op{buf: []byte("root")}.
		Link(Op{buf: []byte("linked")})
	root := op
	ticket := root.prepareSubmission()
	op = Op{}

	if string(root.buf) != "root" {
		t.Fatal("submission did not retain its root operation copy")
	}
	if root.linked == nil || string(root.linked.buf) != "linked" {
		t.Fatal("submission did not retain the immutable linked chain")
	}
	completeSubmission(&root)
	ticket.Wait()
}

func TestDurableWriteInLinkedChainRejected(t *testing.T) {
	f := new(os.File)
	op := WriteOp(f, nil, 0).Durable().Link(CloseOp(f))
	_, err := countAndValidateOps(&op)
	if err == nil || !strings.Contains(err.Error(), "Durable cannot be used in a linked chain") {
		t.Fatalf("validation error: got %v", err)
	}
}

func TestStandaloneDurableWriteAccepted(t *testing.T) {
	op := WriteOp(new(os.File), nil, 0).Durable()
	if _, err := countAndValidateOps(&op); err != nil {
		t.Fatalf("validation error: %v", err)
	}
}

func TestTicketWaitsForSubmissionCompletion(t *testing.T) {
	root := Op{}.Link(Op{})
	ticket := root.prepareSubmission()
	done := make(chan struct{})
	go func() {
		ticket.Wait()
		close(done)
	}()

	recordResult(&root, &root, 1, nil)
	select {
	case <-done:
		t.Fatal("Wait returned before the submission completed")
	default:
	}

	completeSubmission(&root)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after submission completion")
	}
}

func BenchmarkTicketCompletion(b *testing.B) {
	root := Op{}
	ticket := root.prepareSubmission()
	completeSubmission(&root)
	ticket.Wait()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		root := Op{}
		ticket := root.prepareSubmission()
		completeSubmission(&root)
		ticket.Wait()
	}
}

var benchmarkSubmission *Op
var benchmarkTicket Ticket

func BenchmarkSubmissionState(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		root := Op{}
		ticket := root.prepareSubmission()
		benchmarkSubmission = &root
		completeSubmission(&root)
		benchmarkTicket = ticket
	}
}

var benchmarkLinkedOp Op

func BenchmarkOpLink3(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		benchmarkLinkedOp = Op{}.Link(Op{}, Op{})
	}
}

func BenchmarkOpLinkChain8(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		op := Op{}
		for range 7 {
			op = op.Link(Op{})
		}
		benchmarkLinkedOp = op
	}
}

func BenchmarkOpLinkBatch8(b *testing.B) {
	tail := make([]Op, 7)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkLinkedOp = (Op{}).Link(tail...)
	}
}
