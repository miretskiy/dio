package iosched

import (
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTicketErrorReportsLinkedError(t *testing.T) {
	err := errors.New("linked failure")
	root := Op{}.Link(Op{})
	ticket := root.prepareSubmission()
	recordResult(&root, root.linked, 0, err)
	root.finish()
	_, got := ticket.Wait()
	if got != err {
		t.Fatalf("ticket error: got %v want %v", got, err)
	}
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
	if op.completion != nil {
		t.Fatal("preparing the submission copy modified the caller's operation")
	}

	if string(root.buf) != "root" {
		t.Fatal("submission did not retain its root operation copy")
	}
	if root.linked == nil || string(root.linked.buf) != "linked" {
		t.Fatal("submission did not retain the immutable linked chain")
	}
	root.finish()
	ticket.Wait()
}

func TestDurableWriteInLinkedChainRejected(t *testing.T) {
	f := new(os.File)
	op := WriteOp(f, nil, 0).Durable().Link(DrainOp(f))
	_, err := countAndValidateOps(&op, nil)
	if err == nil || !strings.Contains(err.Error(), "Durable cannot be used in a linked chain") {
		t.Fatalf("validation error: got %v", err)
	}
}

func TestDrainMustEndLinkedChain(t *testing.T) {
	f := new(os.File)
	op := DrainOp(f).Link(ReadOp(f, nil, 0))
	_, err := countAndValidateOps(&op, nil)
	if err == nil || !strings.Contains(err.Error(), "DrainOp must be the final operation") {
		t.Fatalf("validation error: got %v", err)
	}
}

func TestStandaloneDurableWriteAccepted(t *testing.T) {
	op := WriteOp(new(os.File), nil, 0).Durable()
	if _, err := countAndValidateOps(&op, nil); err != nil {
		t.Fatalf("validation error: %v", err)
	}
}

func TestDurableWriteSyncOpPreservesTarget(t *testing.T) {
	regular := new(os.File)
	regularSync := WriteOp(regular, nil, 0).syncOp()
	if regularSync.kind() != OpFdatasync || regularSync.f != regular {
		t.Fatalf("regular sync op did not preserve file: %#v", regularSync)
	}

	virtualSync := VWriteOp(3, nil, 0).syncOp()
	if virtualSync.kind() != OpFdatasync || !virtualSync.isVirtual() || virtualSync.vfd != 3 {
		t.Fatalf("virtual sync op did not preserve slot: %#v", virtualSync)
	}
}

func TestWriteResultError(t *testing.T) {
	f := new(os.File)
	write := WriteOp(f, make([]byte, 4), 0)
	if err := writeResultError(&write, 2, nil); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write error: got %v want %v", err, io.ErrShortWrite)
	}
	if err := writeResultError(&write, 4, nil); err != nil {
		t.Fatalf("complete write error: %v", err)
	}
	read := ReadOp(f, make([]byte, 4), 0)
	if err := writeResultError(&read, 2, nil); err != nil {
		t.Fatalf("short read unexpectedly failed: %v", err)
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

	root.finish()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after submission completion")
	}
}

func BenchmarkTicketCompletion(b *testing.B) {
	root := Op{}
	ticket := root.prepareSubmission()
	root.finish()
	ticket.Wait()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		root := Op{}
		ticket := root.prepareSubmission()
		root.finish()
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
		root.finish()
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

func TestTicketDoneClosesOnCompletion(t *testing.T) {
	root := Op{}
	ticket := root.prepareSubmission()
	done := ticket.Done()
	if done != ticket.Done() {
		t.Fatal("Done returned a different channel on the second call")
	}
	select {
	case <-done:
		t.Fatal("Done closed before the ticket completed")
	default:
	}
	recordResult(&root, &root, 7, nil)
	root.finish()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Done not closed after completion")
	}
	if n, err := ticket.Wait(); n != 7 || err != nil {
		t.Fatalf("Wait after Done: got (%d, %v) want (7, nil)", n, err)
	}
}

func TestTicketDoneAfterCompletionIsClosed(t *testing.T) {
	root := Op{}
	ticket := root.prepareSubmission()
	root.finish()
	select {
	case <-ticket.Done():
	default:
		t.Fatal("Done of a completed ticket is not closed")
	}
}

func TestSubmitNotify(t *testing.T) {
	path := t.TempDir() + "/f"
	if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s := NewPOSIXScheduler()
	defer s.Close()

	calls, gotN := 0, 0
	var gotErr error
	ticket, err := SubmitNotify(s, ReadOp(f, make([]byte, 5), 0), func(n int, err error) {
		calls, gotN, gotErr = calls+1, n, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := ticket.Wait(); n != 5 || err != nil {
		t.Fatalf("Wait: (%d, %v)", n, err)
	}
	if calls != 1 || gotN != 5 || gotErr != nil {
		t.Fatalf("whenDone: calls=%d n=%d err=%v", calls, gotN, gotErr)
	}

	plain, err := s.Submit(ReadOp(f, make([]byte, 5), 0))
	if err != nil {
		t.Fatal(err)
	}
	plain.Wait()
	if calls != 1 {
		t.Fatal("whenDone of one submission ran for another")
	}
}

// TestWhenDoneRacesWait completes tickets concurrently with Wait; whenDone
// must run exactly once, after Done is closed (run with -race).
func TestWhenDoneRacesWait(t *testing.T) {
	for range 1000 {
		var calls atomic.Int32
		root := Op{whenDone: func(int, error) { calls.Add(1) }}
		ticket := root.prepareSubmission()
		go root.finish()
		ticket.Wait()
		for deadline := time.Now().Add(time.Second); calls.Load() == 0 && time.Now().Before(deadline); {
			time.Sleep(time.Microsecond)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("whenDone ran %d times", got)
		}
	}
}

// Selectable completion and allocation-free Wait may race each other and
// finish. Every Done caller must see the same channel and published result.
func TestTicketConcurrentDoneAndWait(t *testing.T) {
	for range 500 {
		root := Op{}
		ticket := root.prepareSubmission()
		var workers sync.WaitGroup
		channels := make(chan (<-chan struct{}), 8)
		for range 8 {
			workers.Go(func() {
				done := ticket.Done()
				channels <- done
				<-done
				n, err := ticket.Wait()
				if n != 7 || err != nil {
					t.Errorf("completion result: %d, %v", n, err)
				}
			})
		}
		recordResult(&root, &root, 7, nil)
		root.finish()
		workers.Wait()
		close(channels)
		for done := range channels {
			if done != ticket.Done() {
				t.Fatal("Done channel changed")
			}
		}
	}
}
