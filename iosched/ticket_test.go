package iosched

import (
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestTicketErrorReportsLinkedError(t *testing.T) {
	err := errors.New("linked failure")
	root := Op{}.Link(Op{})
	work := prepareForTest(root)
	ticket := Ticket{&work.completion}
	work.recordResult(work.op(1), 0, err)
	work.finish()
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

func linkedOpAt(op *Op, index int) *operation {
	if index == 0 {
		return &op.operation
	}
	return &op.followers()[index-1]
}

func prepareForTest(op Op) *submission {
	work, err := copySubmission(&op, maxChainLength)
	if err != nil {
		panic(err)
	}
	return work
}

func TestSubmissionOwnsOpCopy(t *testing.T) {
	op := WriteOp(nil, []byte("root"), 0).Link(ReadOp(nil, []byte("linked"), 0))
	a, b := prepareForTest(op), prepareForTest(op)
	a.root.vfd = 9
	a.linked[0].vfd = 7
	if op.vfd != 0 || op.followers()[0].vfd != 0 || b.root.vfd != 0 || b.linked[0].vfd != 0 {
		t.Fatal("prepared requests share mutable operands")
	}
	if string(a.root.buf) != "root" || string(a.linked[0].buf) != "linked" {
		t.Fatal("lost operands")
	}
	a.finish()
	b.finish()
}

func TestPreparationRejectsUnsupportedChainLength(t *testing.T) {
	op := Op{}.Link(make([]Op, maxChainLength)...)
	if _, err := copySubmission(&op, maxChainLength); err == nil {
		t.Fatal("accepted a chain larger than internal result storage")
	}
	f := new(os.File)
	op = WriteOp(f, nil, 0).Durable().Link(make([]Op, maxChainLength-1)...)
	if _, err := copySubmission(&op, maxChainLength); err == nil {
		t.Fatal("a non-final Durable write's fdatasync does not count against the limit")
	}
}

func TestPreparationRetainsDurableTail(t *testing.T) {
	for _, op := range []Op{
		WriteOp(new(os.File), nil, 0).Durable(),
		WriteOp(new(os.File), nil, 0).Link(WriteOp(new(os.File), nil, 0).Durable()),
	} {
		work := prepareForTest(op)
		if work.durable() != work.op(work.opCount()-1) {
			t.Fatal("lost durable tail")
		}
	}
}

func TestPreparationSyncsIntermediateDurableWrite(t *testing.T) {
	f := new(os.File)
	for _, link := range []func(Op, ...Op) Op{Op.Link, Op.HardLink} {
		op := link(WriteOp(f, []byte("data"), 0).Durable(), DrainOp(f))
		work := prepareForTest(op)
		if work.opCount() != 3 || len(work.results) != 2 {
			t.Fatalf("ops=%d results=%d, want 3 operations reporting 2 results", work.opCount(), len(work.results))
		}
		write, sync, drain := work.op(0), work.op(1), work.op(2)
		if write.durable() || work.durable() != nil {
			t.Fatal("an intermediate durable write still awaits the batched sync")
		}
		if sync.kind() != OpFdatasync || sync.f != f || sync.opFlags&opWriteSync == 0 || sync.resultIndex != 0 {
			t.Fatal("the synthesized fdatasync does not target and report into the write")
		}
		if sync.sqeFlags != write.sqeFlags || write.sqeFlags != op.sqeFlags {
			t.Fatal("the fdatasync does not keep the write's link type")
		}
		if drain.resultIndex != 1 {
			t.Fatal("the follower's result moved")
		}
		work.recordResult(write, 4, nil)
		work.recordResult(sync, 0, syscall.EIO)
		work.recordResult(drain, 0, nil)
		if work.n != 4 || work.err != syscall.EIO || work.results[0] != (Result{N: 4, Err: syscall.EIO}) {
			t.Fatalf("n=%d err=%v results=%v; a failed sync must fail the write's result", work.n, work.err, work.results)
		}
		work.finish()
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
	if err := writeResultError(&write.operation, 2, nil); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write error: got %v want %v", err, io.ErrShortWrite)
	}
	if err := writeResultError(&write.operation, 4, nil); err != nil {
		t.Fatalf("complete write error: %v", err)
	}
	read := ReadOp(f, make([]byte, 4), 0)
	if err := writeResultError(&read.operation, 2, nil); err != nil {
		t.Fatalf("short read unexpectedly failed: %v", err)
	}
}

func TestTicketWaitsForSubmissionCompletion(t *testing.T) {
	root := Op{}.Link(Op{})
	work := prepareForTest(root)
	ticket := Ticket{&work.completion}
	done := make(chan struct{})
	go func() {
		ticket.Wait()
		close(done)
	}()

	work.recordResult(&work.root, 1, nil)
	select {
	case <-done:
		t.Fatal("Wait returned before the submission completed")
	default:
	}

	work.finish()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after submission completion")
	}
}

func BenchmarkTicketCompletion(b *testing.B) {
	root := Op{}
	work := prepareForTest(root)
	ticket := Ticket{&work.completion}
	work.finish()
	ticket.Wait()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		root := Op{}
		work := prepareForTest(root)
		ticket := Ticket{&work.completion}
		work.finish()
		ticket.Wait()
	}
}

var benchmarkSubmission *submission
var benchmarkTicket Ticket

func BenchmarkSubmissionState(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		root := Op{}
		work := prepareForTest(root)
		ticket := Ticket{&work.completion}
		benchmarkSubmission = work
		work.finish()
		benchmarkTicket = ticket
	}
}

var benchmarkLinkedOp Op

func TestInlineLinksDoNotAllocate(t *testing.T) {
	allocations := testing.AllocsPerRun(100, func() {
		benchmarkLinkedOp = Op{}.Link(Op{}, Op{}, Op{})
	})
	if allocations != 0 {
		t.Fatalf("inline link construction allocated %g times", allocations)
	}
}

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
	work := prepareForTest(root)
	ticket := Ticket{&work.completion}
	done := ticket.Done()
	if done != ticket.Done() {
		t.Fatal("Done returned a different channel on the second call")
	}
	select {
	case <-done:
		t.Fatal("Done closed before the ticket completed")
	default:
	}
	work.recordResult(&work.root, 7, nil)
	work.finish()
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
	work := prepareForTest(root)
	ticket := Ticket{&work.completion}
	work.finish()
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
		work := prepareForTest(root)
		ticket := Ticket{&work.completion}
		go work.finish()
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
		work := prepareForTest(root)
		ticket := Ticket{&work.completion}
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
		work.recordResult(&work.root, 7, nil)
		work.finish()
		workers.Wait()
		close(channels)
		for done := range channels {
			if done != ticket.Done() {
				t.Fatal("Done channel changed")
			}
		}
	}
}
