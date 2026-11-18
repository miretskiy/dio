package iosched

import (
	"errors"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

var benchmarkLinkedOp Op

func TestLinkAllocatesOnce(t *testing.T) {
	allocations := testing.AllocsPerRun(100, func() {
		benchmarkLinkedOp = Op{}.Link(Op{}, Op{}, Op{})
	})
	if allocations != 1 {
		t.Fatalf("link construction allocated %g times, want one follower slice", allocations)
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
// must run exactly once, before Wait returns (run with -race).
func TestWhenDoneRacesWait(t *testing.T) {
	for range 1000 {
		calls := 0 // Plain: Wait must order the callback's write before the read.
		root := Op{whenDone: func(int, error) { calls++ }}
		work := prepareForTest(root)
		ticket := Ticket{&work.completion}
		go work.finish()
		ticket.Wait()
		if calls != 1 {
			t.Fatalf("whenDone ran %d times before Wait returned", calls)
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

func TestResultsOnShutdown(t *testing.T) {
	for _, unplaced := range []bool{false, true} {
		root := WriteOp(nil, nil, 0).Link(ReadOp(nil, nil, 0))
		work := prepareForTest(root)
		ticket := Ticket{&work.completion}
		if unplaced {
			work.fail(syscall.EIO)
		} else {
			work.recordResult(&work.root, 3, nil)
			work.abandon(syscall.EIO)
		}
		_, err := ticket.Wait()
		require.ErrorIs(t, err, syscall.EIO)
		results := ticket.AllResults()
		if unplaced {
			require.ErrorIs(t, results[0].Err, syscall.EIO)
		} else {
			require.Equal(t, Result{N: 3}, results[0])
		}
		require.ErrorIs(t, results[1].Err, syscall.EIO)
	}
}

var resultStorageTicket Ticket

var allResultsSink []Result

func TestAllResultsAllocatesOnlyForStandalone(t *testing.T) {
	for _, count := range []int32{1, 2} {
		op := Op{}
		if count > 1 {
			op = op.Link(Op{})
		}
		work := prepareForTest(op)
		ticket := Ticket{&work.completion}
		ticket.completion.finish()
		_, err := ticket.Wait()
		require.NoError(t, err)
		allocs := testing.AllocsPerRun(100, func() { allResultsSink = ticket.AllResults() })
		want := float64(0)
		if count == 1 {
			want = 1
		}
		require.Equal(t, want, allocs)
	}
	allResultsSink = nil
}

func TestUnknownResultsPreserveOutOfOrderCompletions(t *testing.T) {
	root := ReadOp(nil, nil, 0).Link(ReadOp(nil, nil, 0), ReadOp(nil, nil, 0))
	work := prepareForTest(root)
	ticket := Ticket{&work.completion}
	work.recordResult(work.op(2), 7, nil)
	work.abandon(syscall.EIO)
	_, err := ticket.Wait()
	require.ErrorIs(t, err, syscall.EIO)
	results := ticket.AllResults()
	require.ErrorIs(t, results[0].Err, syscall.EIO)
	require.ErrorIs(t, results[1].Err, syscall.EIO)
	require.Equal(t, Result{N: 7}, results[2])
}
