//go:build linux

package iosched

import (
	"errors"
	"os"
	"sync"
	"syscall"
	"testing"

	"github.com/miretskiy/dio/giouring"
	"github.com/miretskiy/dio/internal/intrusive"
)

type fakeRingQueue struct {
	sqes      []giouring.SubmissionQueueEntry
	cqes      []giouring.CompletionQueueEvent
	submitErr error
}

func (f *fakeRingQueue) GetSQE() *giouring.SubmissionQueueEntry {
	f.sqes = append(f.sqes, giouring.SubmissionQueueEntry{})
	return &f.sqes[len(f.sqes)-1]
}

func (f *fakeRingQueue) SubmitAndWait(uint32) (uint, error) {
	return uint(len(f.sqes)), f.submitErr
}

func (f *fakeRingQueue) ForEachCQE(fn func(*giouring.CompletionQueueEvent)) {
	for i := range f.cqes {
		fn(&f.cqes[i])
	}
}

func (f *fakeRingQueue) CQAdvance(count uint32) {
	f.cqes = f.cqes[count:]
}

func (f *fakeRingQueue) complete(slot uint64, result int32) {
	f.cqes = append(f.cqes, giouring.CompletionQueueEvent{UserData: slot, Res: result})
}

func newTestCoordinator(depth int, vfiles uint32, ring ringQueue) coordinator {
	c := coordinator{
		sched: &URingScheduler{URingConfig: URingConfig{RingDepth: uint32(depth), VFiles: vfiles}},
		ring:  ring,
		slots: intrusive.MakeFixedList[ringSlot](depth),
		files: newFileTable(vfiles),
	}
	return c
}

func acceptOps(c *coordinator, ops ...Op) ([]Ticket, []intrusive.Handle) {
	tickets := make([]Ticket, len(ops))
	var head, tail *Op
	for i := range ops {
		root := &ops[i]
		tickets[i] = root.prepareSubmission()
		if head == nil {
			head = root
		} else {
			tail.staged = root
		}
		tail = root
	}
	c.accept(head)
	handles := make([]intrusive.Handle, 0, len(ops))
	for handle, ok := c.works.Front(); ok; handle, ok = c.works.Next(handle) {
		handles = append(handles, handle)
	}
	return tickets, handles
}

func TestStagingClosePartitionsConcurrentPushes(t *testing.T) {
	const count = 256
	stopErr := errors.New("stopped")
	s := URingScheduler{wakeup: make(chan struct{}, 1)}
	ops := make([]Op, count)
	accepted := make([]bool, count)

	accepted[0] = s.tryPush(&ops[0])
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 1; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			accepted[i] = s.tryPush(&ops[i])
		}()
	}
	close(start)
	s.stop(stopErr)
	batch := s.closeStaging()
	wg.Wait()

	seen := make(map[*Op]bool, count)
	for op := batch; op != nil; op = op.staged {
		if seen[op] {
			t.Fatal("staged operation appeared more than once")
		}
		seen[op] = true
	}
	for i := range ops {
		if accepted[i] != seen[&ops[i]] {
			t.Fatalf("operation %d: accepted=%v staged=%v", i, accepted[i], seen[&ops[i]])
		}
	}
	if s.tryPush(new(Op)) {
		t.Fatal("push succeeded after staging closed")
	}
	if !errors.Is(s.fatalError(), stopErr) {
		t.Fatalf("fatal error: got %v want %v", s.fatalError(), stopErr)
	}
}

func completeForTest(c *coordinator, handle intrusive.Handle, index uint32, n int, err error) {
	work := c.works.Value(handle)
	if work.ready != 0 {
		c.ready.Remove(work.ready)
		work.ready = 0
		work.inflight = true
	}
	op := work.root
	for i := uint32(0); i < index; i++ {
		op = op.linked
	}
	c.finishOperation(handle, op, n, err)
}

func TestCoordinatorFailAllWorkPreservesCompletedRoot(t *testing.T) {
	err := errors.New("ring failed")
	c := newTestCoordinator(2, 1, &fakeRingQueue{})
	tickets, handles := acceptOps(&c, VReadOp(0, nil, 0).Link(VReadOp(0, nil, 0)))
	ticket := tickets[0]

	completeForTest(&c, handles[0], 0, 1, nil)
	c.stopping = true
	c.failAllWork(err)
	ticket.Wait()

	if ticket.N() != 1 {
		t.Fatalf("completed root count changed: %d", ticket.N())
	}
	if !errors.Is(ticket.Error(), err) {
		t.Fatalf("ticket error: got %v want %v", ticket.Error(), err)
	}
}

func TestDrainFailureCompletesInflightTicket(t *testing.T) {
	err := errors.New("ring failed")
	ring := &fakeRingQueue{submitErr: err}
	c := newTestCoordinator(1, 1, ring)
	tickets, _ := acceptOps(&c, VReadOp(0, make([]byte, 1), 0))
	ticket := tickets[0]
	c.placeReady(false)
	c.stopping = true
	c.drainInflight()

	ticket.Wait()
	if !errors.Is(ticket.Error(), err) {
		t.Fatalf("ticket error: got %v want %v", ticket.Error(), err)
	}
	if c.works.Len() != 0 || c.slots.Len() != 0 {
		t.Fatalf("coordinator retained failed work: works=%d inflight=%d", c.works.Len(), c.slots.Len())
	}
}

func TestFileDependenciesOpenBlocksLaterRead(t *testing.T) {
	c := newTestCoordinator(4, 2, &fakeRingQueue{})
	_, handles := acceptOps(&c,
		VOpenatOp(0, "file", 0, 0, 1),
		VReadOp(1, make([]byte, 1), 0),
	)

	if c.works.Value(handles[1]).waitCount != 1 || c.works.Value(handles[1]).ready != 0 {
		t.Fatal("read did not wait for open")
	}
	completeForTest(&c, handles[0], 0, 0, nil)
	if c.works.Value(handles[1]).ready == 0 {
		t.Fatal("read did not become ready at open completion")
	}
	completeForTest(&c, handles[1], 0, 1, nil)
}

func TestFileDependenciesCloseDrainsOlderRead(t *testing.T) {
	c := newTestCoordinator(4, 2, &fakeRingQueue{})
	_, handles := acceptOps(&c,
		VReadOp(1, make([]byte, 1), 0),
		VCloseOp(1),
	)

	if c.works.Value(handles[1]).waitCount != 1 {
		t.Fatal("close did not wait for older read")
	}
	completeForTest(&c, handles[0], 0, 1, nil)
	if c.works.Value(handles[1]).ready == 0 {
		t.Fatal("close did not become ready after read")
	}
	completeForTest(&c, handles[1], 0, 0, nil)
}

func TestFileDependenciesCloseAtEndOfChainDrainsOnlyOlderWork(t *testing.T) {
	c := newTestCoordinator(4, 1, &fakeRingQueue{})
	_, handles := acceptOps(&c,
		VReadOp(0, make([]byte, 1), 0),
		VWriteOp(0, make([]byte, 1), 1).Link(
			VWriteOp(0, make([]byte, 1), 2),
			VCloseOp(0),
		),
	)

	chain := c.works.Value(handles[1])
	if chain.waitCount != 1 || chain.ready != 0 {
		t.Fatal("close chain did not wait exactly for older work")
	}
	if got := c.files.virtual[0].closeRemaining; got != 1 {
		t.Fatalf("close drain counted its own chain: got %d older operations want 1", got)
	}

	lateRoot := VReadOp(0, make([]byte, 1), 3)
	late := lateRoot.prepareSubmission()
	c.accept(&lateRoot)
	late.Wait()
	if late.Error() == nil {
		t.Fatal("work submitted behind a close at the end of a chain was accepted")
	}

	completeForTest(&c, handles[0], 0, 1, nil)
	if c.works.Value(handles[1]).ready == 0 {
		t.Fatal("close chain did not become ready after older work completed")
	}
	completeForTest(&c, handles[1], 0, 1, nil)
	completeForTest(&c, handles[1], 1, 1, nil)
	completeForTest(&c, handles[1], 2, 0, nil)
}

func TestFileDependenciesRejectWorkBehindClose(t *testing.T) {
	c := newTestCoordinator(4, 1, &fakeRingQueue{})
	tickets, handles := acceptOps(&c,
		VCloseOp(0),
		VReadOp(0, make([]byte, 1), 0),
	)

	if len(handles) != 1 {
		t.Fatalf("accepted work: got %d want 1", len(handles))
	}
	tickets[1].Wait()
	if tickets[1].Error() == nil {
		t.Fatal("work submitted behind close was accepted")
	}
	completeForTest(&c, handles[0], 0, 0, nil)
}

func TestDeferredMultiFileWorkIsKnownAtAdmission(t *testing.T) {
	c := newTestCoordinator(8, 2, &fakeRingQueue{})
	_, handles := acceptOps(&c,
		VOpenatOp(0, "a", 0, 0, 0),
		VReadOp(0, make([]byte, 1), 0).Link(VWriteOp(1, make([]byte, 1), 0)),
		VCloseOp(1),
	)

	if c.works.Value(handles[1]).waitCount != 1 {
		t.Fatal("linked work did not wait for open")
	}
	if c.works.Value(handles[2]).waitCount != 1 {
		t.Fatal("close overtook older blocked write")
	}
	completeForTest(&c, handles[0], 0, 0, nil)
	completeForTest(&c, handles[1], 0, 1, nil)
	if c.works.Value(handles[2]).ready != 0 {
		t.Fatal("close became ready before the linked write completed")
	}
	completeForTest(&c, handles[1], 1, 1, nil)
	completeForTest(&c, handles[2], 0, 0, nil)
}

func TestOpenBarrierWaitsForWholeLinkedChain(t *testing.T) {
	c := newTestCoordinator(4, 2, &fakeRingQueue{})
	_, handles := acceptOps(&c,
		VOpenatOp(0, "a", 0, 0, 0).Link(
			VFallocateOp(0, 4096),
			VReadOp(1, make([]byte, 1), 0),
		),
		VReadOp(0, make([]byte, 1), 0),
	)

	completeForTest(&c, handles[0], 0, 0, nil)
	if c.works.Value(handles[1]).ready != 0 {
		t.Fatal("open dependent escaped after only the open completed")
	}
	completeForTest(&c, handles[0], 1, 0, nil)
	if c.works.Value(handles[1]).ready != 0 {
		t.Fatal("open dependent escaped before the whole linked chain completed")
	}
	completeForTest(&c, handles[0], 2, 1, nil)
	if c.works.Value(handles[1]).ready == 0 {
		t.Fatal("open dependent did not become ready with the linked chain")
	}
	completeForTest(&c, handles[1], 0, 1, nil)
}

func TestFailedOpenRetryWaitsForReleasedSlotWork(t *testing.T) {
	c := newTestCoordinator(4, 1, &fakeRingQueue{})
	_, handles := acceptOps(&c,
		VOpenatOp(0, "a", 0, 0, 0),
		VWriteOp(0, make([]byte, 1), 0),
	)

	completeForTest(&c, handles[0], 0, 0, syscall.ENOENT)
	if c.works.Value(handles[1]).ready == 0 {
		t.Fatal("failed open did not release waiting write")
	}

	retryRoot := VOpenatOp(0, "b", 0, 0, 0)
	retry := retryRoot.prepareSubmission()
	c.accept(&retryRoot)
	retry.Wait()
	if retry.Error() == nil {
		t.Fatal("retry open was accepted while prior slot work remained")
	}

	completeForTest(&c, handles[1], 0, 0, syscall.EBADF)
	finalRoot := VOpenatOp(0, "b", 0, 0, 0)
	final := finalRoot.prepareSubmission()
	c.accept(&finalRoot)
	if c.works.Len() != 1 {
		t.Fatal("retry open was not accepted after prior slot work completed")
	}
	handle, _ := c.works.Front()
	completeForTest(&c, handle, 0, 0, nil)
	final.Wait()
	if final.Error() != nil {
		t.Fatalf("final open failed: %v", final.Error())
	}
}

func TestWriteGroupPreservesCountsOnSyncError(t *testing.T) {
	syncErr := errors.New("fdatasync failed")
	c := newTestCoordinator(4, 1, &fakeRingQueue{})
	tickets, handles := acceptOps(&c,
		VWriteOp(0, make([]byte, 4), 0),
		VWriteOp(0, make([]byte, 4), 4),
	)
	for _, handle := range handles {
		work := c.works.Value(handle)
		c.ready.Remove(work.ready)
		work.ready = 0
		work.inflight = true
	}
	c.works.Value(handles[0]).write = &writeCompletion{targets: []writeTarget{
		{work: handles[0], bytes: 4},
		{work: handles[1], bytes: 4},
	}}
	c.finishWrite(handles[0], 8, nil, syncErr)

	for _, ticket := range tickets {
		ticket.Wait()
		if ticket.N() != 4 || !errors.Is(ticket.Error(), syncErr) {
			t.Fatalf("result: got N=%d error=%v, want N=4 error=%v", ticket.N(), ticket.Error(), syncErr)
		}
	}
}

func TestDurableWritePreservesCountsOnRingFailureAfterWrite(t *testing.T) {
	ringErr := errors.New("ring failed")
	ring := &fakeRingQueue{}
	c := newTestCoordinator(2, 1, ring)
	tickets, _ := acceptOps(&c,
		VWriteOp(0, make([]byte, 4), 0).Durable(),
	)
	c.placeReady(true)

	writeSlot := ring.sqes[0].UserData
	ring.complete(writeSlot, 4)
	c.reap()
	c.stopping = true
	c.failAllWork(ringErr)
	c.failAllInflight(ringErr)

	ticket := tickets[0]
	ticket.Wait()
	if ticket.N() != 4 || !errors.Is(ticket.Error(), ringErr) {
		t.Fatalf("result: got N=%d error=%v, want N=4 error=%v", ticket.N(), ticket.Error(), ringErr)
	}
}

func TestFileTableReusesRegularState(t *testing.T) {
	var files fileTable
	firstFile := new(os.File)
	firstOp := ReadOp(firstFile, nil, 0)
	first := files.state(&firstOp)
	files.removeIfEmpty(&firstOp, first)

	secondFile := new(os.File)
	secondOp := ReadOp(secondFile, nil, 0)
	second := files.state(&secondOp)
	if second != first {
		t.Fatal("regular file state was not reused")
	}
}

func TestReleaseSlotClearsPointersAndReusesStorage(t *testing.T) {
	c := newTestCoordinator(1, 0, &fakeRingQueue{})
	buf := make([]byte, 8)
	handle := c.slots.PushBack()
	rs := c.slots.Value(handle)
	*rs = ringSlot{
		op:       &Op{buf: buf},
		complete: completeNormal,
		iovecs:   []syscall.Iovec{{Base: &buf[0], Len: 8}},
	}
	iovecCap := cap(rs.iovecs)
	c.releaseSlot(handle)

	if rs.op != nil || rs.complete != nil {
		t.Fatal("released slot retained request")
	}
	for i, iv := range rs.iovecs[:cap(rs.iovecs)] {
		if iv.Base != nil {
			t.Fatalf("iovec[%d].Base not cleared", i)
		}
	}
	if cap(rs.iovecs) != iovecCap {
		t.Fatal("released slot dropped reusable iovec storage")
	}
	if c.slots.Len() != 0 {
		t.Fatalf("released slot remains occupied: %d", c.slots.Len())
	}
}

func TestCoordinatorOpenBarrierWithAdversarialCompletions(t *testing.T) {
	ring := &fakeRingQueue{}
	c := newTestCoordinator(4, 2, ring)
	tickets, handles := acceptOps(&c,
		VOpenatOp(0, "file", 0, 0, 0),
		VReadOp(1, make([]byte, 1), 0),
	)
	c.placeReady(false)
	if len(ring.sqes) != 2 {
		t.Fatalf("SQEs before open completion: got %d want 2", len(ring.sqes))
	}

	// Accept the same-slot read only after the open has already been handed to
	// the ring. Its SQE still cannot be prepared before the open CQE arrives.
	readTickets, allHandles := acceptOps(&c, VReadOp(0, make([]byte, 1), 0))
	tickets = append(tickets, readTickets...)
	handles = allHandles
	c.placeReady(false)
	if len(ring.sqes) != 2 {
		t.Fatal("same-slot read reached the ring while the open was in flight")
	}

	var openSlot, otherSlot uint64
	foundOpen, foundOther := false, false
	for i := range ring.sqes {
		sqe := &ring.sqes[i]
		switch {
		case sqe.OpCode == giouring.OpOpenat:
			openSlot = sqe.UserData
			foundOpen = true
		case sqe.OpCode == giouring.OpRead && sqe.Fd == 1:
			otherSlot = sqe.UserData
			foundOther = true
		case sqe.OpCode == giouring.OpRead && sqe.Fd == 0:
			t.Fatal("same-slot read reached the ring before open completed")
		}
	}
	if !foundOpen || !foundOther {
		t.Fatalf("missing initial SQEs: open=%v other=%v sqes=%+v", foundOpen, foundOther, ring.sqes)
	}
	if openSlot == otherSlot {
		t.Fatalf("initial SQEs reused slot %d: %+v; ring slots=%+v", openSlot, ring.sqes, c.slots)
	}
	openRingSlot := c.slots.Value(intrusive.Handle(openSlot))
	otherRingSlot := c.slots.Value(intrusive.Handle(otherSlot))
	if openRingSlot.work != handles[0] || otherRingSlot.work != handles[1] {
		t.Fatalf("wrong work in slots: open=%d other=%d handles=%v", openRingSlot.work, otherRingSlot.work, handles)
	}

	ring.complete(otherSlot, 1)
	c.reap()
	c.placeReady(false)
	if len(ring.sqes) != 2 {
		t.Fatal("same-slot read escaped while open completion was withheld")
	}

	ring.complete(openSlot, 0)
	c.reap()
	c.placeReady(false)
	if len(ring.sqes) != 3 {
		t.Fatalf("read not placed after open completion: sqes=%d", len(ring.sqes))
	}
	last := ring.sqes[len(ring.sqes)-1]
	if last.OpCode != giouring.OpRead || last.Fd != 0 {
		t.Fatalf("last SQE = {opcode:%d fd:%d}, want same-slot read", last.OpCode, last.Fd)
	}

	c.stopping = true
	c.failAllWork(errors.New("test cleanup"))
	c.failAllInflight(errors.New("test cleanup"))
	for _, ticket := range tickets {
		ticket.Wait()
	}
}

func TestCoordinatorCloseDrainWithAdversarialCompletions(t *testing.T) {
	ring := &fakeRingQueue{}
	c := newTestCoordinator(4, 2, ring)
	tickets, _ := acceptOps(&c,
		VReadOp(0, make([]byte, 1), 0),
		VReadOp(1, make([]byte, 1), 0),
	)
	c.placeReady(false)

	closeTickets, _ := acceptOps(&c, VCloseOp(0))
	tickets = append(tickets, closeTickets...)
	c.placeReady(false)
	if len(ring.sqes) != 2 {
		t.Fatal("close reached the ring before the older same-slot read completed")
	}

	var sameSlot, otherSlot uint64
	for i := range ring.sqes {
		sqe := &ring.sqes[i]
		if sqe.Fd == 0 {
			sameSlot = sqe.UserData
		} else if sqe.Fd == 1 {
			otherSlot = sqe.UserData
		}
	}
	if sameSlot == 0 || otherSlot == 0 {
		t.Fatalf("missing read SQEs: %+v", ring.sqes)
	}

	ring.complete(otherSlot, 1)
	c.reap()
	c.placeReady(false)
	if len(ring.sqes) != 2 {
		t.Fatal("unrelated completion released the close")
	}

	ring.complete(sameSlot, 1)
	c.reap()
	c.placeReady(false)
	if len(ring.sqes) != 3 || ring.sqes[2].OpCode != giouring.OpClose {
		t.Fatalf("close was not placed after the same-slot read: %+v", ring.sqes)
	}

	c.stopping = true
	c.failAllWork(errors.New("test cleanup"))
	c.failAllInflight(errors.New("test cleanup"))
	for _, ticket := range tickets {
		ticket.Wait()
	}
}
