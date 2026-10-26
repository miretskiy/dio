package iosched

import (
	"fmt"
	"time"
)

const maxChainLength = 8

// submission owns mutable copies of the operation records extracted from Op,
// plus completion, admission, and scheduling state. Op itself is needed only
// during preparation. The separate layout keeps Op's inline follower array out
// of standalone submissions.
type submission struct {
	completion
	root          operation   // copy of Op.operation
	linked        []operation // private copies of Op.followers() and synthesized fdatasyncs
	reads, writes ioCharge    // request totals until coordinator acceptance; accounted charges afterward
	whenDone      func(int, error)
	resultDone    uint8 // bit per results entry recorded
	// staged links the staging stack until acceptance, then a coalesced run's
	// members from its leader until the run completes.
	staged *submission

	state      workState
	waitCount  int32
	remaining  int32
	sequence   uint64
	files      []fileUse
	inlineFile [1]fileUse
	links      [2]queueLinks
}

type workState uint8

const (
	workWaiting workState = iota
	workReady
	workIssued
	workDone
)

func (o *operation) isWrite() bool { return o.kind() == OpWrite || o.kind() == OpWritev }

// copySubmission owns copies of op's operation records and completion storage.
// The buffers and files referenced by those records remain caller-owned.
//
// A Durable write that is not the chain's final operation is followed by a
// synthesized fdatasync linked the way the write links to its successor, so
// later operations run after the flush. It counts against limit and reports
// into the write's result. A final Durable write keeps the shared, batched sync.
func copySubmission(op *Op, limit int) (*submission, error) {
	followers := op.followers()
	user := 1 + len(followers)
	userOp := func(i int) *operation {
		if i == 0 {
			return &op.operation
		}
		return &followers[i-1]
	}
	count := user
	for i := range user - 1 {
		if src := userOp(i); src.durable() && src.isWrite() {
			count++
		}
	}
	if count > limit {
		return nil, fmt.Errorf("iosched: linked chain of %d operations, including one fdatasync per non-final Durable write, exceeds the limit of %d", count, limit)
	}
	var work *submission
	if count == 1 {
		work = new(submission)
	} else {
		chain := new(struct {
			submission
			operands [maxChainLength - 1]operation
			results  [maxChainLength]Result
		})
		work = &chain.submission
		work.linked = chain.operands[:count-1]
		work.results = chain.results[:user]
	}
	*work = submission{
		completion: completion{started: time.Now(), results: work.results},
		linked:     work.linked,
		remaining:  int32(count),
		whenDone:   op.whenDone,
	}
	next := 0
	for i := range user {
		src := userOp(i)
		dst := work.op(next)
		*dst = *src
		dst.resultIndex = uint8(i)
		next++
		if i < user-1 && src.durable() && src.isWrite() {
			dst.opFlags &^= opDurable
			sync := src.syncOp()
			sync.sqeFlags = src.sqeFlags
			sync.resultIndex = uint8(i)
			sync.opFlags |= opWriteSync
			*work.op(next) = sync
			next++
		}
	}
	if work.durable() != nil {
		work.remaining++
	}
	work.wait.Add(1)
	return work, nil
}

func (work *submission) opCount() int { return 1 + len(work.linked) }

func (work *submission) op(i int) *operation {
	if i == 0 {
		return &work.root
	}
	return &work.linked[i-1]
}

// durable returns the final write when it awaits a shared fdatasync, or nil.
func (work *submission) durable() *operation {
	last := work.op(work.opCount() - 1)
	if last.durable() && last.isWrite() {
		return last
	}
	return nil
}

func (work *submission) finish() {
	work.completion.finish()
	if work.whenDone != nil {
		callback := work.whenDone
		work.whenDone = nil
		callback(work.n, work.err)
	}
}

func (work *submission) recordResult(op *operation, n int, err error) {
	if op.opFlags&opWriteSync != 0 {
		// A synthesized fdatasync reports only its error, into its write's result.
		if err != nil && work.results != nil && work.results[op.resultIndex].Err == nil {
			work.results[op.resultIndex].Err = err
		}
	} else {
		if work.results != nil {
			work.results[op.resultIndex] = Result{N: n, Err: err}
			work.resultDone |= 1 << op.resultIndex
		}
		if op == &work.root {
			work.n = n
		}
	}
	if err != nil && work.err == nil {
		work.err = err
	}
}

// fail completes a submission that can no longer run. Preserve reported
// results; fill in missing outcomes and a durable write still awaiting its sync.
func (work *submission) fail(err error) {
	if work.err == nil {
		work.err = err
	}
	durable := work.durable() != nil
	for i := range work.results {
		if work.resultDone&(1<<i) == 0 || (durable && i == len(work.results)-1) {
			work.results[i].Err = err
		}
	}
	work.finish()
}

// abandon is used when shutdown could not reap the submitted kernel I/O.
// Report that unknown outcome even if an earlier operation failed, and suppress
// the callback: it may release buffers the kernel is still accessing.
func (work *submission) abandon(err error) {
	work.err = err
	work.whenDone = nil
	work.fail(err)
}

type fileState struct {
	active int32
	refs   int32 // submissions retaining this state until their whole chain completes

	closing        *submission
	closeRemaining int32

	// syncBatch holds the durable writes on the file whose writes have
	// completed and which wait for an fdatasync not yet placed.
	syncBatch submissionQueue
}

// fileUse is what one submission does to one file.
type fileUse struct {
	op    *operation // the submission's first operation on the file, naming it
	ops   int32      // operations a lifecycle barrier waits for: all but open and close
	close bool       // the submission drains the regular file
	state *fileState // file state bound at acceptance
}

// queueLinks are a submission's neighbors on one submissionQueue.
type queueLinks struct {
	prev, next *submission
}

// Which of a submission's links a submissionQueue uses.
const (
	// placementLinks link work waiting to be placed: the ready queue, or a
	// file's sync batch waiting for its fdatasync. Work is on at most one.
	placementLinks = iota
	// acceptedLinks link the accepted queue, which holds all accepted work.
	acceptedLinks
)

// submissionQueue is a FIFO of submissions, linked through their links[links].
// Its zero value is an empty queue on placementLinks.
type submissionQueue struct {
	head, tail *submission
	len        int
	links      int
}

func (q *submissionQueue) push(work *submission) {
	link := &work.links[q.links]
	link.prev = q.tail
	if q.tail == nil {
		q.head = work
	} else {
		q.tail.links[q.links].next = work
	}
	q.tail = work
	q.len++
}

// next returns the submission after work, or nil.
func (q *submissionQueue) next(work *submission) *submission {
	return work.links[q.links].next
}

func (q *submissionQueue) remove(work *submission) {
	link := &work.links[q.links]
	if link.prev == nil {
		q.head = link.next
	} else {
		link.prev.links[q.links].next = link.next
	}
	if link.next == nil {
		q.tail = link.prev
	} else {
		link.next.links[q.links].prev = link.prev
	}
	*link = queueLinks{}
	q.len--
}

func (q *submissionQueue) pop() *submission {
	work := q.head
	q.remove(work)
	return work
}
