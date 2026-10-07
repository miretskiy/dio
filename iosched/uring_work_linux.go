package iosched

import "github.com/miretskiy/dio/v2/internal/buildutil"

// submission is one validated Submit call. Submit builds it and pushes it onto
// the staging stack; once the coordinator accepts it, it is the coordinator's
// record of that work until its ticket completes. Only the coordinator touches
// the fields after staged.
type submission struct {
	root       Op          // scheduler-owned copy of the submitted operation chain
	completion completion  // result state shared with the returned Ticket
	count      int32       // validated number of operations in root's chain
	staged     *submission // next item in the lock-free staging stack

	state workState
	// waitCount counts the lifecycle barriers still holding the work.
	waitCount int32
	// remaining counts the completions the work still owes: one per operation,
	// and for a durable write one more for the fdatasync that covers it.
	remaining int32
	// sequence is the acceptance order; coalescing joins only consecutive work.
	sequence uint64
	// durable marks a standalone durable write. Its ticket completes only once
	// an fdatasync placed after its write completed has completed too.
	durable bool
	// class and cost are what the work's first operation charges against the
	// in-flight budget while it runs; charged reports that it is charged now.
	class   ioClass
	cost    int64
	charged bool

	// links place the submission on up to two queues at once: the accepted
	// queue, and either the ready queue or a file's sync batch. A queue clears
	// a submission's links when it leaves, so a completed submission, which its
	// Ticket keeps reachable, does not keep other submissions reachable too.
	links [2]queueLinks

	// coalesced lists the members of the coalesced run of writes this work
	// leads, itself first, in writev order, until the run's write completes.
	coalesced []*submission
}

// workState is where accepted work is on its way to completion.
type workState uint8

const (
	// workWaiting work is held by a lifecycle barrier.
	workWaiting workState = iota
	// workReady work is in the ready queue, eligible for placement.
	workReady
	// workIssued work has been placed and owes completions: operations in the
	// ring, or a durable write waiting for its fdatasync.
	workIssued
	// workDone work has completed its ticket.
	workDone
)

func newSubmission(op Op, count int32) (*submission, Ticket) {
	request := &submission{root: op, count: count}
	request.root.completion = &request.completion
	request.completion.done.Add(1)
	return request, Ticket{&request.completion}
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

// makeReady moves work whose barriers have all released into the ready queue.
func (c *coordinator) makeReady(work *submission) {
	if err := buildutil.Assert(work.state == workWaiting); err != nil {
		panic(err)
	}
	work.state = workReady
	c.ready.push(work)
}
