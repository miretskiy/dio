//go:build linux

package iosched

import (
	"fmt"
	"os"

	"github.com/miretskiy/dio/v2/internal/buildutil"
)

type fileState struct {
	active int32
	refs   int32 // submissions retaining this state until their whole chain completes

	opening     *submission
	openWaiters []*submission

	closing        *submission
	closeRemaining int32
	closeEnds      bool        // this chain leaves the slot closed
	reopen         *submission // next generation, waiting for this close chain

	// syncBatch holds the durable writes on the file whose writes have
	// completed and which wait for an fdatasync not yet placed.
	syncBatch submissionQueue
}

type fileTable struct {
	virtual []*fileState
	regular map[*os.File]*fileState
	free    []*fileState
}

func newFileTable(vfiles uint32) fileTable {
	return fileTable{virtual: make([]*fileState, vfiles)}
}

func (f *fileTable) lookup(op *Op) *fileState {
	if (op.kind() == OpOpenat || op.kind() == OpUnlinkat) && !op.isVirtual() {
		return nil
	}
	if op.isVirtual() {
		return f.virtual[op.vfd]
	}
	return f.regular[op.f]
}

func (f *fileTable) state(op *Op) *fileState {
	if state := f.lookup(op); state != nil {
		return state
	}
	state := f.newState()
	if op.isVirtual() {
		f.virtual[op.vfd] = state
	} else {
		if f.regular == nil {
			f.regular = make(map[*os.File]*fileState)
		}
		f.regular[op.f] = state
	}
	return state
}

func (f *fileTable) newState() *fileState {
	if n := len(f.free); n != 0 {
		state := f.free[n-1]
		f.free = f.free[:n-1]
		return state
	}
	return new(fileState)
}

func (f *fileTable) removeIfEmpty(op *Op, state *fileState) {
	if state.refs != 0 || state.active != 0 || state.opening != nil || state.closing != nil || len(state.openWaiters) != 0 {
		return
	}
	if op.isVirtual() {
		if f.virtual[op.vfd] == state {
			return
		}
	} else {
		delete(f.regular, op.f)
	}
	*state = fileState{openWaiters: state.openWaiters[:0]}
	f.free = append(f.free, state)
}

func isVirtualOpen(op *Op) bool {
	return op.kind() == OpOpenat && op.isVirtual()
}

// fileUse is what one submission does to one file.
type fileUse struct {
	op     *Op        // the submission's first operation on the file, naming it
	ops    int32      // operations a lifecycle barrier waits for: all but open and close
	open   bool       // the submission opens the file into its virtual slot
	close  bool       // the submission closes the slot or drains the file
	closed bool       // its last lifecycle operation is a close
	state  *fileState // the generation bound at acceptance
}

// fileUses appends to uses one fileUse per file work addresses, in the order
// of each file's first operation. Path operations address no open file. A durable write's fdatasync counts as an operation on its file.
func fileUses(work *submission, uses []fileUse) []fileUse {
	for op := &work.root; op != nil; op = op.linked {
		if (op.kind() == OpOpenat || op.kind() == OpUnlinkat) && !op.isVirtual() {
			continue
		}
		i := 0
		for i < len(uses) && !sameFile(uses[i].op, op) {
			i++
		}
		if i == len(uses) {
			uses = append(uses, fileUse{op: op})
		}
		switch {
		case isVirtualOpen(op):
			uses[i].open = true
			uses[i].closed = false
		case op.kind() == OpClose:
			uses[i].close = true
			uses[i].closed = true
		default:
			uses[i].ops++
		}
		if op == work.durable {
			uses[i].ops++ // The sync belongs to this write's file, not the root's.
		}
	}
	return uses
}

// check rejects work whose ordering the scheduler cannot honor: work on a file
// whose lifecycle barrier was accepted earlier, and a virtual open while earlier
// work on the slot remains. It only reads file state, so a rejected submission
// leaves none behind.
//
// A chain that closes a virtual slot and then opens it again is a replacement.
// Its open is accepted while earlier work on the slot remains, because the
// chain's own close drains that work first. While a replacement is pending,
// later plain operations on the slot are accepted and wait for it as for an
// open. An explicit open after a chain that leaves the slot closed queues the
// next generation; it waits for that entire close chain.
func (f *fileTable) check(uses []fileUse) error {
	for _, use := range uses {
		state := f.lookup(use.op)
		if state == nil {
			continue
		}
		// An explicitly queued open after a closing generation starts the next
		// one. Reads/writes without that open remain rejected after close.
		if isVirtualOpen(use.op) && state.closing != nil && state.closeEnds {
			continue
		}
		replacing := state.closing != nil && state.closing == state.opening && !state.closeEnds
		if state.closing != nil && (!replacing || use.open || use.close) {
			return fmt.Errorf("iosched: operation submitted before file lifecycle barrier completed")
		}
		if use.open && (state.opening != nil || (state.active != 0 && !use.close)) {
			return fmt.Errorf("iosched: virtual open submitted before prior slot work completed")
		}
	}
	return nil
}

// admit records, for work that check accepted, the two orderings the scheduler
// provides on each file it uses: work waits for an earlier submission's
// unfinished open, and a close or drain waits for the file's earlier
// operations.
func (c *coordinator) admit(work *submission, uses []fileUse) {
	work.files = uses
	for i := range work.files {
		use := &work.files[i]
		state := c.files.state(use.op)
		if isVirtualOpen(use.op) && state.closing != nil && state.closeEnds {
			state.reopen = work
			work.waitCount++
			state = c.files.newState()
			c.files.virtual[use.op.vfd] = state
		}
		use.state = state
		state.refs++
		if state.opening != nil {
			state.openWaiters = append(state.openWaiters, work)
			work.waitCount++
		}
		if use.open {
			state.opening = work
		}
		if use.close {
			state.closing = work
			state.closeEnds = use.closed
			state.closeRemaining = state.active
			if state.closeRemaining != 0 {
				work.waitCount++
			}
		}
		state.active += use.ops
	}
}

func (work *submission) fileState(op *Op) *fileState {
	for _, use := range work.files {
		if sameFile(use.op, op) {
			return use.state
		}
	}
	return nil
}

func (c *coordinator) completedOperation(work *submission, op *Op) {
	state := work.fileState(op)
	if state == nil {
		return
	}

	switch {
	case isVirtualOpen(op):
		// The whole linked work is the open barrier. Its remaining operations
		// are already ordered after open by io_uring, but unrelated work is not.
	case op.kind() == OpClose:
		// Keep the generation barrier until its whole chain completes. A
		// queued reopen must not overtake followers of this close.
	default:
		state.active--
		if err := buildutil.Assert(state.active >= 0); err != nil {
			panic(err)
		}
		if state.closeRemaining != 0 {
			state.closeRemaining--
			if state.closeRemaining == 0 {
				c.releaseWait(state.closing)
			}
		}
	}
}

func (c *coordinator) completedWork(work *submission) {
	for _, use := range work.files {
		state := use.state
		state.refs--
		if state.opening == work {
			state.opening = nil
			waiters := state.openWaiters
			state.openWaiters = state.openWaiters[:0]
			for _, waiter := range waiters {
				c.releaseWait(waiter)
			}
			clear(waiters)
		}
		if state.closing == work {
			state.closing = nil
			state.closeRemaining = 0
			state.closeEnds = false
			if state.reopen != nil {
				next := state.reopen
				state.reopen = nil
				c.releaseWait(next)
			}
		}
		c.files.removeIfEmpty(use.op, state)
	}
	clear(work.files)
	work.files = nil
}
