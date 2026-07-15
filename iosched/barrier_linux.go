//go:build linux

package iosched

import (
	"fmt"
	"os"

	"github.com/miretskiy/dio/internal/buildutil"
	"github.com/miretskiy/dio/internal/intrusive"
)

type fileState struct {
	active int32

	opening     intrusive.Handle
	openWaiters []intrusive.Handle

	closing        intrusive.Handle
	closeRemaining int32
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
	if op.kind() == OpOpenat && !op.isVirtual() {
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
	var state *fileState
	if n := len(f.free); n != 0 {
		state = f.free[n-1]
		f.free = f.free[:n-1]
	} else {
		state = new(fileState)
	}
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

func (f *fileTable) removeIfEmpty(op *Op, state *fileState) {
	if state.active != 0 || state.opening != 0 || state.closing != 0 || len(state.openWaiters) != 0 {
		return
	}
	if op.isVirtual() {
		return
	}
	delete(f.regular, op.f)
	*state = fileState{openWaiters: state.openWaiters[:0]}
	f.free = append(f.free, state)
}

func isVirtualOpen(op *Op) bool {
	return op.kind() == OpOpenat && op.isVirtual()
}

func isFileOperation(op *Op) bool {
	return op.kind() != OpOpenat && op.kind() != OpClose
}

// admit records the two ordering conditions the scheduler provides: virtual
// file operations wait for work containing an unfinished open, and close waits
// for older file operations to complete.
func (f *fileTable) admit(c *coordinator, handle intrusive.Handle) error {
	work := c.works.Value(handle)

	// Work submitted after close, and an open while prior slot work remains,
	// have no useful ordering contract. Reject them before changing file state.
	for op := work.root; op != nil; op = op.linked {
		state := f.lookup(op)
		if state == nil {
			continue
		}
		if state.closing != 0 {
			return fmt.Errorf("iosched: operation submitted before close completed")
		}
		if isVirtualOpen(op) && (state.opening != 0 || state.active != 0) {
			return fmt.Errorf("iosched: virtual open submitted before prior slot work completed")
		}
	}

	// Lifecycle waits are computed before this work's ordinary operations are
	// counted. A close in a linked chain therefore waits only for outside work;
	// the kernel link orders operations within the chain.
	for op := work.root; op != nil; op = op.linked {
		if op.kind() == OpOpenat && !op.isVirtual() {
			continue
		}
		state := f.state(op)
		if state.opening != 0 && state.opening != handle {
			state.openWaiters = append(state.openWaiters, handle)
			work.waitCount++
		}
		switch {
		case isVirtualOpen(op):
			if state.opening == 0 {
				state.opening = handle
			}
		case op.kind() == OpClose:
			if state.closing == handle {
				continue
			}
			state.closing = handle
			state.closeRemaining = state.active
			if state.closeRemaining != 0 {
				work.waitCount++
			}
		}
	}

	for op := work.root; op != nil; op = op.linked {
		if isFileOperation(op) {
			f.state(op).active++
		}
	}
	return nil
}

func (f *fileTable) completedOperation(c *coordinator, handle intrusive.Handle, op *Op) {
	state := f.lookup(op)
	if state == nil {
		return
	}

	switch {
	case isVirtualOpen(op):
		// The whole linked work is the open barrier. Its remaining operations
		// are already ordered after open by io_uring, but unrelated work is not.
	case op.kind() == OpClose:
		if state.closing == handle {
			state.closing = 0
			state.closeRemaining = 0
		}
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
	f.removeIfEmpty(op, state)
}

func (f *fileTable) completedWork(c *coordinator, handle intrusive.Handle, root *Op) {
	for op := root; op != nil; op = op.linked {
		if !isVirtualOpen(op) {
			continue
		}
		state := f.lookup(op)
		if state == nil || state.opening != handle {
			continue
		}
		state.opening = 0
		waiters := state.openWaiters
		state.openWaiters = nil
		for _, waiter := range waiters {
			c.releaseWait(waiter)
		}
	}
}
