//go:build linux

package iosched

import (
	"fmt"
	"os"

	"github.com/miretskiy/dio/v2/internal/buildutil"
)

type fileTable struct {
	virtual []*fileState
	regular map[*os.File]*fileState
	free    []*fileState
}

func newFileTable(vfiles uint32) fileTable {
	return fileTable{virtual: make([]*fileState, vfiles)}
}

func (f *fileTable) lookup(op *operation) *fileState {
	if (op.kind() == OpOpenat || op.kind() == OpUnlinkat) && !op.isVirtual() {
		return nil
	}
	if op.isVirtual() {
		return f.virtual[op.vfd]
	}
	return f.regular[op.f]
}

func (f *fileTable) state(op *operation) *fileState {
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

func (f *fileTable) removeIfEmpty(op *operation, state *fileState) {
	if op.isVirtual() || state.refs != 0 || state.active != 0 || state.closing != nil {
		return
	}
	delete(f.regular, op.f)
	*state = fileState{}
	f.free = append(f.free, state)
}

// fileUses appends to uses one fileUse per file work addresses, in the order
// of each file's first operation. Path operations address no open file. A durable write's fdatasync counts as an operation on its file.
func fileUses(work *submission, uses []fileUse) []fileUse {
	for i := range work.opCount() {
		op := work.op(i)
		if op.kind() == OpOpenat || op.kind() == OpUnlinkat {
			continue
		}
		if op.isVirtual() && (op.kind() == OpClose || op.vfd == AllocateVFD) {
			continue
		}
		i := 0
		for i < len(uses) && !sameFile(uses[i].op, op) {
			i++
		}
		if i == len(uses) {
			uses = append(uses, fileUse{op: op})
		}
		if op.kind() == OpClose {
			uses[i].close = true
		} else {
			uses[i].ops++
		}
		if op == work.durable() {
			uses[i].ops++ // The sync belongs to this write's file, not the root's.
		}
	}
	return uses
}

// check rejects regular-file work submitted after a pending DrainOp.
func (f *fileTable) check(uses []fileUse) error {
	for _, use := range uses {
		if state := f.lookup(use.op); state != nil && state.closing != nil {
			return fmt.Errorf("iosched: operation submitted before file lifecycle barrier completed")
		}
	}
	return nil
}

// admit accounts for file operations and holds DrainOp behind earlier work.
func (c *coordinator) admit(work *submission, uses []fileUse) {
	work.files = uses
	for i := range work.files {
		use := &work.files[i]
		state := c.files.state(use.op)
		use.state = state
		state.refs++
		if use.close {
			state.closing = work
			state.closeRemaining = state.active
			if state.closeRemaining != 0 {
				work.waitCount++
			}
		}
		state.active += use.ops
	}
}

func (work *submission) fileState(op *operation) *fileState {
	for _, use := range work.files {
		if sameFile(use.op, op) {
			return use.state
		}
	}
	return nil
}

func (c *coordinator) completedOperation(work *submission, op *operation) {
	state := work.fileState(op)
	if state == nil {
		return
	}

	if op.kind() != OpOpenat && op.kind() != OpClose {
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
		if state.closing == work {
			state.closing = nil
			state.closeRemaining = 0
		}
		c.files.removeIfEmpty(use.op, state)
	}
	clear(work.files)
	work.files = nil
}
