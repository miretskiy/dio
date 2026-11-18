//go:build linux

package iosched

import (
	"fmt"
	"math"
	"math/bits"
	"os"
	"syscall"

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
		if buildutil.TestMode {
			buildutil.Assert(state.active >= 0)
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

func (c *coordinator) initVFiles(capacity uint32) {
	c.vfiles = make([]uint64, (int(capacity)+63)/64)
	if tail := capacity % 64; tail != 0 {
		c.vfiles[len(c.vfiles)-1] = math.MaxUint64 << tail
	}
}

func (c *coordinator) allocVFD() uint32 {
	word := c.nextVFile
	for range c.vfiles {
		if free := ^c.vfiles[word]; free != 0 {
			bit := bits.TrailingZeros64(free)
			c.vfiles[word] |= uint64(1) << bit
			c.nextVFile = word
			return uint32(word*64 + bit)
		}
		word++
		if word == len(c.vfiles) {
			word = 0
		}
	}
	return AllocateVFD
}

func (c *coordinator) freeVFD(vfd uint32) {
	if vfd < c.reservedVFiles {
		return
	}
	c.vfiles[vfd/64] &^= uint64(1) << (vfd % 64)
	c.nextVFile = int(vfd / 64)
}

func (c *coordinator) prepareVFiles(work *submission) error {
	root := &work.root
	if !root.isVirtual() || root.kind() != OpOpenat || root.vfd != AllocateVFD {
		return nil
	}
	vfd := c.allocVFD()
	if vfd == AllocateVFD {
		return syscall.ENFILE
	}
	root.opFlags |= opAllocatedVFD
	for i := range work.opCount() {
		op := work.op(i)
		if op.isVirtual() && op.vfd == AllocateVFD {
			op.vfd = vfd
		}
	}
	return nil
}

func (c *coordinator) releaseVFDReservation(root *operation) {
	if root.opFlags&opAllocatedVFD != 0 {
		c.freeVFD(root.vfd)
	}
}
