//go:build linux

package iosched

import (
	"errors"
	"fmt"
	"math"
	"os"
	"syscall"

	"github.com/miretskiy/dio/v2/nvme"
	"github.com/miretskiy/dio/v2/ringo"
)

// NVMeReadOp reads buf at a device-relative byte offset. Submit validates block
// alignment and the Device's range, then splits transfers at its command limit.
// A successful ticket reports len(buf); an error reports the successful prefix.
// Keep the Device open and buf valid until completion. WithNVMeCommands is required.
func NVMeReadOp(device *nvme.Device, buf []byte, offset int64) Op {
	return nvmeDataOp(OpRead, device, buf, offset)
}

// NVMeWriteOp writes buf at a device-relative byte offset, with the same bounds
// and splitting as NVMeReadOp. Durable requests FUA on every constituent write.
// After an error, writes beyond the reported prefix may also have reached storage.
func NVMeWriteOp(device *nvme.Device, buf []byte, offset int64) Op {
	return nvmeDataOp(OpWrite, device, buf, offset)
}

func nvmeDataOp(kind uint8, device *nvme.Device, buf []byte, offset int64) Op {
	var file *os.File
	if device != nil {
		file = device.File()
	}
	return Op{operation: operation{
		opcode: kind | opNVMe,
		f:      file,
		buf:    buf,
		offset: offset,
		nvme:   device,
	}}
}

// NVMeFlushOp makes the namespace's prior completed writes durable, covering
// every Device range on that namespace. Its successful ticket result is zero.
// WithNVMeCommands is required.
func NVMeFlushOp(device *nvme.Device) Op {
	return nvmeDataOp(OpFdatasync, device, nil, 0)
}

func nvmeCommand(op *operation) (nvme.Command, error) {
	switch op.kind() {
	case OpRead:
		return op.nvme.ReadCommand(op.buf, uint64(op.offset))
	case OpWrite:
		return op.nvme.WriteCommand(op.buf, uint64(op.offset), op.opFlags&opDurable != 0)
	case OpFdatasync:
		return op.nvme.FlushCommand(), nil
	default:
		return nvme.Command{}, fmt.Errorf("iosched: unsupported NVMe operation %d", op.kind())
	}
}

func (s *URingScheduler) validateNVMe(op *operation) error {
	if !s.config.nvmeCommands {
		return fmt.Errorf("iosched: NVMe operation requires WithNVMeCommands")
	}
	if op.isVirtual() || op.isFixed() {
		return fmt.Errorf("iosched: NVMe operation requires a regular descriptor and buffer")
	}
	if op.nvme == nil || op.nvme.File() == nil {
		return fmt.Errorf("iosched: NVMe operation requires an open device")
	}
	if op.offset < 0 || int64(len(op.buf)) > math.MaxInt64-op.offset {
		return fmt.Errorf("iosched: NVMe byte offset exceeds int64 range")
	}
	switch op.kind() {
	case OpRead, OpWrite:
		return op.nvme.ValidateIO(uint64(op.offset), uint64(len(op.buf)))
	case OpFdatasync:
		return nil
	default:
		return fmt.Errorf("iosched: unsupported NVMe operation %d", op.kind())
	}
}

// nvmeCommandLimit returns the maximum bytes in one command on this handle.
func nvmeCommandLimit(device *nvme.Device) int {
	return int(device.Info().MaxTransferBytes)
}

func translateNVMe(op *operation, recycle ringo.OpOption) ringo.Op {
	if op.kind() != OpFdatasync && len(op.buf) == 0 {
		return ringo.Nop(recycle)
	}
	cmd, err := nvmeCommand(op)
	if err != nil {
		panic(fmt.Sprintf("iosched: validated NVMe operands changed: %v", err))
	}
	request, err := cmd.Op(ringo.FileFD(op.f), recycle)
	if err != nil {
		panic(fmt.Sprintf("iosched: validated NVMe command changed: %v", err))
	}
	return request
}

func nvmeResult(op *operation, completion ringo.Completion) (int, error) {
	if err := nvme.Status(completion); err != nil {
		return 0, err
	}
	if op.kind() == OpRead || op.kind() == OpWrite {
		return len(op.buf), nil
	}
	return 0, nil
}

// nvmeSplit tracks one logical operation at a time. A batch occupies at most
// the ring's free slots; all its completions are reaped before another batch.
// This bounds state independently of request size and keeps linked successors
// behind the entire logical transfer.
type nvmeSplit struct {
	started bool  // The submission already holds its dispatch allowance.
	index   int   // Current logical operation in the caller's chain.
	cursor  int   // Bytes placed for the current operation.
	pending int   // Commands still awaiting completion in this batch.
	prefix  int   // First failed byte offset, or the complete operation length.
	err     error // Error at the earliest failing byte offset.
	failed  bool  // Kernel link failure: Linux error or ordinary short I/O.
}

func (c *coordinator) placeNVMeParts(work *submission) {
	state := work.nvmeSplit
	op := work.op(state.index)
	if state.cursor == 0 {
		state.prefix = len(op.buf)
	}
	for c.occupied < int(c.sched.config.ringDepth) {
		part := *op
		start, length := 0, len(op.buf)
		if op.isNVMe() && op.kind() != OpFdatasync {
			start = state.cursor
			length = min(len(op.buf)-start, nvmeCommandLimit(op.nvme))
			part.buf = op.buf[start : start+length]
			part.offset += int64(start)
		}
		handle, err := c.ring.Push(c.translateOp(&part))
		if err != nil {
			state.record(start, 0, err, true)
			if state.pending == 0 {
				c.advanceNVMe(work, state.prefix, err)
			}
			return
		}
		work.markIssued()
		c.place(handle, ringSlot{kind: slotNVMePart, work: work, op: op, partOffset: start, partLength: length})
		state.pending++
		state.cursor += length
		if !op.isNVMe() || state.cursor == len(op.buf) {
			break
		}
	}
}

// record retains the contiguous successful prefix even when commands complete
// out of order. Outstanding commands beyond that prefix are drained too.
func (state *nvmeSplit) record(offset, n int, err error, failed bool) {
	state.failed = state.failed || failed
	if err != nil && (state.err == nil || offset+n < state.prefix) {
		state.prefix, state.err = offset+n, err
	}
}

func (c *coordinator) finishNVMePart(slot ringSlot, completion ringo.Completion) {
	work, op := slot.work, slot.op
	state := work.nvmeSplit
	n, err := completion.Result, completion.Err
	failed := completion.Err != nil
	if op.isNVMe() {
		err = nvme.Status(completion)
		n = 0
		if err == nil {
			n = slot.partLength
		}
	} else {
		// Linux breaks ordinary read/write soft links on a short transfer too.
		if op.kind() == OpRead || op.kind() == OpReadv || op.isWrite() {
			failed = failed || n != opBytes(op)
		}
		err = writeResultError(op, n, err)
	}
	if errors.Is(err, syscall.ECANCELED) && c.sched.stopCause() == errSchedulerClosed {
		err = errSchedulerClosed
	}
	state.record(slot.partOffset, n, err, failed)
	state.pending--
	if state.pending != 0 {
		return
	}
	if op.isNVMe() {
		if state.err == nil && state.cursor < len(op.buf) {
			c.resumeNVMe(work)
			return
		}
		n, err = state.prefix, state.err
	}
	c.advanceNVMe(work, n, err)
}

// advanceNVMe applies native link failure rules between logical operations.
// Positive NVMe statuses are reported to the caller and allow a soft successor,
// matching io_uring_cmd. Negative Linux errors break a soft chain.
func (c *coordinator) advanceNVMe(work *submission, n int, err error) {
	state := work.nvmeSplit
	op := work.op(state.index)
	cancel := state.failed && op.sqeFlags&sqeHardLink == 0
	c.finishOperation(work, op, n, err)
	next := state.index + 1
	if cancel {
		for i := next; i < work.opCount(); i++ {
			c.finishOperation(work, work.op(i), 0, syscall.ECANCELED)
		}
		return
	}
	if next < work.opCount() {
		*state = nvmeSplit{started: true, index: next}
		c.resumeNVMe(work)
	}
}

// resumeNVMe queues placement after Reap releases its borrow of the ring. Work
// stays issued and retains its dispatch allowance until its logical I/O finishes.
func (c *coordinator) resumeNVMe(work *submission) {
	c.readyClasses[0]++
	c.ready.push(work)
}
