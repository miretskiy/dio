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
	"golang.org/x/sys/unix"
)

func nvmeCommand(op *operation) (nvme.Command, error) {
	switch op.kind() {
	case OpRead, OpReadv:
		return op.nvme.ReadCommand(op.buf, uint64(op.offset))
	case OpWrite, OpWritev:
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
	length := opBytes(op)
	if op.offset < 0 || int64(length) > math.MaxInt64-op.offset {
		return fmt.Errorf("iosched: NVMe byte offset exceeds int64 range")
	}
	switch op.kind() {
	case OpRead, OpWrite, OpReadv, OpWritev:
		if err := op.nvme.ValidateIO(uint64(op.offset), uint64(length)); err != nil {
			return err
		}
		block := int(op.nvme.Info().BlockSize)
		if len(op.buf)%block != 0 {
			return fmt.Errorf("iosched: unaligned NVMe buffer allocation")
		}
		for _, memory := range op.allocations {
			if len(memory.Bytes())%block != 0 {
				return fmt.Errorf("iosched: unaligned NVMe buffer allocation")
			}
		}
		return nil
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

// nvmeQueueDepth bounds a ring depth by each device's request queue depth,
// keeping room for the longest chain.
func nvmeQueueDepth(depth uint32, devices []*nvme.Device) uint32 {
	for _, device := range devices {
		depth = min(depth, max(device.Info().QueueDepth, minRingDepth))
	}
	return depth
}

// registered binds an NVMe command's data to the scheduler's registered
// buffers, when it lies inside one entry the kernel can map. Before Linux 6.15,
// io_import_fixed counts every page to the end of the entry, and the block
// layer fails the command with EINVAL when that exceeds the queue's segment
// limit; such commands keep ordinary addresses. The device's transfer limit
// stays within that segment limit.
func (c *coordinator) registered(op *operation) (ringo.FixedBuffer, bool) {
	if c.sched.registeredBuffers == nil {
		return ringo.FixedBuffer{}, false
	}
	registered, err := c.sched.registeredBuffers.Bind(op.buf)
	if err != nil {
		return ringo.FixedBuffer{}, false
	}
	if fixedNVMeCountsWholeEntry && registered.EntryRemaining() > nvmeCommandLimit(op.nvme) {
		return ringo.FixedBuffer{}, false
	}
	return registered, true
}

// fixedNVMeCountsWholeEntry reports a kernel older than 6.15.
var fixedNVMeCountsWholeEntry = func() bool {
	var name unix.Utsname
	if unix.Uname(&name) != nil {
		return true
	}
	var major, minor int
	if _, err := fmt.Sscanf(unix.ByteSliceToString(name.Release[:]), "%d.%d", &major, &minor); err != nil {
		return true
	}
	return major < 6 || major == 6 && minor < 15
}()

// translateNVMe builds one command. Data inside a registered buffer uses it,
// which spares the driver pinning the pages of every command.
func (c *coordinator) translateNVMe(op *operation) ringo.Op {
	recycle := ringo.WithAlloc(&c.alloc)
	if op.kind() != OpFdatasync && len(op.buf) == 0 {
		return ringo.Nop(recycle)
	}
	cmd, err := nvmeCommand(op)
	if err != nil {
		panic(fmt.Sprintf("iosched: validated NVMe operands changed: %v", err))
	}
	var request ringo.Op
	if registered, ok := c.registered(op); ok {
		request, err = cmd.FixedOp(ringo.FileFD(op.f), registered, recycle)
	} else {
		request, err = cmd.Op(ringo.FileFD(op.f), recycle)
	}
	if err != nil {
		panic(fmt.Sprintf("iosched: validated NVMe command changed: %v", err))
	}
	return request
}

func nvmeResult(op *operation, completion ringo.Completion) (int, error) {
	if err := nvme.Status(completion); err != nil {
		return 0, err
	}
	if op.kind() == OpRead || op.kind() == OpReadv || op.isWrite() {
		return opBytes(op), nil
	}
	return 0, nil
}

// splitNVMe reports whether op needs more than one command. Such operations
// are submitted unlinked and placed by placeNVMe.
func splitNVMe(op *operation) bool {
	return op.isNVMe() && opBytes(op) > min(len(op.buf), nvmeCommandLimit(op.nvme))
}

// placeNVMe issues commands from the front of the submission's private op
// while the ring has room, advancing buf and offset past each one. The work
// keeps its place at the head of the ready queue, and its dispatch allowance,
// until its last command is placed; completions free the slots its next pass
// fills.
func (c *coordinator) placeNVMe(work *submission) {
	work.state = workIssued
	op := &work.root
	for opBytes(op) != 0 && c.occupied < int(c.sched.config.ringDepth) {
		part := *op
		part.buf = op.buf[:min(len(op.buf), opBytes(op), nvmeCommandLimit(op.nvme))]
		handle, err := c.ring.Push(c.translateNVMe(&part))
		if err != nil {
			c.stopNVMe(work, work.sent, err)
			break
		}
		work.markIssued()
		c.place(handle, ringSlot{kind: slotNVMePart, work: work, op: op, partOffset: work.sent})
		work.pending++
		work.sent += len(part.buf)
		op.advance(len(part.buf))
	}
	if opBytes(op) != 0 {
		return
	}
	c.take(work)
	if work.pending == 0 {
		c.finishNVMe(work)
	}
}

// advance consumes n leading bytes, moving to the next allocation when the
// current one is used up.
func (op *operation) advance(n int) {
	op.buf, op.offset = op.buf[n:], op.offset+int64(n)
	if op.kind() == OpReadv || op.kind() == OpWritev {
		op.length -= int64(n)
	}
	if len(op.buf) == 0 && len(op.allocations) != 0 {
		op.buf, op.allocations = op.allocations[0].Bytes(), op.allocations[1:]
	}
}

func (c *coordinator) finishNVMePart(slot ringSlot, completion ringo.Completion) {
	work := slot.work
	work.pending--
	if err := nvme.Status(completion); err != nil {
		if opBytes(&work.root) != 0 {
			c.take(work) // Its unplaced commands are dropped.
		}
		c.stopNVMe(work, slot.partOffset, err)
	}
	if work.pending == 0 && opBytes(&work.root) == 0 {
		c.finishNVMe(work)
	}
}

// stopNVMe records a failure at a logical byte offset and drops the unissued
// remainder. The earliest failure sets the result, in any completion order.
func (c *coordinator) stopNVMe(work *submission, offset int, err error) {
	if errors.Is(err, syscall.ECANCELED) && c.sched.stopCause() == errSchedulerClosed {
		err = errSchedulerClosed
	}
	if work.err == nil || offset < work.n {
		work.n, work.err = offset, err
	}
	op := &work.root
	op.buf, op.allocations, op.length = nil, nil, 0
}

// finishNVMe reports the bytes issued, or the prefix before the first failure.
func (c *coordinator) finishNVMe(work *submission) {
	n := work.sent
	if work.err != nil {
		n = work.n
	}
	c.finishOperation(work, &work.root, n, work.err)
}

// RawNVMeCmdOp schedules a caller-built NVMe command as a ringo operation.
// Construct request with nvme.Command.Op, nvme.Command.AdminOp, or ringo.UringCmd.
// For example, submit a namespace command and interpret its NVMe status:
//
//	request, err := command.Op(ringo.FileFD(device.File()))
//	if err != nil { return err }
//	var completion ringo.Completion
//	ticket, err := scheduler.Submit(iosched.RawNVMeCmdOp(device.File(), request, &completion))
//	if err != nil { return err }
//	if _, err = ticket.Wait(); err != nil { return err }
//	return nvme.Status(completion)
//
// file is the controller or namespace character-device handle targeted by
// request. It participates in DrainOp bookkeeping and remains retained through
// completion. Keep it open and referenced buffers mapped until the ticket completes.
//
// A successful Submit transfers request to the scheduler permanently. Discard
// all copies of both request and this Op afterward. A Submit error leaves
// request with the caller, who may retry it. Each accepted RawNVMeCmdOp is single-use.
//
// Wait reports the driver's Result and Linux error, along with the usual
// scheduler errors. completion may be nil; otherwise it receives the final CQE,
// including Flags and Extra, before the ticket completes. Failures before a CQE
// populate its Err field. The ring consumes earlier multishot completions.
// Decode NVMe statuses after Wait with nvme.Status.
//
// Raw commands are submitted intact, with caller-defined addressing, transfer
// sizes, and ordering. DiskPolicy charges typed read/write ops; RawNVMeCmdOp uses ring
// capacity and file barriers. NVMe commands require WithNVMeCommands to configure
// extended ring entries. The POSIX scheduler returns EOPNOTSUPP for RawNVMeCmdOp.
func RawNVMeCmdOp(file *os.File, request ringo.Op, completion *ringo.Completion) Op {
	return Op{operation: operation{
		opcode: OpRawNVMeCmd,
		f:      file,
		raw:    &rawOperation{request: request, destination: completion},
	}}
}

type rawOperation struct {
	request     ringo.Op          // Operation transferred to the coordinator on accepted submission.
	destination *ringo.Completion // Optional caller-owned final completion destination.
	recorded    bool              // The destination already contains its final outcome.
}

func (raw *rawOperation) record(completion ringo.Completion) {
	if raw.recorded {
		return
	}
	if raw.destination != nil {
		*raw.destination = completion
	}
	raw.recorded = true
	ringo.Discard(raw.request) // Unplaced requests are still owned by the scheduler.
	raw.request = nil
}

// recordRawResult covers placement errors and cancellation before an SQE runs.
// A completion already reported by the kernel retains its original CQE fields.
func recordRawResult(op *operation, n int, err error) {
	if op.raw != nil {
		op.raw.record(ringo.Completion{Result: n, Err: err})
	}
}
