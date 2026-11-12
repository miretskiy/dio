//go:build linux

package iosched

import (
	"os"

	"github.com/miretskiy/dio/v2/ringo"
)

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
