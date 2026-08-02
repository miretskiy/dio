//go:build linux

package ringo

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"syscall"
	"unsafe"
)

var nativePollMaskNeedsSwap = binary.NativeEndian.Uint16([]byte{0, 1}) == 1

func encodePollMask(mask uint32) int32 {
	return encodePollMaskForEndian(mask, nativePollMaskNeedsSwap)
}

func encodePollMaskForEndian(mask uint32, bigEndian bool) int32 {
	if bigEndian {
		mask = bits.RotateLeft32(mask, 16)
	}
	return int32(mask)
}

// ownedTimespec copies spec into the kernel's __kernel_timespec, whose fields
// are 64-bit on every architecture. syscall.Timespec is 32-bit on 32-bit
// Linux, so an SQE must never point at one directly.
func ownedTimespec(spec syscall.Timespec) rawTimespec {
	return rawTimespec{Sec: int64(spec.Sec), Nsec: int64(spec.Nsec)}
}

type timeoutOp struct {
	opBase
	spec  rawTimespec
	count uint32
	flags TimeoutFlags
}

func (op *timeoutOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.timeouts.put(op)
}

func (op *timeoutOp) reset() { *op = timeoutOp{} }

func (op *timeoutOp) opcode() rawOpcode { return rawOpTimeout }
func (op *timeoutOp) validate(*Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	const allowed = TimeoutAbsolute |
		TimeoutBoottime |
		TimeoutRealtime |
		TimeoutSuccess |
		TimeoutMultishot
	return validateTimeoutFlags(op.flags, allowed)
}
func (op *timeoutOp) prepare(sqe *rawSQE) {
	*sqe = rawSQE{
		Opcode:   uint8(rawOpTimeout),
		Fd:       -1,
		Off:      uint64(op.count),
		Addr:     uint64(uintptr(unsafe.Pointer(&op.spec))),
		Len:      1,
		Rw_flags: int32(op.flags),
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// Timeout constructs IORING_OP_TIMEOUT and copies spec.
// liburing: io_uring_prep_timeout - https://man7.org/linux/man-pages/man3/io_uring_prep_timeout.3.html
func Timeout(
	spec syscall.Timespec,
	count uint32,
	flags TimeoutFlags,
	options ...OpOption,
) Op {
	op := optionAlloc(options).newTimeoutOp()
	op.spec, op.count, op.flags = ownedTimespec(spec), count, flags
	return op
}

type linkTimeoutOp struct {
	opBase
	spec  rawTimespec
	flags TimeoutFlags
}

func (op *linkTimeoutOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.linkTimeouts.put(op)
}

func (op *linkTimeoutOp) reset() { *op = linkTimeoutOp{} }

func (op *linkTimeoutOp) opcode() rawOpcode { return rawOpLinkTimeout }
func (op *linkTimeoutOp) validate(*Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	const allowed = TimeoutAbsolute |
		TimeoutBoottime |
		TimeoutRealtime |
		TimeoutSuccess
	return validateTimeoutFlags(op.flags, allowed)
}
func (op *linkTimeoutOp) prepare(sqe *rawSQE) {
	*sqe = rawSQE{
		Opcode:   uint8(rawOpLinkTimeout),
		Fd:       -1,
		Addr:     uint64(uintptr(unsafe.Pointer(&op.spec))),
		Len:      1,
		Rw_flags: int32(op.flags),
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// LinkTimeout constructs a timeout for the preceding linked request and
// copies spec.
// liburing: io_uring_prep_link_timeout - https://man7.org/linux/man-pages/man3/io_uring_prep_link_timeout.3.html
func LinkTimeout(
	spec syscall.Timespec,
	flags TimeoutFlags,
	options ...OpOption,
) Op {
	op := optionAlloc(options).newLinkTimeoutOp()
	op.spec, op.flags = ownedTimespec(spec), flags
	return op
}

type timeoutRemoveOp struct {
	opBase
	target Handle
}

func (op *timeoutRemoveOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.timeoutRemoves.put(op)
}

func (op *timeoutRemoveOp) reset() { *op = timeoutRemoveOp{} }

func (op *timeoutRemoveOp) opcode() rawOpcode { return rawOpTimeoutRemove }
func (op *timeoutRemoveOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	return validateHandle(ring, op.target)
}
func (op *timeoutRemoveOp) prepare(sqe *rawSQE) {
	*sqe = rawSQE{
		Opcode: uint8(rawOpTimeoutRemove),
		Fd:     -1,
		Addr:   uint64(op.target.slot),
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// TimeoutRemove removes the timeout identified by target.
// liburing: io_uring_prep_timeout_remove - https://man7.org/linux/man-pages/man3/io_uring_prep_timeout_remove.3.html
func TimeoutRemove(target Handle, options ...OpOption) Op {
	op := optionAlloc(options).newTimeoutRemoveOp()
	op.target = target
	return op
}

type timeoutUpdateOp struct {
	opBase
	spec   rawTimespec
	target Handle
	flags  TimeoutUpdateFlags
}

func (op *timeoutUpdateOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.timeoutUpdates.put(op)
}

func (op *timeoutUpdateOp) reset() { *op = timeoutUpdateOp{} }

func (op *timeoutUpdateOp) opcode() rawOpcode { return rawOpTimeoutRemove }
func (op *timeoutUpdateOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	if op.flags&^TimeoutUpdateAbsolute != 0 {
		return errors.New("ringo: invalid timeout-update flags")
	}
	return validateHandle(ring, op.target)
}
func (op *timeoutUpdateOp) prepare(sqe *rawSQE) {
	*sqe = rawSQE{
		Opcode:   uint8(rawOpTimeoutRemove),
		Fd:       -1,
		Off:      uint64(uintptr(unsafe.Pointer(&op.spec))),
		Addr:     uint64(op.target.slot),
		Rw_flags: int32(TimeoutFlags(op.flags) | timeoutUpdateFlag),
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// TimeoutUpdate updates target and copies spec.
// liburing: io_uring_prep_timeout_update - https://man7.org/linux/man-pages/man3/io_uring_prep_timeout_update.3.html
func TimeoutUpdate(
	spec syscall.Timespec,
	target Handle,
	flags TimeoutUpdateFlags,
	options ...OpOption,
) Op {
	op := optionAlloc(options).newTimeoutUpdateOp()
	op.spec, op.target, op.flags = ownedTimespec(spec), target, flags
	return op
}

type cancelOp struct {
	opBase
	target Handle
}

func (op *cancelOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.cancels.put(op)
}

func (op *cancelOp) reset() { *op = cancelOp{} }

func (op *cancelOp) opcode() rawOpcode { return rawOpAsyncCancel }
func (op *cancelOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	return validateHandle(ring, op.target)
}
func (op *cancelOp) prepare(sqe *rawSQE) {
	*sqe = rawSQE{
		Opcode: uint8(rawOpAsyncCancel),
		Fd:     -1,
		Addr:   uint64(op.target.slot),
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// Cancel requests cancellation of the operation identified by target.
// liburing: io_uring_prep_cancel64 - https://man7.org/linux/man-pages/man3/io_uring_prep_cancel64.3.html
func Cancel(target Handle, options ...OpOption) Op {
	op := optionAlloc(options).newCancelOp()
	op.target = target
	return op
}

type cancelFDOp struct {
	opBase
	fd  FD
	all bool
}

func (op *cancelFDOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.cancelFDs.put(op)
}

func (op *cancelFDOp) reset() { *op = cancelFDOp{} }

func (op *cancelFDOp) opcode() rawOpcode { return rawOpAsyncCancel }
func (op *cancelFDOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	return op.fd.validate(ring)
}
func (op *cancelFDOp) prepare(sqe *rawSQE) {
	fd, flags := op.fd.cancel()
	if op.all {
		flags |= rawAsyncCancelAll
	}
	*sqe = rawSQE{
		Opcode:   uint8(rawOpAsyncCancel),
		Fd:       fd,
		Rw_flags: int32(rawAsyncCancelFD | flags),
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// CancelFD requests cancellation by descriptor rather than by Handle.
// liburing: io_uring_prep_cancel_fd - https://man7.org/linux/man-pages/man3/io_uring_prep_cancel_fd.3.html
func CancelFD(fd FD, options ...OpOption) Op {
	return newCancelFDOp(optionAlloc(options), fd, false)
}

func newCancelFDOp(alloc *OpAlloc, fd FD, all bool) *cancelFDOp {
	op := alloc.newCancelFDOp()
	op.fd, op.all = fd, all
	return op
}

// CancelAllFD requests cancellation of every operation using fd.
// liburing: io_uring_prep_cancel_fd - https://man7.org/linux/man-pages/man3/io_uring_prep_cancel_fd.3.html
func CancelAllFD(fd FD, options ...OpOption) Op {
	return newCancelFDOp(optionAlloc(options), fd, true)
}

type pollAddOp struct {
	opBase
	fd        FD
	mask      uint32
	multishot bool
}

func (op *pollAddOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.pollAdds.put(op)
}

func (op *pollAddOp) reset() { *op = pollAddOp{} }

func (op *pollAddOp) opcode() rawOpcode { return rawOpPollAdd }
func (op *pollAddOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	return op.fd.validate(ring)
}
func (op *pollAddOp) prepare(sqe *rawSQE) {
	length := uint32(0)
	if op.multishot {
		length = rawPollMultishot
	}
	fd, flags := op.fd.sqe()
	*sqe = rawSQE{
		Opcode:   uint8(rawOpPollAdd),
		Flags:    uint8(flags),
		Fd:       fd,
		Len:      length,
		Rw_flags: encodePollMask(op.mask),
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// PollAdd starts a poll request for mask.
// liburing: io_uring_prep_poll_add - https://man7.org/linux/man-pages/man3/io_uring_prep_poll_add.3.html
func PollAdd(fd FD, mask uint32, options ...OpOption) Op {
	return newPollAddOp(optionAlloc(options), fd, mask, false)
}

func newPollAddOp(alloc *OpAlloc, fd FD, mask uint32, multishot bool) *pollAddOp {
	op := alloc.newPollAddOp()
	op.fd, op.mask, op.multishot = fd, mask, multishot
	return op
}

// PollMultishot starts a multishot poll request.
// liburing: io_uring_prep_poll_multishot - https://man7.org/linux/man-pages/man3/io_uring_prep_poll_multishot.3.html
func PollMultishot(fd FD, mask uint32, options ...OpOption) Op {
	return newPollAddOp(optionAlloc(options), fd, mask, true)
}

type pollRemoveOp struct {
	opBase
	target Handle
}

func (op *pollRemoveOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.pollRemoves.put(op)
}

func (op *pollRemoveOp) reset() { *op = pollRemoveOp{} }

func (op *pollRemoveOp) opcode() rawOpcode { return rawOpPollRemove }
func (op *pollRemoveOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	return validateHandle(ring, op.target)
}
func (op *pollRemoveOp) prepare(sqe *rawSQE) {
	*sqe = rawSQE{
		Opcode: uint8(rawOpPollRemove),
		Fd:     -1,
		Addr:   uint64(op.target.slot),
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// PollRemove removes the poll request identified by target.
// liburing: io_uring_prep_poll_remove - https://man7.org/linux/man-pages/man3/io_uring_prep_poll_remove.3.html
func PollRemove(target Handle, options ...OpOption) Op {
	op := optionAlloc(options).newPollRemoveOp()
	op.target = target
	return op
}

// Ringo deliberately does not expose IORING_POLL_UPDATE_EVENTS. Its only
// Ringo-expressible effect is replacing an in-flight poll's event mask, which
// PollRemove followed by PollAdd also does; the kernel's other half,
// IORING_POLL_UPDATE_USER_DATA, would rewrite the identity that makes Handle
// generation-safe. IORING_POLL_ADD_LEVEL is likewise absent: the UAPI header
// documents it, but io_poll_add_prep and io_poll_remove_prep both reject it, so
// no kernel accepts it in either position.

func validateTimeoutFlags(flags, allowed TimeoutFlags) error {
	if flags&^allowed != 0 {
		return errors.New("ringo: invalid timeout flags")
	}
	if flags&TimeoutBoottime != 0 && flags&TimeoutRealtime != 0 {
		return errors.New("ringo: timeout selects multiple clocks")
	}
	return nil
}
