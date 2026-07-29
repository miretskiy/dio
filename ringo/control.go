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

type timeoutOp struct {
	opBase
	spec  syscall.Timespec
	count uint32
	flags TimeoutFlags
}

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
func Timeout(spec syscall.Timespec, count uint32, flags TimeoutFlags) Op {
	return &timeoutOp{spec: spec, count: count, flags: flags}
}

type linkTimeoutOp struct {
	opBase
	spec  syscall.Timespec
	flags TimeoutFlags
}

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
func LinkTimeout(spec syscall.Timespec, flags TimeoutFlags) Op {
	return &linkTimeoutOp{spec: spec, flags: flags}
}

type timeoutRemoveOp struct {
	opBase
	target Handle
}

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
func TimeoutRemove(target Handle) Op {
	return &timeoutRemoveOp{target: target}
}

type timeoutUpdateOp struct {
	opBase
	spec   syscall.Timespec
	target Handle
	flags  TimeoutUpdateFlags
}

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
) Op {
	return &timeoutUpdateOp{spec: spec, target: target, flags: flags}
}

type cancelOp struct {
	opBase
	target Handle
}

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
func Cancel(target Handle) Op {
	return &cancelOp{target: target}
}

type cancelFDOp struct {
	opBase
	fd  FD
	all bool
}

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
func CancelFD(fd FD) Op {
	return &cancelFDOp{fd: fd}
}

// CancelAllFD requests cancellation of every operation using fd.
// liburing: io_uring_prep_cancel_fd - https://man7.org/linux/man-pages/man3/io_uring_prep_cancel_fd.3.html
func CancelAllFD(fd FD) Op {
	return &cancelFDOp{fd: fd, all: true}
}

type pollAddOp struct {
	opBase
	fd        FD
	mask      uint32
	multishot bool
}

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
func PollAdd(fd FD, mask uint32) Op { return &pollAddOp{fd: fd, mask: mask} }

// PollMultishot starts a multishot poll request.
// liburing: io_uring_prep_poll_multishot - https://man7.org/linux/man-pages/man3/io_uring_prep_poll_multishot.3.html
func PollMultishot(fd FD, mask uint32) Op {
	return &pollAddOp{fd: fd, mask: mask, multishot: true}
}

type pollRemoveOp struct {
	opBase
	target Handle
}

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
func PollRemove(target Handle) Op { return &pollRemoveOp{target: target} }

type pollUpdateOp struct {
	opBase
	target Handle
	mask   uint32
	flags  PollUpdateFlags
}

func (op *pollUpdateOp) opcode() rawOpcode { return rawOpPollRemove }
func (op *pollUpdateOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	const allowed = PollUpdateMultishot | PollUpdateLevel
	if op.flags&^allowed != 0 {
		return errors.New("ringo: invalid poll-update flags")
	}
	return validateHandle(ring, op.target)
}
func (op *pollUpdateOp) prepare(sqe *rawSQE) {
	*sqe = rawSQE{
		Opcode:   uint8(rawOpPollRemove),
		Fd:       -1,
		Off:      uint64(op.target.slot),
		Addr:     uint64(op.target.slot),
		Len:      uint32(op.flags | pollUpdateEvents),
		Rw_flags: encodePollMask(op.mask),
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// PollUpdate changes the events watched by target.
// liburing: io_uring_prep_poll_update - https://man7.org/linux/man-pages/man3/io_uring_prep_poll_update.3.html
func PollUpdate(target Handle, mask uint32, flags PollUpdateFlags) Op {
	return &pollUpdateOp{target: target, mask: mask, flags: flags}
}

func validateTimeoutFlags(flags, allowed TimeoutFlags) error {
	if flags&^allowed != 0 {
		return errors.New("ringo: invalid timeout flags")
	}
	if flags&TimeoutBoottime != 0 && flags&TimeoutRealtime != 0 {
		return errors.New("ringo: timeout selects multiple clocks")
	}
	return nil
}

type ftruncateOp struct {
	opBase
	fd     FD
	length int64
}

func (op *ftruncateOp) opcode() rawOpcode { return rawOpFtruncate }
func (op *ftruncateOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	return op.fd.validate(ring)
}
func (op *ftruncateOp) prepare(sqe *rawSQE) {
	fd, flags := op.fd.sqe()
	*sqe = rawSQE{
		Opcode: uint8(rawOpFtruncate),
		Flags:  uint8(flags),
		Fd:     fd,
		Off:    uint64(op.length),
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// Ftruncate constructs IORING_OP_FTRUNCATE.
// liburing: io_uring_prep_ftruncate - https://man7.org/linux/man-pages/man3/io_uring_prep_ftruncate.3.html
func Ftruncate(fd FD, length int64) Op {
	op := &ftruncateOp{fd: fd, length: length}
	if length < 0 {
		op.fail(errors.New("ftruncate length must be nonnegative"))
	}
	return op
}

type closeFDOp struct {
	opBase
	fd int
}

func (op *closeFDOp) opcode() rawOpcode { return rawOpClose }
func (op *closeFDOp) validate(*Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	if !fitsInt32(op.fd) {
		return errors.New("ringo: descriptor does not fit the kernel ABI")
	}
	return nil
}
func (op *closeFDOp) prepare(sqe *rawSQE) {
	*sqe = rawSQE{Opcode: uint8(rawOpClose), Fd: int32(op.fd)}
	sqe.Flags |= uint8(op.sqeFlags)
}

// CloseFD asynchronously closes a borrowed process descriptor.
// liburing: io_uring_prep_close - https://man7.org/linux/man-pages/man3/io_uring_prep_close.3.html
func CloseFD(fd int) Op { return &closeFDOp{fd: fd} }
