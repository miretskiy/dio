//go:build linux

package ringo

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

// CommandBuffer connects one address slot in a command payload to its Go buffer.
// For example, an NVMe read uses a 72-byte payload with its data address at byte
// 24. For a 4096-byte read of LBA 0 in namespace 1 with 512-byte blocks:
//
//	var payload [72]byte
//	payload[0] = 0x02 // NVMe Read opcode inside the driver payload.
//	binary.NativeEndian.PutUint32(payload[4:], 1) // Namespace ID.
//	binary.NativeEndian.PutUint32(payload[36:], 4096) // Transfer bytes.
//	binary.NativeEndian.PutUint32(payload[48:], 7) // Eight blocks, encoded minus one.
//	data := make([]byte, 4096)
//	const nvmeIO = 0xc0484e80 // NVME_URING_CMD_IO: selects the driver's I/O interface.
//	op := ringo.UringCmd(fd, nvmeIO, payload[:], []ringo.CommandBuffer{
//		{Offset: 24, Buffer: data},
//	})
//
// Push writes data's address into payload bytes [24, 32), after arranging for
// the buffer to escape to the heap. The Ring retains data through the final
// completion. Keep its storage mapped and reserved for this I/O until then.
// The nvme package supplies typed helpers for this command layout.
type CommandBuffer struct {
	// Offset begins an eight-byte address slot within the command payload.
	// Each slot must fit in the payload and be disjoint from the other slots.
	Offset uint8
	// Buffer supplies the address and remains retained until final completion.
	// An empty buffer writes a zero address into its slot.
	Buffer []byte
}

type uringCmdOp struct {
	opBase
	fd            FD
	driverOp      uint32
	payload       [80]byte
	buffers       []CommandBuffer
	inlineBuffers [2]CommandBuffer
	registered    FixedBuffer
	fixed         bool
}

// UringCmd prepares a device request for a Ring created WithUringCmd.
// driverOp selects the driver's command interface (such as NVME_URING_CMD_IO).
// payload contains up to 80 bytes in that interface's command layout, including
// its device opcode and parameters. See CommandBuffer for a complete NVMe example.
//
// The constructor copies payload and the buffer descriptors. Push fills the
// descriptors' address slots in native byte order and retains their buffers.
// Callers manage the lifetime of any additional addresses embedded in payload.
// Completion.Result preserves driver-specific positive results; negative Linux
// errors populate Completion.Err.
func UringCmd(fd FD, driverOp uint32, payload []byte, buffers []CommandBuffer, options ...OpOption) Op {
	return newUringCmdOp(optionAlloc(options), fd, driverOp, payload, buffers, FixedBuffer{}, false)
}

// UringCmdFixed is UringCmd for a command whose data lies in a registered
// buffer: the driver maps registered's entry instead of pinning user pages for
// each command. Every nonempty buffer must lie within registered's entry, and
// the driver must support IORING_URING_CMD_FIXED, as NVMe does for its data
// buffer.
func UringCmdFixed(fd FD, driverOp uint32, payload []byte, buffers []CommandBuffer, registered FixedBuffer, options ...OpOption) Op {
	return newUringCmdOp(optionAlloc(options), fd, driverOp, payload, buffers, registered, true)
}

func newUringCmdOp(alloc *OpAlloc, fd FD, driverOp uint32, payload []byte, buffers []CommandBuffer, registered FixedBuffer, fixed bool) Op {
	op := alloc.newUringCmdOp()
	op.registered, op.fixed = registered, fixed
	op.fd, op.driverOp = fd, driverOp
	copy(op.payload[:], payload)
	if len(payload) > len(op.payload) {
		op.fail(errors.New("command exceeds 80 inline bytes"))
	}
	if len(buffers) <= len(op.inlineBuffers) {
		op.buffers = op.inlineBuffers[:len(buffers)]
		copy(op.buffers, buffers)
	} else {
		op.buffers = append([]CommandBuffer(nil), buffers...)
	}
	var occupied [80]bool
	for _, b := range op.buffers {
		start := int(b.Offset)
		if start+8 > min(len(payload), len(op.payload)) {
			op.fail(errors.New("command buffer address is outside payload"))
			continue
		}
		for i := start; i < start+8; i++ {
			if occupied[i] {
				op.fail(errors.New("command buffer addresses overlap"))
			}
			occupied[i] = true
		}
	}
	return op
}

func (op *uringCmdOp) opcode() rawOpcode { return rawOpUringCmd }
func (op *uringCmdOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	if ring.backend.flags&(rawSetupSQE128|rawSetupCQE32) != rawSetupSQE128|rawSetupCQE32 {
		return errors.New("ringo: UringCmd requires WithUringCmd")
	}
	if op.fixed {
		if err := op.registered.validate(ring); err != nil {
			return err
		}
		for _, b := range op.buffers {
			if len(b.Buffer) != 0 && !op.registered.contains(b.Buffer) {
				return errNotRegistered
			}
		}
	}
	return op.fd.validate(ring)
}
func (op *uringCmdOp) prepare(sqe *rawSQE) {
	fd, flags := op.fd.sqe()
	*sqe = rawSQE{Opcode: uint8(rawOpUringCmd), Fd: fd, Flags: uint8(flags | op.sqeFlags)}
	if op.fixed {
		sqe.Rw_flags = int32(rawUringCmdFixed) // The SQE's uring_cmd_flags.
		sqe.Buf_index = op.registered.index
	}
	binary.NativeEndian.PutUint32(unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(sqe), 8)), 4), op.driverOp)
	payload := unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(sqe), 48)), 80)
	copy(payload, op.payload[:])
	for _, b := range op.buffers {
		var addr uint64
		if len(b.Buffer) != 0 {
			addr = uint64(slicePtr(b.Buffer))
		}
		binary.NativeEndian.PutUint64(payload[int(b.Offset):], addr)
	}
}
func (op *uringCmdOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	*op = uringCmdOp{}
	alloc.commands.put(op, alloc)
}
