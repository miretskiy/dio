package iosched

import (
	"os"

	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/nvme"
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

// NVMeReadBufferOp reads length bytes into buffer's allocations. The scheduler
// splits at allocation boundaries and the device command limit, retaining one
// logical result. length must fit within Cap(), and allocations and transfer
// lengths must be block aligned. Keep buffer alive and unchanged through completion.
func NVMeReadBufferOp(device *nvme.Device, buffer *alloc.Buffer, length int, offset int64) Op {
	return nvmeBufferOp(OpReadv, device, buffer, length, offset)
}

// NVMeWriteBufferOp writes length bytes from buffer's allocations with the same
// splitting as NVMeReadBufferOp. Durable applies FUA to each constituent write.
func NVMeWriteBufferOp(device *nvme.Device, buffer *alloc.Buffer, length int, offset int64) Op {
	return nvmeBufferOp(OpWritev, device, buffer, length, offset)
}

func nvmeBufferOp(kind uint8, device *nvme.Device, buffer *alloc.Buffer, length int, offset int64) Op {
	op := nvmeDataOp(kind, device, nil, offset)
	op.length = int64(length)
	if length < 0 || length > buffer.Cap() {
		panic("iosched: NVMe buffer length outside capacity")
	}
	if length != 0 {
		parts := buffer.RawAllocations()
		op.buf = parts[0].Bytes()[:min(length, len(parts[0].Bytes()))]
		op.allocations = parts[1:]
	}
	return op
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
