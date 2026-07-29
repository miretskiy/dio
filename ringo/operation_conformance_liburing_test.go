//go:build linux && cgo && liburing_conformance

package ringo

import (
	"syscall"
	"testing"

	"github.com/miretskiy/dio/ringo/internal/testdata/liburingoracle"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestOperationPreparationMatchesLiburing(t *testing.T) {
	const (
		fd            = 9
		handleSlot    = 0x1234
		operationFlag = uint32(3)
	)
	handle := Handle{slot: handleSlot}
	resourceRing := new(Ring)
	table := &FixedFiles{ring: resourceRing, count: 16}
	fixed := FixedFile{table: table, index: 7}
	buffer := make([]byte, 64)
	vectors := [][]byte{buffer[:16], buffer[32:48]}
	registeredBuffers := make([][]byte, 6)
	registeredBuffers[5] = buffer
	registered := FixedBuffer{
		set:   &FixedBuffers{ring: resourceRing, buffers: registeredBuffers},
		index: 5,
		data:  buffer,
	}
	how := unix.OpenHow{Flags: unix.O_RDWR, Mode: 0o600}
	stat := new(unix.Statx_t)
	spec := syscall.Timespec{Sec: 7, Nsec: 11}
	readv := &readvOp{
		fd: BorrowedFD(fd), iovecs: make([]syscall.Iovec, 2), offset: 17,
	}
	writev := &writevOp{
		fd: BorrowedFD(fd), iovecs: make([]syscall.Iovec, 2), offset: 17,
	}
	openat := newOpenAtOp(BorrowedFD(unix.AT_FDCWD), "file.dat", 0x42, 0o600, nil)
	openatDirect := newOpenAtOp(
		BorrowedFD(unix.AT_FDCWD),
		"file.dat",
		0x42,
		0o600,
		&fixed,
	)

	tests := []struct {
		name string
		op   Op
		want func(rawSQE) liburingoracle.SQE
	}{
		{
			name: "nop",
			op:   Nop(),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.PrepareNop()
			},
		},
		{
			name: "read",
			op:   Read(BorrowedFD(fd), buffer, 17),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.PrepareRead(fd, buffer, uint32(len(buffer)), 17)
			},
		},
		{
			name: "read-direct",
			op:   Read(FixedFD(fixed), buffer, 17),
			want: func(rawSQE) liburingoracle.SQE {
				return withSQEFlags(
					liburingoracle.PrepareRead(
						int(fixed.index),
						buffer,
						uint32(len(buffer)),
						17,
					),
					rawSqeFixedFile,
				)
			},
		},
		{
			name: "read-fixed",
			op:   newReadOp(BorrowedFD(fd), buffer, 17, registered, true),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.PrepareReadFixed(
					fd,
					buffer,
					uint32(len(buffer)),
					17,
					int(registered.index),
				)
			},
		},
		{
			name: "readv",
			op:   readv,
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.PrepareReadv(fd, readv.iovecs, 17)
			},
		},
		{
			name: "write",
			op:   Write(BorrowedFD(fd), buffer, 17),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.PrepareWrite(fd, buffer, uint32(len(buffer)), 17)
			},
		},
		{
			name: "write-fixed",
			op:   newWriteOp(BorrowedFD(fd), buffer, 17, registered, true),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.PrepareWriteFixed(
					fd,
					buffer,
					uint32(len(buffer)),
					17,
					int(registered.index),
				)
			},
		},
		{
			name: "writev",
			op:   writev,
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.PrepareWritev(fd, writev.iovecs, 17)
			},
		},
		{
			name: "fsync",
			op:   Fdatasync(BorrowedFD(fd)),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.PrepareFsync(fd, rawFsyncDatasync)
			},
		},
		{
			name: "fallocate",
			op:   FallocateMode(BorrowedFD(fd), FallocatePunchHole, 17, 4096),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.PrepareFallocate(fd, 2, 17, 4096)
			},
		},
		{
			name: "openat",
			op:   openat,
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.PrepareOpenat(
					unix.AT_FDCWD,
					openat.path,
					0x42,
					0o600,
				)
			},
		},
		{
			name: "openat-direct",
			op:   openatDirect,
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.PrepareOpenatDirect(
					unix.AT_FDCWD,
					openatDirect.path,
					0x42,
					0o600,
					fixed.index,
				)
			},
		},
		{
			name: "close-direct",
			op:   CloseDirect(fixed),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.PrepareCloseDirect(fixed.index)
			},
		},
		{
			name: "readv-fixed",
			op: ReadvFixed(
				BorrowedFD(fd),
				registered,
				vectors,
				17,
				int(operationFlag),
			),
			want: func(sqe rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PrepareReadvFixed,
					fd,
					sqe.Addr,
					uint64(len(vectors)),
					17,
					uint64(operationFlag),
					5,
				)
			},
		},
		{
			name: "writev-fixed",
			op: WritevFixed(
				BorrowedFD(fd),
				registered,
				vectors,
				17,
				int(operationFlag),
			),
			want: func(sqe rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PrepareWritevFixed,
					fd,
					sqe.Addr,
					uint64(len(vectors)),
					17,
					uint64(operationFlag),
					5,
				)
			},
		},
		{
			name: "timeout",
			op:   Timeout(spec, 2, TimeoutAbsolute|TimeoutBoottime),
			want: func(sqe rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PrepareTimeout,
					sqe.Addr,
					2,
					uint64(TimeoutAbsolute|TimeoutBoottime),
				)
			},
		},
		{
			name: "link-timeout",
			op:   LinkTimeout(spec, TimeoutAbsolute|TimeoutSuccess),
			want: func(sqe rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PrepareLinkTimeout,
					sqe.Addr,
					uint64(TimeoutAbsolute|TimeoutSuccess),
				)
			},
		},
		{
			name: "timeout-remove",
			op:   TimeoutRemove(handle),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PrepareTimeoutRemove,
					handleSlot,
					0,
				)
			},
		},
		{
			name: "timeout-update",
			op:   TimeoutUpdate(spec, handle, TimeoutUpdateAbsolute),
			want: func(sqe rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PrepareTimeoutUpdate,
					sqe.Off,
					handleSlot,
					uint64(TimeoutUpdateAbsolute)|uint64(timeoutUpdateFlag),
				)
			},
		},
		{
			name: "cancel",
			op:   Cancel(handle),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PrepareCancel,
					handleSlot,
					0,
				)
			},
		},
		{
			name: "poll-add",
			op:   PollAdd(BorrowedFD(fd), unix.POLLIN),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PreparePollAdd,
					fd,
					unix.POLLIN,
				)
			},
		},
		{
			name: "poll-multishot",
			op:   PollMultishot(BorrowedFD(fd), unix.POLLIN),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PreparePollMultishot,
					fd,
					unix.POLLIN,
				)
			},
		},
		{
			name: "poll-remove",
			op:   PollRemove(handle),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PreparePollRemove,
					handleSlot,
				)
			},
		},
		{
			name: "openat2",
			op:   OpenAt2(BorrowedFD(unix.AT_FDCWD), "file.dat", how),
			want: func(sqe rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PrepareOpenat2,
					signedArgument(unix.AT_FDCWD),
					sqe.Addr,
					sqe.Off,
				)
			},
		},
		{
			name: "statx",
			op: StatxAt(
				BorrowedFD(unix.AT_FDCWD),
				"file.dat",
				0,
				unix.STATX_SIZE,
				stat,
			),
			want: func(sqe rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PrepareStatx,
					signedArgument(unix.AT_FDCWD),
					sqe.Addr,
					0,
					unix.STATX_SIZE,
					sqe.Off,
				)
			},
		},
		{
			name: "ftruncate",
			op:   Ftruncate(BorrowedFD(fd), 4096),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PrepareFtruncate,
					fd,
					4096,
				)
			},
		},
		{
			name: "close",
			op:   CloseFD(fd),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PrepareClose,
					fd,
				)
			},
		},
		{
			name: "poll-update",
			op: PollUpdate(
				handle,
				unix.POLLIN,
				PollUpdateMultishot,
			),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PreparePollUpdate,
					handleSlot,
					handleSlot,
					unix.POLLIN,
					uint64(PollUpdateMultishot|pollUpdateEvents),
				)
			},
		},
		{
			name: "cancel-fd",
			op:   CancelFD(BorrowedFD(fd)),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PrepareCancelFD,
					fd,
					0,
				)
			},
		},
		{
			name: "cancel-all-fd",
			op:   CancelAllFD(BorrowedFD(fd)),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PrepareCancelFD,
					fd,
					uint64(rawAsyncCancelAll),
				)
			},
		},
		{
			name: "cancel-fixed-fd",
			op:   CancelFD(FixedFD(fixed)),
			want: func(rawSQE) liburingoracle.SQE {
				return liburingoracle.Prepare(
					liburingoracle.PrepareCancelFD,
					uint64(fixed.index),
					uint64(rawAsyncCancelFDFixed),
				)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := materializeOperation(tc.op)
			require.Equal(t, tc.want(got), rawSQEBytes(got))
		})
	}
}

func signedArgument(value int) uint64 {
	return uint64(int64(value))
}
