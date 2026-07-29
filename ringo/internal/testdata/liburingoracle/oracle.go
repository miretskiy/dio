//go:build linux && cgo

package liburingoracle

/*
#cgo CFLAGS: -D_GNU_SOURCE -I${SRCDIR}/include

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <liburing.h>

static void clear_sqe(struct io_uring_sqe *sqe) {
	memset(sqe, 0, sizeof(*sqe));
}

static void oracle_prep_nop(struct io_uring_sqe *sqe) {
	clear_sqe(sqe);
	io_uring_prep_nop(sqe);
}

static void oracle_prep_read(struct io_uring_sqe *sqe, int fd, void *buf,
	uint32_t nbytes, uint64_t offset) {
	clear_sqe(sqe);
	io_uring_prep_read(sqe, fd, buf, nbytes, offset);
}

static void oracle_prep_read_fixed(struct io_uring_sqe *sqe, int fd, void *buf,
	uint32_t nbytes, uint64_t offset, int buf_index) {
	clear_sqe(sqe);
	io_uring_prep_read_fixed(sqe, fd, buf, nbytes, offset, buf_index);
}

static void oracle_prep_readv(struct io_uring_sqe *sqe, int fd, void *iovecs,
	uint32_t nr_vecs, uint64_t offset) {
	clear_sqe(sqe);
	io_uring_prep_readv(sqe, fd, (const struct iovec *)iovecs, nr_vecs, offset);
}

static void oracle_prep_write(struct io_uring_sqe *sqe, int fd, const void *buf,
	uint32_t nbytes, uint64_t offset) {
	clear_sqe(sqe);
	io_uring_prep_write(sqe, fd, buf, nbytes, offset);
}

static void oracle_prep_write_fixed(struct io_uring_sqe *sqe, int fd, const void *buf,
	uint32_t nbytes, uint64_t offset, int buf_index) {
	clear_sqe(sqe);
	io_uring_prep_write_fixed(sqe, fd, buf, nbytes, offset, buf_index);
}

static void oracle_prep_writev(struct io_uring_sqe *sqe, int fd, void *iovecs,
	uint32_t nr_vecs, uint64_t offset) {
	clear_sqe(sqe);
	io_uring_prep_writev(sqe, fd, (const struct iovec *)iovecs, nr_vecs, offset);
}

static void oracle_prep_fsync(struct io_uring_sqe *sqe, int fd, uint32_t flags) {
	clear_sqe(sqe);
	io_uring_prep_fsync(sqe, fd, flags);
}

static void oracle_prep_fallocate(struct io_uring_sqe *sqe, int fd, int mode,
	uint64_t offset, uint64_t length) {
	clear_sqe(sqe);
	io_uring_prep_fallocate(sqe, fd, mode, offset, length);
}

static void oracle_prep_openat(struct io_uring_sqe *sqe, int dfd, const char *path,
	int flags, uint32_t mode) {
	clear_sqe(sqe);
	io_uring_prep_openat(sqe, dfd, path, flags, mode);
}

static void oracle_prep_openat_direct(struct io_uring_sqe *sqe, int dfd, const char *path,
	int flags, uint32_t mode, uint32_t file_index) {
	clear_sqe(sqe);
	io_uring_prep_openat_direct(sqe, dfd, path, flags, mode, file_index);
}

static void oracle_prep_close_direct(struct io_uring_sqe *sqe, uint32_t file_index) {
	clear_sqe(sqe);
	io_uring_prep_close_direct(sqe, file_index);
}

static void oracle_prepare(struct io_uring_sqe *sqe, int kind,
	uint64_t a0, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5) {
	clear_sqe(sqe);
	switch (kind) {
	case 5: io_uring_prep_link_timeout(sqe, (struct __kernel_timespec *)(uintptr_t)a0, a1); break;
	case 6: io_uring_prep_timeout(sqe, (struct __kernel_timespec *)(uintptr_t)a0, a1, a2); break;
	case 7: io_uring_prep_timeout_remove(sqe, a0, a1); break;
	case 8: io_uring_prep_timeout_update(sqe, (struct __kernel_timespec *)(uintptr_t)a0, a1, a2); break;
	case 13: io_uring_prep_statx(sqe, a0, (char *)(uintptr_t)a1, a2, a3, (struct statx *)(uintptr_t)a4); break;
	case 17: io_uring_prep_openat2(sqe, a0, (char *)(uintptr_t)a1, (struct open_how *)(uintptr_t)a2); break;
	case 24: io_uring_prep_poll_add(sqe, a0, a1); break;
	case 25: io_uring_prep_poll_multishot(sqe, a0, a1); break;
	case 26: io_uring_prep_poll_remove(sqe, a0); break;
	case 27: io_uring_prep_poll_update(sqe, a0, a1, a2, a3); break;
	case 32: io_uring_prep_cancel64(sqe, a0, a1); break;
	case 33: io_uring_prep_cancel_fd(sqe, a0, a1); break;
	case 62: io_uring_prep_readv_fixed(sqe, a0, (struct iovec *)(uintptr_t)a1, a2, a3, a4, a5); break;
	case 63: io_uring_prep_writev_fixed(sqe, a0, (struct iovec *)(uintptr_t)a1, a2, a3, a4, a5); break;
	case 67: io_uring_prep_ftruncate(sqe, a0, a1); break;
	case 68: io_uring_prep_close(sqe, a0); break;
	}
}

struct retained_abi_constants {
	uint32_t sqe_flags[4];
	uint32_t setup_flags[10];
	uint32_t cqe_flags[1];
	uint32_t features[3];
	uint32_t sq_flags[3];
	uint32_t enter_flags[2];
	uint32_t register_ops[9];
	uint32_t rsrc_register_sparse;
	uint32_t cancel_flags[4];
	uint64_t off_sq_ring;
	uint64_t off_cq_ring;
	uint64_t off_sqes;
};

static struct retained_abi_constants retained_constants(void) {
	return (struct retained_abi_constants) {
		.sqe_flags = {
			IOSQE_FIXED_FILE,
				IOSQE_IO_DRAIN,
				IOSQE_IO_LINK,
				IOSQE_IO_HARDLINK,
			},
		.setup_flags = {
			IORING_SETUP_IOPOLL,
			IORING_SETUP_SQPOLL,
			IORING_SETUP_SQ_AFF,
				IORING_SETUP_CQSIZE,
				IORING_SETUP_CLAMP,
				IORING_SETUP_SUBMIT_ALL,
				IORING_SETUP_COOP_TASKRUN,
				IORING_SETUP_TASKRUN_FLAG,
				IORING_SETUP_NO_SQARRAY,
				IORING_SETUP_HYBRID_IOPOLL,
			},
			.cqe_flags = {
				IORING_CQE_F_MORE,
			},
			.features = {
				IORING_FEAT_SINGLE_MMAP,
				IORING_FEAT_NODROP,
				IORING_FEAT_LINKED_FILE,
			},
		.sq_flags = {
			IORING_SQ_NEED_WAKEUP,
			IORING_SQ_CQ_OVERFLOW,
			IORING_SQ_TASKRUN,
		},
			.enter_flags = {
				IORING_ENTER_GETEVENTS,
				IORING_ENTER_SQ_WAKEUP,
			},
			.register_ops = {
				IORING_REGISTER_BUFFERS,
				IORING_REGISTER_FILES,
				IORING_REGISTER_FILES_UPDATE,
				IORING_REGISTER_EVENTFD,
				IORING_UNREGISTER_EVENTFD,
				IORING_REGISTER_EVENTFD_ASYNC,
				IORING_REGISTER_PROBE,
				IORING_REGISTER_FILES2,
				IORING_REGISTER_SYNC_CANCEL,
			},
		.rsrc_register_sparse = IORING_RSRC_REGISTER_SPARSE,
		.cancel_flags = {
			IORING_ASYNC_CANCEL_ALL,
			IORING_ASYNC_CANCEL_FD,
			IORING_ASYNC_CANCEL_ANY,
			IORING_ASYNC_CANCEL_FD_FIXED,
		},
		.off_sq_ring = IORING_OFF_SQ_RING,
		.off_cq_ring = IORING_OFF_CQ_RING,
		.off_sqes = IORING_OFF_SQES,
	};
}

static size_t abi_size_sqe(void) { return sizeof(struct io_uring_sqe); }
static size_t abi_size_cqe(void) { return sizeof(struct io_uring_cqe); }
static size_t abi_size_params(void) { return sizeof(struct io_uring_params); }
static size_t abi_size_sq_offsets(void) { return sizeof(struct io_sqring_offsets); }
static size_t abi_size_cq_offsets(void) { return sizeof(struct io_cqring_offsets); }
static size_t abi_size_files_update(void) { return sizeof(struct io_uring_files_update); }
static size_t abi_size_rsrc_register(void) { return sizeof(struct io_uring_rsrc_register); }
static size_t abi_size_sync_cancel_reg(void) { return sizeof(struct io_uring_sync_cancel_reg); }
static size_t abi_size_probe_op(void) { return sizeof(struct io_uring_probe_op); }
*/
import "C"

import (
	"syscall"
	"unsafe"
)

// SQE is the raw 64-byte io_uring submission queue entry emitted by liburing.
type SQE [64]byte

func PrepareNop() SQE {
	var sqe SQE
	C.oracle_prep_nop((*C.struct_io_uring_sqe)(unsafe.Pointer(&sqe)))
	return sqe
}

func PrepareRead(fd int, buf []byte, nbytes uint32, offset uint64) SQE {
	var sqe SQE
	C.oracle_prep_read(
		(*C.struct_io_uring_sqe)(unsafe.Pointer(&sqe)), C.int(fd),
		unsafe.Pointer(unsafe.SliceData(buf)), C.uint32_t(nbytes), C.uint64_t(offset),
	)
	return sqe
}

func PrepareReadFixed(fd int, buf []byte, nbytes uint32, offset uint64, bufIndex int) SQE {
	var sqe SQE
	C.oracle_prep_read_fixed(
		(*C.struct_io_uring_sqe)(unsafe.Pointer(&sqe)), C.int(fd),
		unsafe.Pointer(unsafe.SliceData(buf)), C.uint32_t(nbytes), C.uint64_t(offset), C.int(bufIndex),
	)
	return sqe
}

func PrepareReadv(fd int, iovecs []syscall.Iovec, offset uint64) SQE {
	var sqe SQE
	C.oracle_prep_readv(
		(*C.struct_io_uring_sqe)(unsafe.Pointer(&sqe)), C.int(fd),
		unsafe.Pointer(unsafe.SliceData(iovecs)), C.uint32_t(len(iovecs)), C.uint64_t(offset),
	)
	return sqe
}

func PrepareWrite(fd int, buf []byte, nbytes uint32, offset uint64) SQE {
	var sqe SQE
	C.oracle_prep_write(
		(*C.struct_io_uring_sqe)(unsafe.Pointer(&sqe)), C.int(fd),
		unsafe.Pointer(unsafe.SliceData(buf)), C.uint32_t(nbytes), C.uint64_t(offset),
	)
	return sqe
}

func PrepareWriteFixed(fd int, buf []byte, nbytes uint32, offset uint64, bufIndex int) SQE {
	var sqe SQE
	C.oracle_prep_write_fixed(
		(*C.struct_io_uring_sqe)(unsafe.Pointer(&sqe)), C.int(fd),
		unsafe.Pointer(unsafe.SliceData(buf)), C.uint32_t(nbytes), C.uint64_t(offset), C.int(bufIndex),
	)
	return sqe
}

func PrepareWritev(fd int, iovecs []syscall.Iovec, offset uint64) SQE {
	var sqe SQE
	C.oracle_prep_writev(
		(*C.struct_io_uring_sqe)(unsafe.Pointer(&sqe)), C.int(fd),
		unsafe.Pointer(unsafe.SliceData(iovecs)), C.uint32_t(len(iovecs)), C.uint64_t(offset),
	)
	return sqe
}

func PrepareFsync(fd int, flags uint32) SQE {
	var sqe SQE
	C.oracle_prep_fsync(
		(*C.struct_io_uring_sqe)(unsafe.Pointer(&sqe)), C.int(fd), C.uint32_t(flags),
	)
	return sqe
}

func PrepareFallocate(fd int, mode int, offset, length uint64) SQE {
	var sqe SQE
	C.oracle_prep_fallocate(
		(*C.struct_io_uring_sqe)(unsafe.Pointer(&sqe)), C.int(fd), C.int(mode),
		C.uint64_t(offset), C.uint64_t(length),
	)
	return sqe
}

func PrepareOpenat(dfd int, path []byte, flags int, mode uint32) SQE {
	var sqe SQE
	C.oracle_prep_openat(
		(*C.struct_io_uring_sqe)(unsafe.Pointer(&sqe)), C.int(dfd),
		(*C.char)(unsafe.Pointer(unsafe.SliceData(path))), C.int(flags), C.uint32_t(mode),
	)
	return sqe
}

func PrepareOpenatDirect(dfd int, path []byte, flags int, mode, fileIndex uint32) SQE {
	var sqe SQE
	C.oracle_prep_openat_direct(
		(*C.struct_io_uring_sqe)(unsafe.Pointer(&sqe)), C.int(dfd),
		(*C.char)(unsafe.Pointer(unsafe.SliceData(path))), C.int(flags),
		C.uint32_t(mode), C.uint32_t(fileIndex),
	)
	return sqe
}

func PrepareCloseDirect(fileIndex uint32) SQE {
	var sqe SQE
	C.oracle_prep_close_direct(
		(*C.struct_io_uring_sqe)(unsafe.Pointer(&sqe)), C.uint32_t(fileIndex),
	)
	return sqe
}

// PrepareKind selects one of Ringo's liburing preparation helpers.
type PrepareKind int

const (
	PrepareLinkTimeout   PrepareKind = 5
	PrepareTimeout       PrepareKind = 6
	PrepareTimeoutRemove PrepareKind = 7
	PrepareTimeoutUpdate PrepareKind = 8
	PrepareStatx         PrepareKind = 13
	PrepareOpenat2       PrepareKind = 17
	PreparePollAdd       PrepareKind = 24
	PreparePollMultishot PrepareKind = 25
	PreparePollRemove    PrepareKind = 26
	PreparePollUpdate    PrepareKind = 27
	PrepareCancel        PrepareKind = 32
	PrepareCancelFD      PrepareKind = 33
	PrepareReadvFixed    PrepareKind = 62
	PrepareWritevFixed   PrepareKind = 63
	PrepareFtruncate     PrepareKind = 67
	PrepareClose         PrepareKind = 68
)

// Prepare returns the raw SQE emitted by the selected liburing helper.
func Prepare(kind PrepareKind, arguments ...uint64) SQE {
	var padded [6]uint64
	copy(padded[:], arguments)
	var sqe SQE
	C.oracle_prepare(
		(*C.struct_io_uring_sqe)(unsafe.Pointer(&sqe)),
		C.int(kind),
		C.uint64_t(padded[0]),
		C.uint64_t(padded[1]),
		C.uint64_t(padded[2]),
		C.uint64_t(padded[3]),
		C.uint64_t(padded[4]),
		C.uint64_t(padded[5]),
	)
	return sqe
}

type RetainedConstants struct {
	SQEFlags    [4]uint32
	SetupFlags  [10]uint32
	CQEFlags    [1]uint32
	Features    [3]uint32
	SQFlags     [3]uint32
	EnterFlags  [2]uint32
	RegisterOps [9]uint32
	RsrcSparse  uint32
	CancelFlags [4]uint32
	MmapOffsets [3]uint64
}

func Constants() RetainedConstants {
	constants := C.retained_constants()
	result := RetainedConstants{
		RsrcSparse: uint32(constants.rsrc_register_sparse),
		MmapOffsets: [3]uint64{
			uint64(constants.off_sq_ring),
			uint64(constants.off_cq_ring),
			uint64(constants.off_sqes),
		},
	}
	for index := range result.SQEFlags {
		result.SQEFlags[index] = uint32(constants.sqe_flags[index])
	}
	for index := range result.SetupFlags {
		result.SetupFlags[index] = uint32(constants.setup_flags[index])
	}
	for index := range result.CQEFlags {
		result.CQEFlags[index] = uint32(constants.cqe_flags[index])
	}
	for index := range result.Features {
		result.Features[index] = uint32(constants.features[index])
	}
	for index := range result.SQFlags {
		result.SQFlags[index] = uint32(constants.sq_flags[index])
	}
	for index := range result.EnterFlags {
		result.EnterFlags[index] = uint32(constants.enter_flags[index])
	}
	for index := range result.RegisterOps {
		result.RegisterOps[index] = uint32(constants.register_ops[index])
	}
	for index := range result.CancelFlags {
		result.CancelFlags[index] = uint32(constants.cancel_flags[index])
	}
	return result
}

type ABISizes struct {
	SQE              uintptr
	CQE              uintptr
	Params           uintptr
	SQOffsets        uintptr
	CQOffsets        uintptr
	FilesUpdate      uintptr
	ResourceRegister uintptr
	SyncCancel       uintptr
	ProbeOperation   uintptr
}

func Sizes() ABISizes {
	return ABISizes{
		SQE:              uintptr(C.abi_size_sqe()),
		CQE:              uintptr(C.abi_size_cqe()),
		Params:           uintptr(C.abi_size_params()),
		SQOffsets:        uintptr(C.abi_size_sq_offsets()),
		CQOffsets:        uintptr(C.abi_size_cq_offsets()),
		FilesUpdate:      uintptr(C.abi_size_files_update()),
		ResourceRegister: uintptr(C.abi_size_rsrc_register()),
		SyncCancel:       uintptr(C.abi_size_sync_cancel_reg()),
		ProbeOperation:   uintptr(C.abi_size_probe_op()),
	}
}
