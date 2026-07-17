//go:build linux && cgo && liburing_conformance

package giouring

/*
#cgo LDFLAGS: -luring
#include <stdint.h>
#include <string.h>
#include <liburing.h>

static void prepare_oracle(struct io_uring_sqe *sqe, int kind,
	uint64_t a0, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5) {
	memset(sqe, 0, sizeof(*sqe));
	switch (kind) {
	case 0: io_uring_prep_read(sqe, a0, (void *)(uintptr_t)a1, a2, a3); break;
	case 1: io_uring_prep_write(sqe, a0, (void *)(uintptr_t)a1, a2, a3); break;
	case 2: io_uring_prep_readv(sqe, a0, (void *)(uintptr_t)a1, a2, a3); break;
	case 3: io_uring_prep_writev(sqe, a0, (void *)(uintptr_t)a1, a2, a3); break;
	case 4: io_uring_prep_fsync(sqe, a0, a1); break;
	case 5: io_uring_prep_fallocate(sqe, a0, a1, a2, a3); break;
	case 6: io_uring_prep_openat(sqe, a0, (char *)(uintptr_t)a1, a2, a3); break;
	case 7: io_uring_prep_openat_direct(sqe, a0, (char *)(uintptr_t)a1, a2, a3, a4); break;
	case 8: io_uring_prep_close(sqe, a0); break;
	case 9: io_uring_prep_close_direct(sqe, a0); break;
	case 10: io_uring_prep_fgetxattr(sqe, a0, (char *)(uintptr_t)a1, (char *)(uintptr_t)a2, a3); break;
	case 11: io_uring_prep_fsetxattr(sqe, a0, (char *)(uintptr_t)a1, (char *)(uintptr_t)a2, a3, a4); break;
	case 12: io_uring_prep_getxattr(sqe, (char *)(uintptr_t)a0, (char *)(uintptr_t)a1, (char *)(uintptr_t)a2, a3); break;
	case 13: io_uring_prep_setxattr(sqe, (char *)(uintptr_t)a0, (char *)(uintptr_t)a1, (char *)(uintptr_t)a2, a3, a4); break;
	case 14: io_uring_prep_files_update(sqe, (int *)(uintptr_t)a0, a1, a2); break;
	case 15: io_uring_prep_link_timeout(sqe, (struct __kernel_timespec *)(uintptr_t)a0, a1); break;
	case 16: io_uring_prep_timeout(sqe, (struct __kernel_timespec *)(uintptr_t)a0, a1, a2); break;
	case 17: io_uring_prep_timeout_remove(sqe, a0, a1); break;
	case 18: io_uring_prep_timeout_update(sqe, (struct __kernel_timespec *)(uintptr_t)a0, a1, a2); break;
	case 19: io_uring_prep_linkat(sqe, a0, (char *)(uintptr_t)a1, a2, (char *)(uintptr_t)a3, a4); break;
	case 20: io_uring_prep_mkdirat(sqe, a0, (char *)(uintptr_t)a1, a2); break;
	case 21: io_uring_prep_renameat(sqe, a0, (char *)(uintptr_t)a1, a2, (char *)(uintptr_t)a3, a4); break;
	case 22: io_uring_prep_symlinkat(sqe, (char *)(uintptr_t)a0, a1, (char *)(uintptr_t)a2); break;
	case 23: io_uring_prep_statx(sqe, a0, (char *)(uintptr_t)a1, a2, a3, (struct statx *)(uintptr_t)a4); break;
	case 24: io_uring_prep_send_zc(sqe, a0, (void *)(uintptr_t)a1, a2, a3, a4); break;
	case 25: io_uring_prep_sendto(sqe, a0, (void *)(uintptr_t)a1, a2, a3, (struct sockaddr *)(uintptr_t)a4, a5); break;
	case 26: io_uring_prep_msg_ring_fd(sqe, a0, a1, a2, a3, a4); break;
	case 27: io_uring_prep_connect(sqe, a0, (struct sockaddr *)(uintptr_t)a1, a2); break;
	case 28: io_uring_prep_openat2(sqe, a0, (char *)(uintptr_t)a1, (struct open_how *)(uintptr_t)a2); break;
	case 29: io_uring_prep_cmd_sock(sqe, a0, a1, a2, a3, (void *)(uintptr_t)a4, a5); break;
	}
}

static int64_t memory_size_oracle(uint32_t entries, uint32_t flags, uint32_t cq_entries) {
	struct io_uring_params p;
	memset(&p, 0, sizeof(p));
	p.flags = flags;
	p.cq_entries = cq_entries;
	return io_uring_memory_size_params(entries, &p);
}
*/
import "C"

import "unsafe"

type liburingOracleKind int

const (
	oracleRead liburingOracleKind = iota
	oracleWrite
	oracleReadv
	oracleWritev
	oracleFsync
	oracleFallocate
	oracleOpenat
	oracleOpenatDirect
	oracleClose
	oracleCloseDirect
	oracleFgetxattr
	oracleFsetxattr
	oracleGetxattr
	oracleSetxattr
	oracleFilesUpdate
	oracleLinkTimeout
	oracleTimeout
	oracleTimeoutRemove
	oracleTimeoutUpdate
	oracleLinkat
	oracleMkdirat
	oracleRenameat
	oracleSymlinkat
	oracleStatx
	oracleSendZC
	oracleSendto
	oracleMsgRingFd
	oracleConnect
	oracleOpenat2
	oracleCmdSock
)

func liburingOracle(kind liburingOracleKind, args ...uint64) SubmissionQueueEntry {
	var padded [6]uint64
	copy(padded[:], args)
	var sqe SubmissionQueueEntry
	C.prepare_oracle(
		(*C.struct_io_uring_sqe)(unsafe.Pointer(&sqe)), C.int(kind),
		C.uint64_t(padded[0]), C.uint64_t(padded[1]), C.uint64_t(padded[2]),
		C.uint64_t(padded[3]), C.uint64_t(padded[4]), C.uint64_t(padded[5]),
	)
	return sqe
}

func liburingMemorySizeOracle(entries, flags, cqEntries uint32) int64 {
	return int64(C.memory_size_oracle(C.uint32_t(entries), C.uint32_t(flags), C.uint32_t(cqEntries)))
}
