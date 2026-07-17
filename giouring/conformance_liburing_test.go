//go:build linux && cgo && liburing_conformance

package giouring

import (
	"syscall"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func checkLiburingSQE(t *testing.T, name string, got SubmissionQueueEntry, want SubmissionQueueEntry) {
	t.Helper()
	require.Equal(t, want, got, "%s SQE", name)
}

func TestDIOPreparationMatchesLiburing(t *testing.T) {
	buf := make([]byte, 32)
	path := []byte("file.dat\x00")
	iovecs := make([]syscall.Iovec, 2)
	bufAddr := slicePtr(buf)
	pathAddr := slicePtr(path)
	iovecsAddr := slicePtr(iovecs)
	dfd32 := int32(-100)
	dfd := uint64(uint32(dfd32))

	tests := []struct {
		name string
		got  func() SubmissionQueueEntry
		want func() SubmissionQueueEntry
	}{
		{"read", func() (s SubmissionQueueEntry) { s.PrepareRead(11, bufAddr, 32, 17); return }, func() SubmissionQueueEntry { return liburingOracle(oracleRead, 11, uint64(bufAddr), 32, 17) }},
		{"write", func() (s SubmissionQueueEntry) { s.PrepareWrite(11, bufAddr, 32, 17); return }, func() SubmissionQueueEntry { return liburingOracle(oracleWrite, 11, uint64(bufAddr), 32, 17) }},
		{"readv", func() (s SubmissionQueueEntry) { s.PrepareReadv(11, iovecsAddr, 2, 17); return }, func() SubmissionQueueEntry { return liburingOracle(oracleReadv, 11, uint64(iovecsAddr), 2, 17) }},
		{"writev", func() (s SubmissionQueueEntry) { s.PrepareWritev(11, iovecsAddr, 2, 17); return }, func() SubmissionQueueEntry { return liburingOracle(oracleWritev, 11, uint64(iovecsAddr), 2, 17) }},
		{"fsync", func() (s SubmissionQueueEntry) { s.PrepareFsync(11, FsyncDatasync); return }, func() SubmissionQueueEntry { return liburingOracle(oracleFsync, 11, uint64(FsyncDatasync)) }},
		{"fallocate", func() (s SubmissionQueueEntry) { s.PrepareFallocate(11, 2, 17, 4096); return }, func() SubmissionQueueEntry { return liburingOracle(oracleFallocate, 11, 2, 17, 4096) }},
		{"openat", func() (s SubmissionQueueEntry) { s.PrepareOpenat(-100, path, 0x42, 0o600); return }, func() SubmissionQueueEntry { return liburingOracle(oracleOpenat, dfd, uint64(pathAddr), 0x42, 0o600) }},
		{"openat-direct", func() (s SubmissionQueueEntry) { s.PrepareOpenatDirect(-100, path, 0x42, 0o600, 7); return }, func() SubmissionQueueEntry {
			return liburingOracle(oracleOpenatDirect, dfd, uint64(pathAddr), 0x42, 0o600, 7)
		}},
		{"close", func() (s SubmissionQueueEntry) { s.PrepareClose(11); return }, func() SubmissionQueueEntry { return liburingOracle(oracleClose, 11) }},
		{"close-direct", func() (s SubmissionQueueEntry) { s.PrepareCloseDirect(7); return }, func() SubmissionQueueEntry { return liburingOracle(oracleCloseDirect, 7) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { checkLiburingSQE(t, tc.name, tc.got(), tc.want()) })
	}
}

func TestCorrectedPreparationMatchesLiburing(t *testing.T) {
	name := []byte("user.key\x00")
	value := make([]byte, 32)
	path := []byte("file.dat\x00")
	path2 := []byte("other.dat\x00")
	fds := []int32{3, 4}
	spec := syscall.Timespec{Sec: 1, Nsec: 2}
	statx := new(unix.Statx_t)
	sockaddr := make([]byte, 32)
	buffer := make([]byte, 64)
	openHow := &unix.OpenHow{Flags: unix.O_RDONLY, Mode: 0o600, Resolve: 1}
	optVal := make([]byte, 16)

	ptr := func(p unsafe.Pointer) uint64 { return uint64(uintptr(p)) }
	tests := []struct {
		name string
		got  func() SubmissionQueueEntry
		want func() SubmissionQueueEntry
	}{
		{"fgetxattr", func() (s SubmissionQueueEntry) { s.PrepareFgetxattr(9, name, value); return }, func() SubmissionQueueEntry {
			return liburingOracle(oracleFgetxattr, 9, uint64(slicePtr(name)), uint64(slicePtr(value)), uint64(len(value)))
		}},
		{"fsetxattr", func() (s SubmissionQueueEntry) { s.PrepareFsetxattr(9, name, value, 3); return }, func() SubmissionQueueEntry {
			return liburingOracle(oracleFsetxattr, 9, uint64(slicePtr(name)), uint64(slicePtr(value)), 3, uint64(len(value)))
		}},
		{"getxattr", func() (s SubmissionQueueEntry) { s.PrepareGetxattr(name, value, path); return }, func() SubmissionQueueEntry {
			return liburingOracle(oracleGetxattr, uint64(slicePtr(name)), uint64(slicePtr(value)), uint64(slicePtr(path)), uint64(len(value)))
		}},
		{"setxattr", func() (s SubmissionQueueEntry) { s.PrepareSetxattr(name, value, path, 3, uint32(len(value))); return }, func() SubmissionQueueEntry {
			return liburingOracle(oracleSetxattr, uint64(slicePtr(name)), uint64(slicePtr(value)), uint64(slicePtr(path)), 3, uint64(len(value)))
		}},
		{"files-update", func() (s SubmissionQueueEntry) { s.PrepareFilesUpdate(fds, 5); return }, func() SubmissionQueueEntry {
			return liburingOracle(oracleFilesUpdate, uint64(slicePtr(fds)), uint64(len(fds)), 5)
		}},
		{"link-timeout", func() (s SubmissionQueueEntry) { s.PrepareLinkTimeout(&spec, 3); return }, func() SubmissionQueueEntry { return liburingOracle(oracleLinkTimeout, ptr(unsafe.Pointer(&spec)), 3) }},
		{"timeout", func() (s SubmissionQueueEntry) { s.PrepareTimeout(&spec, 2, 3); return }, func() SubmissionQueueEntry { return liburingOracle(oracleTimeout, ptr(unsafe.Pointer(&spec)), 2, 3) }},
		{"timeout-remove", func() (s SubmissionQueueEntry) { s.PrepareTimeoutRemove(0x1234, 3); return }, func() SubmissionQueueEntry { return liburingOracle(oracleTimeoutRemove, 0x1234, 3) }},
		{"timeout-update", func() (s SubmissionQueueEntry) { s.PrepareTimeoutUpdate(&spec, 0x1234, 3); return }, func() SubmissionQueueEntry {
			return liburingOracle(oracleTimeoutUpdate, ptr(unsafe.Pointer(&spec)), 0x1234, 3)
		}},
		{"linkat", func() (s SubmissionQueueEntry) { s.PrepareLinkat(3, path, 4, path2, 5); return }, func() SubmissionQueueEntry {
			return liburingOracle(oracleLinkat, 3, uint64(slicePtr(path)), 4, uint64(slicePtr(path2)), 5)
		}},
		{"mkdirat", func() (s SubmissionQueueEntry) { s.PrepareMkdirat(3, path, 0o750); return }, func() SubmissionQueueEntry { return liburingOracle(oracleMkdirat, 3, uint64(slicePtr(path)), 0o750) }},
		{"renameat", func() (s SubmissionQueueEntry) { s.PrepareRenameat(3, path, 4, path2, 5); return }, func() SubmissionQueueEntry {
			return liburingOracle(oracleRenameat, 3, uint64(slicePtr(path)), 4, uint64(slicePtr(path2)), 5)
		}},
		{"symlinkat", func() (s SubmissionQueueEntry) { s.PrepareSymlinkat(path, 4, path2); return }, func() SubmissionQueueEntry {
			return liburingOracle(oracleSymlinkat, uint64(slicePtr(path)), 4, uint64(slicePtr(path2)))
		}},
		{"statx", func() (s SubmissionQueueEntry) { s.PrepareStatx(3, path, 5, 7, statx); return }, func() SubmissionQueueEntry {
			return liburingOracle(oracleStatx, 3, uint64(slicePtr(path)), 5, 7, ptr(unsafe.Pointer(statx)))
		}},
		{"send-zc", func() (s SubmissionQueueEntry) { s.PrepareSendZC(3, buffer, 5, 7); return }, func() SubmissionQueueEntry {
			return liburingOracle(oracleSendZC, 3, uint64(slicePtr(buffer)), uint64(len(buffer)), 5, 7)
		}},
		{"sendto", func() (s SubmissionQueueEntry) {
			s.PrepareSendto(3, buffer, 5, slicePtr(sockaddr), uint32(len(sockaddr)))
			return
		}, func() SubmissionQueueEntry {
			return liburingOracle(oracleSendto, 3, uint64(slicePtr(buffer)), uint64(len(buffer)), 5, uint64(slicePtr(sockaddr)), uint64(len(sockaddr)))
		}},
		{"msg-ring-fd", func() (s SubmissionQueueEntry) { s.PrepareMsgRingFd(3, 4, 5, 0x1234, 7); return }, func() SubmissionQueueEntry { return liburingOracle(oracleMsgRingFd, 3, 4, 5, 0x1234, 7) }},
		{"connect", func() (s SubmissionQueueEntry) {
			s.PrepareConnect(3, slicePtr(sockaddr), uint64(len(sockaddr)))
			return
		}, func() SubmissionQueueEntry {
			return liburingOracle(oracleConnect, 3, uint64(slicePtr(sockaddr)), uint64(len(sockaddr)))
		}},
		{"openat2", func() (s SubmissionQueueEntry) { s.PrepareOpenat2(3, path, openHow); return }, func() SubmissionQueueEntry {
			return liburingOracle(oracleOpenat2, 3, uint64(slicePtr(path)), ptr(unsafe.Pointer(openHow)))
		}},
		{"cmd-sock", func() (s SubmissionQueueEntry) {
			s.PrepareCmdSock(4, 3, 1, 2, unsafe.Pointer(unsafe.SliceData(optVal)), len(optVal))
			return
		}, func() SubmissionQueueEntry {
			return liburingOracle(oracleCmdSock, 4, 3, 1, 2, uint64(slicePtr(optVal)), uint64(len(optVal)))
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { checkLiburingSQE(t, tc.name, tc.got(), tc.want()) })
	}
}

func TestAllocHugeNoSQArrayMemorySizeMatchesLiburing(t *testing.T) {
	const entries = uint32(1024)
	// io_uring_memory_size_params is an exact oracle for NO_SQARRAY layouts.
	// Some released liburing versions omit the SQ-array page from this public
	// size even though their private allocator reserves it, so layouts with an
	// SQ array are covered by TestAllocHugeUsesKernelEntrySizes instead.
	tests := []struct {
		name      string
		flags     uint32
		cqEntries uint32
	}{
		{name: "no SQ array", flags: SetupNoSQArray},
		{name: "large entries", flags: SetupNoSQArray | SetupSQE128 | SetupCQE32},
		{name: "explicit CQ size", flags: SetupNoSQArray | SetupCQSize, cqEntries: 4096},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, 512*1024)
			p := &Params{flags: tc.flags, cqEntries: tc.cqEntries}
			ring := NewRing()

			got, err := allocHuge(entries, p, ring.sqRing, ring.cqRing,
				unsafe.Pointer(unsafe.SliceData(buf)), uint64(len(buf)))

			require.NoError(t, err)
			require.Equal(t, liburingMemorySizeOracle(entries, tc.flags, tc.cqEntries), int64(got))
		})
	}
}
