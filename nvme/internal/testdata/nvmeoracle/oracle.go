//go:build linux && cgo

package nvmeoracle

/*
#include <stdint.h>
#include <sys/ioctl.h>
#include <string.h>
#include <linux/nvme_ioctl.h>

static void rw(void *out, uint32_t nsid, uint64_t slba, uint32_t bytes, uint32_t cdw12) {
 struct nvme_uring_cmd c = {
  .opcode = 1, .nsid = nsid, .data_len = bytes,
  .cdw10 = (uint32_t)slba, .cdw11 = slba >> 32, .cdw12 = cdw12,
 };
 memcpy(out, &c, sizeof(c));
}
*/
import "C"
import "unsafe"

func RW(nsid uint32, slba uint64, bytes, cdw12 uint32) (out [72]byte) {
	if C.sizeof_struct_nvme_uring_cmd != len(out) {
		panic("nvme_uring_cmd ABI size changed")
	}
	C.rw(unsafe.Pointer(&out), C.uint32_t(nsid), C.uint64_t(slba), C.uint32_t(bytes), C.uint32_t(cdw12))
	return out
}
func IOCommand() uint32    { return uint32(C.NVME_URING_CMD_IO) }
func AdminCommand() uint32 { return uint32(C.NVME_URING_CMD_ADMIN) }

func NamespaceIDIoctl() uint { return uint(C.NVME_IOCTL_ID) }
func AdminIoctl() uint       { return uint(C.NVME_IOCTL_ADMIN_CMD) }
