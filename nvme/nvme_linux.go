//go:build linux

package nvme

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"unsafe"

	"github.com/miretskiy/dio/v2/ringo"
	"golang.org/x/sys/unix"
)

func identify(f *os.File) (Info, error) {
	var info Info
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		return info, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFCHR {
		return info, fmt.Errorf("nvme: expected namespace character device, got %s", f.Name())
	}
	sys, err := filepath.EvalSymlinks(fmt.Sprintf("/sys/dev/char/%d:%d", unix.Major(uint64(st.Rdev)), unix.Minor(uint64(st.Rdev))))
	if err != nil {
		return info, err
	}
	// Match the character and block interfaces by their sysfs identity and NSID.
	name := filepath.Base(sys)
	if _, err := os.Stat(filepath.Join("/sys/class/nvme-generic", name)); err != nil {
		return info, fmt.Errorf("nvme: %s is not a generic namespace device", f.Name())
	}
	info.NSID, err = namespaceID(f)
	if err != nil {
		return info, err
	}
	blockSys, err := blockForNamespace(sys, info.NSID)
	if err != nil {
		return info, err
	}
	csi, err := readUint(filepath.Join(blockSys, "csi"))
	if err != nil {
		return info, fmt.Errorf("nvme: read command set: %w", err)
	}
	if csi != 0 {
		return info, fmt.Errorf("nvme: command set %d is unsupported (requires ordinary NVM)", csi)
	}
	depth, err := readUint(filepath.Join(blockSys, "queue", "nr_requests"))
	if err != nil {
		return info, fmt.Errorf("nvme: read queue depth: %w", err)
	}
	info.QueueDepth = uint32(depth)
	info.BlockDevice, info.CharDevice = "/dev/"+filepath.Base(blockSys), "/dev/"+name
	ns, err := identifyPage(f, info.NSID, 0)
	if err != nil {
		return info, err
	}
	ctrlPage, err := identifyPage(f, 0, 1)
	if err != nil {
		return info, err
	}
	info, err = parseIdentify(info, ns, ctrlPage)
	if err != nil {
		return info, err
	}
	// Linux derives max_hw_sectors_kb from MDTS and the controller's minimum
	// memory page size (CAP.MPSMIN), including on hosts with larger memory pages.
	hw, err := readUint(filepath.Join(blockSys, "queue/max_hw_sectors_kb"))
	if err != nil {
		return info, err
	}
	sw, err := readUint(filepath.Join(blockSys, "queue/max_sectors_kb"))
	if err != nil {
		return info, err
	}
	segments, err := readUint(filepath.Join(blockSys, "queue/max_segments"))
	if err != nil {
		return info, err
	}
	if hw == 0 || sw == 0 || segments == 0 {
		return info, fmt.Errorf("nvme: zero kernel transfer limit")
	}
	limit := min(min(hw, sw)*1024, uint64(65536)*uint64(info.BlockSize), uint64(math.MaxUint32))
	// Allow a partial first page; every remaining segment can be a page.
	limit = min(limit, (segments-1)*uint64(os.Getpagesize())+1)
	limit -= limit % uint64(info.BlockSize)
	if limit == 0 {
		return info, fmt.Errorf("nvme: transfer limit is smaller than one block")
	}
	info.MaxTransferBytes = uint32(limit)
	return info, nil
}

// namespaceID obtains the namespace selected by an open generic handle.
func namespaceID(f *os.File) (uint32, error) {
	for {
		id, err := unix.IoctlRetInt(int(f.Fd()), ioctlNamespaceID)
		runtime.KeepAlive(f)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("nvme: namespace ID: %w", err)
		}
		return uint32(id), nil
	}
}

// blockForNamespace finds a generic handle's sibling block device with this NSID.
func blockForNamespace(charSys string, id uint32) (string, error) {
	controller := filepath.Dir(charSys)
	entries, err := os.ReadDir(controller)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if _, err := os.Stat(filepath.Join("/sys/class/block", e.Name())); err != nil {
			continue
		}
		candidate := filepath.Join(controller, e.Name())
		nsid, err := readUint(filepath.Join(candidate, "nsid"))
		if err == nil && nsid == uint64(id) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("nvme: cannot resolve block interface for NSID %d", id)
}

const (
	ioctlNamespaceID  = 0x4e40     // NVME_IOCTL_ID, returns the target volume's NSID.
	ioctlAdminCommand = 0xc0484e41 // NVME_IOCTL_ADMIN_CMD, with a 72-byte payload.
)

// identifyPage reads the volume or controller Identify data, as fio does during
// startup. The ioctl returns zero on success or a positive NVMe status on failure.
func identifyPage(f *os.File, nsid, cns uint32) ([]byte, error) {
	data := make([]byte, 4096)
	var cmd [72]byte // struct nvme_passthru_cmd, including result at offset 68
	cmd[0] = 0x06
	binary.NativeEndian.PutUint32(cmd[4:], nsid)
	binary.NativeEndian.PutUint64(cmd[24:], uint64(uintptr(unsafe.Pointer(unsafe.SliceData(data)))))
	binary.NativeEndian.PutUint32(cmd[36:], 4096)
	binary.NativeEndian.PutUint32(cmd[40:], cns)
	binary.NativeEndian.PutUint32(cmd[64:], 30000)
	for {
		result, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), ioctlAdminCommand, uintptr(unsafe.Pointer(&cmd)))
		runtime.KeepAlive(data)
		runtime.KeepAlive(f)
		if errno == unix.EINTR {
			continue
		}
		if errno != 0 {
			return nil, fmt.Errorf("nvme: Identify CNS %d: %w", cns, errno)
		}
		if result != 0 {
			return nil, StatusError(result)
		}
		return data, nil
	}
}

func readUint(path string) (uint64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("nvme: parse %s: %w", path, err)
	}
	return value, nil
}

// Op builds a ringo operation targeting fd and retains Data and Metadata until
// final completion. Submit it to a Ring created WithUringCmd and check Status
// on its completion to handle both Linux errors and NVMe device status.
func (cmd Command) Op(fd ringo.FD, options ...ringo.OpOption) (ringo.Op, error) {
	return cmd.op(fd, UringCmdIO, nil, options...)
}

// FixedOp is Op for a command whose Data lies within registered, a buffer of
// the Ring's registered table. The driver maps that entry instead of pinning
// Data's pages for each command. Metadata must be empty.
func (cmd Command) FixedOp(fd ringo.FD, registered ringo.FixedBuffer, options ...ringo.OpOption) (ringo.Op, error) {
	if len(cmd.Metadata) != 0 {
		return nil, fmt.Errorf("nvme: a fixed-buffer command cannot carry metadata")
	}
	return cmd.op(fd, UringCmdIO, &registered, options...)
}

// AdminOp builds a controller command for a handle such as /dev/nvme1. For
// example, OpcodeAdminGetLogPage with CDW10 = ((4096/4-1)<<16)|0xd0 reads the
// 4096-byte AWS statistics page into Data. The log page defines the NSID and
// buffer format. Retain the controller handle through completion and check Status.
func (cmd Command) AdminOp(fd ringo.FD, options ...ringo.OpOption) (ringo.Op, error) {
	return cmd.op(fd, UringCmdAdmin, nil, options...)
}

func (cmd Command) op(fd ringo.FD, driverOp uint32, registered *ringo.FixedBuffer, options ...ringo.OpOption) (ringo.Op, error) {
	if uint64(len(cmd.Data)) > math.MaxUint32 || uint64(len(cmd.Metadata)) > math.MaxUint32 {
		return nil, fmt.Errorf("nvme: command buffer exceeds uint32 length")
	}
	p := cmd.payload()
	buffers := []ringo.CommandBuffer{{Offset: 16, Buffer: cmd.Metadata}, {Offset: 24, Buffer: cmd.Data}}
	if registered != nil {
		return ringo.UringCmdFixed(fd, driverOp, p[:], buffers, *registered, options...), nil
	}
	return ringo.UringCmd(fd, driverOp, p[:], buffers, options...), nil
}

// Status checks a command completion for Linux and NVMe errors. Successful
// Read, Write, and Flush commands return zero. Extra[0] holds the command-specific
// result for opcodes that define one.
func Status(c ringo.Completion) error {
	if c.Err != nil {
		return c.Err
	}
	if c.Result != 0 {
		return StatusError(c.Result)
	}
	return nil
}
