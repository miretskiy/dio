//go:build linux

// Package nvme opens NVMe storage volumes, called namespaces in the NVMe
// protocol. Each volume has an identifier, a block format, and a capacity.
// For example, controller /dev/nvme0 can expose namespace ID 1 through both
// /dev/nvme0n1 (block interface) and /dev/ng0n1 (character interface). Controller
// number 0 and namespace ID 1 identify different things; namespace ID 0 is
// reserved. Open returns a Device containing the character-device handle and
// an Info snapshot of the volume's geometry and I/O limits.
//
// WithRange bounds a Device to a byte range on that volume. iosched accepts
// Devices in NVMeReadOp and NVMeWriteOp and splits byte requests into commands.
// Command.Op supplies direct ringo submission for low-level callers.
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

// Info is a snapshot of an NVMe volume's identity, block format, and I/O limits.
// Device.Info returns this metadata by value; Device owns the open file handle.
type Info struct {
	NSID             uint32 // Namespace ID placed in each command submitted to this device.
	BlockSize        uint32 // Bytes per logical block in the active LBA format.
	Blocks           uint64 // Addressable logical blocks, from Identify Namespace's NSZE.
	SizeBytes        uint64 // Namespace capacity: Blocks multiplied by BlockSize.
	MaxTransferBytes uint32 // Conservative single-command limit for any buffer alignment.
	MetadataSize     uint16 // Metadata bytes per logical block in the active format.
	ProtectionType   uint8  // Active Protection Information type; zero means disabled.
	Model            string // Controller model from Identify Controller.
	Serial           string // Controller serial from Identify Controller.
	BlockDevice      string // Namespace block interface, for example /dev/nvme1n1.
	CharDevice       string // Namespace character interface, for example /dev/ng1n1.
	// MDTS is the Maximum Data Transfer Size exponent from Identify Controller.
	// A nonzero value limits transfers to 2^MDTS controller-minimum memory pages.
	// Zero leaves transfer sizing to the queue and command bounds.
	// MaxTransferBytes incorporates the kernel's normalized limit and queue limits.
	MDTS uint8
}

// Device is an open handle to one NVMe namespace, such as /dev/ng1n1.
// Info().NSID identifies that namespace. WithRange bounds its read/write
// commands to a byte range. Close it after all submitted commands complete.
type Device struct {
	file   *os.File // O_RDWR namespace character-device handle.
	info   Info     // Discovered namespace geometry and limits.
	offset uint64   // Absolute byte offset of the accessible range.
	length uint64   // Length of the accessible range in bytes.
}

// OpenOption configures the byte range exposed by an open Device.
type OpenOption func(*openConfig)

type openConfig struct {
	offset uint64 // Absolute namespace byte offset.
	length uint64 // Accessible bytes from offset.
}

// WithRange exposes length bytes starting at an absolute namespace byte offset.
// Both values must be block aligned and length must be positive. Read/write
// offsets are relative to this range. The caller reserves it against other users.
func WithRange(offset, length uint64) OpenOption {
	return func(c *openConfig) { c.offset, c.length = offset, length }
}

// Open opens and identifies an NVMe namespace character device or a symlink
// to one. It supports the ordinary NVM command set with a data-only LBA format.
// The caller supplies exclusive ownership of every range it uses for I/O.
// DiscoverTopology traces a filesystem or block device to its backing namespace.
func Open(path string, options ...OpenOption) (*Device, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	info, err := identify(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	cfg := openConfig{length: info.SizeBytes}
	for _, option := range options {
		option(&cfg)
	}
	d := &Device{file: f, info: info, offset: cfg.offset, length: cfg.length}
	if err := d.validateRange(); err != nil {
		f.Close()
		return nil, err
	}
	return d, nil
}

// Info returns the discovered namespace geometry and controller limits.
func (d *Device) Info() Info { return d.info }

// File returns the namespace handle used by commands. Keep it open through the
// completion of all submitted operations; Device.Close releases it.
func (d *Device) File() *os.File { return d.file }

// Close releases the namespace handle after its outstanding commands complete.
func (d *Device) Close() error { return d.file.Close() }

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

func parseIdentify(info Info, ns, ctrl []byte) (Info, error) {
	if len(ns) != 4096 || len(ctrl) != 4096 {
		return info, fmt.Errorf("nvme: Identify page must be 4096 bytes")
	}
	format := int(ns[26] & 15)
	if ns[25] >= 16 {
		format |= int((ns[26]>>5)&3) << 4
	}
	if format > int(ns[25]) || format >= 64 {
		return info, fmt.Errorf("nvme: invalid active LBA format %d", format)
	}
	lbaf := ns[128+format*4:]
	shift := lbaf[2]
	if shift < 9 || shift > 31 {
		return info, fmt.Errorf("nvme: unsupported LBA size exponent %d", shift)
	}
	info.BlockSize = uint32(1) << shift
	info.Blocks = binary.LittleEndian.Uint64(ns[:8])
	if info.Blocks == 0 || info.Blocks > math.MaxUint64/uint64(info.BlockSize) {
		return info, fmt.Errorf("nvme: invalid namespace capacity")
	}
	info.SizeBytes = info.Blocks * uint64(info.BlockSize)
	info.MetadataSize = binary.LittleEndian.Uint16(lbaf[:2])
	info.ProtectionType = ns[29] & 7
	if info.MetadataSize != 0 || info.ProtectionType != 0 {
		return info, fmt.Errorf("nvme: metadata/PI format unsupported (metadata=%d, protection=%d)", info.MetadataSize, info.ProtectionType)
	}
	info.Model = strings.TrimSpace(string(ctrl[24:64]))
	info.Serial = strings.TrimSpace(string(ctrl[4:24]))
	info.MDTS = ctrl[77]
	return info, nil
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

// Offset returns the handle's absolute byte offset in the namespace.
func (d *Device) Offset() uint64 { return d.offset }

// Size returns the accessible range's length in bytes. Info.SizeBytes reports
// the capacity of the entire namespace.
func (d *Device) Size() uint64 { return d.length }

func (d *Device) validateRange() error {
	block := uint64(d.info.BlockSize)
	if block == 0 || d.length == 0 || d.offset%block != 0 || d.length%block != 0 ||
		d.offset > d.info.SizeBytes || d.length > d.info.SizeBytes-d.offset {
		return fmt.Errorf("nvme: unaligned or out-of-bounds device range")
	}
	return nil
}

// ValidateIO checks a device-relative byte range for block alignment and bounds.
// It accepts zero length at an aligned offset through Size(). iosched validates
// the complete request with this method before splitting it into commands.
func (d *Device) ValidateIO(offset, length uint64) error {
	if d == nil || d.info.BlockSize == 0 {
		return fmt.Errorf("nvme: invalid device")
	}
	block := uint64(d.info.BlockSize)
	if length%block != 0 || offset%block != 0 || offset > d.length || length > d.length-offset {
		return fmt.Errorf("nvme: unaligned or out-of-bounds request")
	}
	return nil
}

// ReadCommand encodes one read at a device-relative byte offset. It validates
// alignment, the Device's range, and Info.MaxTransferBytes. For range base b,
// relative offset o, and byte length n, starting LBA = (b+o)/BlockSize and
// block count = n/BlockSize. iosched handles splitting larger byte requests.
func (d *Device) ReadCommand(buffer []byte, offset uint64) (Command, error) {
	return d.command(false, buffer, offset, false)
}

// WriteCommand encodes one write with the same bounds as ReadCommand. fua
// requests Force Unit Access: the write reaches stable storage before completion.
func (d *Device) WriteCommand(buffer []byte, offset uint64, fua bool) (Command, error) {
	return d.command(true, buffer, offset, fua)
}

func (d *Device) command(write bool, buffer []byte, offset uint64, fua bool) (Command, error) {
	length := uint64(len(buffer))
	if err := d.ValidateIO(offset, length); err != nil {
		return Command{}, err
	}
	info := d.info
	block := uint64(info.BlockSize)
	if length == 0 || length > uint64(info.MaxTransferBytes) || length/block > 65536 {
		return Command{}, fmt.Errorf("nvme: command requires 1..%d bytes within 65536 blocks", info.MaxTransferBytes)
	}
	slba := (d.offset + offset) / block
	cmd := Command{Opcode: OpcodeRead, NSID: info.NSID, Data: buffer, CDW10: uint32(slba), CDW11: uint32(slba >> 32), CDW12: uint32(length/block - 1)}
	if write {
		cmd.Opcode = OpcodeWrite
		if fua {
			cmd.CDW12 |= ForceUnitAccess
		}
	}
	return cmd, nil
}

// FlushCommand encodes a flush of the entire namespace, including writes outside
// this Device's range. NVMe flush scope is determined by the namespace ID.
func (d *Device) FlushCommand() Command {
	return Command{Opcode: OpcodeFlush, NSID: d.info.NSID}
}

// Opcode selects an operation within the I/O or Admin command set.
// Command.Op selects I/O; Command.AdminOp selects Admin.
type Opcode uint8

const (
	OpcodeFlush             Opcode = 0x00 // Make the namespace's completed writes durable.
	OpcodeWrite             Opcode = 0x01 // Transfer Data from memory to namespace blocks.
	OpcodeRead              Opcode = 0x02 // Transfer namespace blocks into Data.
	OpcodeCompare           Opcode = 0x05 // Compare Data with namespace blocks, when supported.
	OpcodeWriteZeroes       Opcode = 0x08 // Zero a block range, when supported.
	OpcodeDatasetManagement Opcode = 0x09 // Apply hints or deallocate ranges, when supported.
	OpcodeVerify            Opcode = 0x0c // Verify a block range, when supported.
	OpcodeAdminGetLogPage   Opcode = 0x02 // Read a standard or vendor-defined log page.
	OpcodeAdminIdentify     Opcode = 0x06 // Read controller or namespace information.
	// UringCmdIO is NVME_URING_CMD_IO from linux/nvme_ioctl.h (72-byte payload).
	UringCmdIO = 0xc0484e80
	// UringCmdAdmin selects NVME_URING_CMD_ADMIN on a controller character device.
	UringCmdAdmin = 0xc0484e82
	// ForceUnitAccess requires this write to reach nonvolatile storage.
	ForceUnitAccess uint32 = 1 << 30
)

// Command describes one NVMe command, encoded as Linux's nvme_uring_cmd.
// For example, with 512-byte blocks, this reads 4096 bytes starting at LBA 128:
//
//	cmd := nvme.Command{
//		Opcode: nvme.OpcodeRead,
//		NSID: device.Info().NSID,
//		CDW10: 128,
//		CDW12: 7, // Eight blocks, encoded minus one.
//		Data: make([]byte, 4096),
//	}
//
// CDW means command dword, a 32-bit field numbered in the NVMe command layout.
// Dwords 4-5 carry metadata addresses and 6-9 carry data pointers; Op supplies
// those through Data and Metadata and the Linux driver maps their memory.
// The remaining dwords below carry opcode-specific parameters. Device.ReadCommand
// and Device.WriteCommand fill the read/write parameters and validate their bounds.
//
// The Linux payload uses native byte order. The command's data and metadata
// formats determine their own byte order. Callers select supported opcodes,
// transfer limits, metadata formats, privileges, and exclusively owned ranges.
type Command struct {
	Opcode    Opcode // Opcode in the I/O or Admin command set selected by the builder.
	NSID      uint32 // Namespace selected by the command; controller-scoped commands use zero.
	CDW2      uint32 // Opcode-specific dword 2; ordinary reads/writes leave it zero.
	CDW3      uint32 // Opcode-specific dword 3; ordinary reads/writes leave it zero.
	CDW10     uint32 // Read/write: low 32 bits of the starting logical block address.
	CDW11     uint32 // Read/write: high 32 bits of the starting logical block address.
	CDW12     uint32 // Read/write: zero-based block count in bits 15:0, control in 31:16.
	CDW13     uint32 // Read/write: dataset-management hints.
	CDW14     uint32 // Read/write: expected initial protection reference tag.
	CDW15     uint32 // Read/write: protection application tag and mask.
	TimeoutMS uint32 // Command timeout in milliseconds; zero uses the driver default.
	Data      []byte // Transfer buffer; Op derives its address and byte length.
	Metadata  []byte // Per-block metadata buffer, according to the namespace format.
}

func (cmd Command) payload() [72]byte {
	var p [72]byte
	p[0] = byte(cmd.Opcode)
	binary.NativeEndian.PutUint32(p[4:], cmd.NSID)
	binary.NativeEndian.PutUint32(p[8:], cmd.CDW2)
	binary.NativeEndian.PutUint32(p[12:], cmd.CDW3)
	binary.NativeEndian.PutUint32(p[32:], uint32(len(cmd.Metadata)))
	binary.NativeEndian.PutUint32(p[36:], uint32(len(cmd.Data)))
	binary.NativeEndian.PutUint32(p[40:], cmd.CDW10)
	binary.NativeEndian.PutUint32(p[44:], cmd.CDW11)
	binary.NativeEndian.PutUint32(p[48:], cmd.CDW12)
	binary.NativeEndian.PutUint32(p[52:], cmd.CDW13)
	binary.NativeEndian.PutUint32(p[56:], cmd.CDW14)
	binary.NativeEndian.PutUint32(p[60:], cmd.CDW15)
	binary.NativeEndian.PutUint32(p[64:], cmd.TimeoutMS)
	return p
}

// Op builds a ringo operation targeting fd and retains Data and Metadata until
// final completion. Submit it to a Ring created WithUringCmd and check Status
// on its completion to handle both Linux errors and NVMe device status.
func (cmd Command) Op(fd ringo.FD, options ...ringo.OpOption) (ringo.Op, error) {
	return cmd.op(fd, UringCmdIO, options...)
}

// AdminOp builds a controller command for a handle such as /dev/nvme1. For
// example, OpcodeAdminGetLogPage with CDW10 = ((4096/4-1)<<16)|0xd0 reads the
// 4096-byte AWS statistics page into Data. The log page defines the NSID and
// buffer format. Retain the controller handle through completion and check Status.
func (cmd Command) AdminOp(fd ringo.FD, options ...ringo.OpOption) (ringo.Op, error) {
	return cmd.op(fd, UringCmdAdmin, options...)
}

func (cmd Command) op(fd ringo.FD, driverOp uint32, options ...ringo.OpOption) (ringo.Op, error) {
	if uint64(len(cmd.Data)) > math.MaxUint32 || uint64(len(cmd.Metadata)) > math.MaxUint32 {
		return nil, fmt.Errorf("nvme: command buffer exceeds uint32 length")
	}
	p := cmd.payload()
	return ringo.UringCmd(fd, driverOp, p[:], []ringo.CommandBuffer{
		{Offset: 16, Buffer: cmd.Metadata}, {Offset: 24, Buffer: cmd.Data},
	}, options...), nil
}

// StatusError carries an NVMe completion status, including its status code type
// and status code. Linux reports device errors as positive completion results.
type StatusError uint32

func (s StatusError) Error() string {
	return fmt.Sprintf("nvme: command status %#x (type %d, code %#x)", uint32(s), (s>>8)&7, s&255)
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
