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
	"strings"
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
	// QueueDepth is the block layer's request limit per hardware queue
	// (queue/nr_requests). A command issued while its CPU's queue is full
	// cannot get a request without blocking, so io_uring hands it to a worker.
	QueueDepth uint32
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

// StatusError carries an NVMe completion status, including its status code type
// and status code. Linux reports device errors as positive completion results.
type StatusError uint32

func (s StatusError) Error() string {
	return fmt.Sprintf("nvme: command status %#x (type %d, code %#x)", uint32(s), (s>>8)&7, s&255)
}
