//go:build linux

package ringo

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"unsafe"
)

func (ring *Ring) register(
	opcode rawRegisterOpcode,
	argument unsafe.Pointer,
	count uint32,
) (uint, error) {
	if err := ring.ready(); err != nil {
		return 0, err
	}
	result, errno := ring.backend.register(opcode, argument, count)
	if errno != 0 {
		return 0, os.NewSyscallError("io_uring_register", errno)
	}
	return result, nil
}

// RegisterEventFD registers file for completion notifications and retains it
// until UnregisterEventFD or Ring.Close.
// liburing: io_uring_register_eventfd - https://man7.org/linux/man-pages/man3/io_uring_register_eventfd.3.html
func (ring *Ring) RegisterEventFD(file *os.File) error {
	return ring.registerEventFD(file, false)
}

// RegisterEventFDAsync is RegisterEventFD with
// IORING_REGISTER_EVENTFD_ASYNC semantics.
// liburing: io_uring_register_eventfd_async - https://man7.org/linux/man-pages/man3/io_uring_register_eventfd_async.3.html
func (ring *Ring) RegisterEventFDAsync(file *os.File) error {
	return ring.registerEventFD(file, true)
}

func (ring *Ring) registerEventFD(file *os.File, async bool) error {
	if err := ring.ready(); err != nil {
		return err
	}
	if ring.eventFD != nil {
		return errors.New("ringo: an eventfd is already registered")
	}
	if file == nil {
		return errors.New("ringo: eventfd file is nil")
	}
	fd := int(file.Fd())
	if !fitsInt32(fd) {
		return errors.New("ringo: eventfd does not fit the kernel ABI")
	}
	value := int32(fd)
	opcode := rawRegisterEventFD
	if async {
		opcode = rawRegisterEventFDAsync
	}
	if _, err := ring.register(opcode, unsafe.Pointer(&value), 1); err != nil {
		return err
	}
	ring.eventFD = file
	return nil
}

// UnregisterEventFD removes the registered completion notification eventfd.
// liburing: io_uring_unregister_eventfd - https://man7.org/linux/man-pages/man3/io_uring_unregister_eventfd.3.html
func (ring *Ring) UnregisterEventFD() error {
	if err := ring.ready(); err != nil {
		return err
	}
	if ring.eventFD == nil {
		return errors.New("ringo: no eventfd is registered")
	}
	if _, err := ring.register(rawUnregisterEventFD, nil, 0); err != nil {
		return err
	}
	ring.eventFD = nil
	return nil
}

// Probe is a value copy of the kernel's supported-opcode probe.
type Probe struct {
	supported [rawOpLast]bool
}

// rawProbe appends storage for every opcode after the generated flexible-array
// header from struct io_uring_probe.
type rawProbe struct {
	header rawProbeHeader
	ops    [rawOpLast]rawProbeOp
}

// Supports reports whether the kernel supports op's opcode.
func (probe *Probe) Supports(op Op) bool {
	if probe == nil || op == nil {
		return false
	}
	code := op.opcode()
	return code < rawOpLast && probe.supported[code]
}

// Probe returns the operation support advertised by this ring's kernel.
// liburing: io_uring_register_probe - https://man7.org/linux/man-pages/man3/io_uring_register_probe.3.html
func (ring *Ring) Probe() (*Probe, error) {
	var raw rawProbe
	if _, err := ring.register(
		rawRegisterProbe,
		unsafe.Pointer(&raw),
		uint32(len(raw.ops)),
	); err != nil {
		return nil, err
	}
	probe := new(Probe)
	count := int(raw.header.Ops_len)
	if count > len(raw.ops) {
		count = len(raw.ops)
	}
	for i := 0; i < count; i++ {
		entry := raw.ops[i]
		if rawOpcode(entry.Op) < rawOpLast &&
			entry.Flags&rawOpSupported != 0 {
			probe.supported[rawOpcode(entry.Op)] = true
		}
	}
	return probe, nil
}

// RegisterFiles installs a nonempty fixed-file table and retains each non-nil
// file until its slot is replaced, cleared, or the Ring is closed.
// Registration never changes RLIMIT_NOFILE; an insufficient process limit is
// returned as the kernel's registration error.
// liburing: io_uring_register_files - https://man7.org/linux/man-pages/man3/io_uring_register_files.3.html
func (ring *Ring) RegisterFiles(files ...*os.File) (*FixedFiles, error) {
	if err := ring.ready(); err != nil {
		return nil, err
	}
	if ring.files != nil {
		return nil, errors.New("ringo: fixed files are already registered")
	}
	if len(files) == 0 || uint64(len(files)) > math.MaxUint32 {
		return nil, errors.New("ringo: invalid fixed-file table size")
	}
	descriptors := make([]int32, len(files))
	for i, file := range files {
		descriptor, err := fixedFileDescriptor(file)
		if err != nil {
			return nil, fmt.Errorf("ringo: fixed file %d: %w", i, err)
		}
		descriptors[i] = descriptor
	}

	_, errno := ring.backend.register(
		rawRegisterFiles,
		unsafe.Pointer(unsafe.SliceData(descriptors)),
		uint32(len(descriptors)),
	)
	// descriptors contain fd numbers, not references to their owners.
	runtime.KeepAlive(files)
	if errno != 0 {
		return nil, os.NewSyscallError("io_uring_register", errno)
	}
	ring.files = &FixedFiles{
		ring:   ring,
		count:  uint32(len(files)),
		owners: append([]*os.File(nil), files...),
	}
	return ring.files, nil
}

// RegisterSparseFiles installs an empty fixed-file table after ring creation.
// It does not change RLIMIT_NOFILE.
// liburing: io_uring_register_files_sparse - https://man7.org/linux/man-pages/man3/io_uring_register_files_sparse.3.html
func (ring *Ring) RegisterSparseFiles(count uint32) (*FixedFiles, error) {
	if err := ring.ready(); err != nil {
		return nil, err
	}
	if ring.files != nil {
		return nil, errors.New("ringo: fixed files are already registered")
	}
	if count == 0 {
		return nil, errors.New("ringo: fixed-file table size must be nonzero")
	}
	if _, err := ring.backend.registerFilesSparse(count); err != nil {
		return nil, err
	}
	ring.files = &FixedFiles{
		ring:   ring,
		count:  count,
		owners: make([]*os.File, count),
	}
	return ring.files, nil
}
