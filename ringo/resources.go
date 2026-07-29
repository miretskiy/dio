//go:build linux

package ringo

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

// FixedFiles is a ring-lifetime registered-file table.
//
// A FixedFile names a table slot, not a particular file. Direct open and close
// operations may change the file occupying a sparse slot. The caller must not
// reuse a slot until the operation that previously used or closed it has
// completed.
type FixedFiles struct {
	ring   *Ring
	count  uint32
	owners []*os.File
}

func fixedFileDescriptor(file *os.File) (int32, error) {
	if file == nil {
		return -1, nil
	}
	fd := int(file.Fd())
	if fd < 0 {
		return 0, errors.New("file is closed")
	}
	if !fitsInt32(fd) {
		return 0, fmt.Errorf("descriptor %d does not fit the kernel ABI", fd)
	}
	return int32(fd), nil
}

// Len returns the number of slots in the fixed-file table.
func (files *FixedFiles) Len() int {
	if files == nil || files.ring == nil {
		return 0
	}
	return int(files.count)
}

// File returns the typed registered-file slot at index.
func (files *FixedFiles) File(index uint32) (FixedFile, error) {
	if files == nil || files.ring == nil {
		return FixedFile{}, errors.New("ringo: no registered-file table")
	}
	if index >= files.count {
		return FixedFile{}, fmt.Errorf(
			"ringo: registered-file index %d is outside table size %d",
			index, files.count,
		)
	}
	return FixedFile{table: files, index: index}, nil
}

// Update replaces consecutive fixed-file slots beginning at offset. A nil
// replacement clears its slot. The returned count is the number of slots the
// kernel changed.
//
// Update is synchronous, but it is not an ordering barrier for queued or
// in-flight I/O. Linux keeps an old registered file alive while requests that
// already use it complete; callers remain responsible for deciding when a
// slot may change occupants. FixedFile values continue to identify the slot,
// not the file generation.
//
// liburing: io_uring_register_files_update - https://man7.org/linux/man-pages/man3/io_uring_register_files_update.3.html
func (files *FixedFiles) Update(
	offset uint32,
	replacements ...*os.File,
) (int, error) {
	if files == nil || files.ring == nil {
		return 0, errors.New("ringo: invalid fixed-file table")
	}
	if len(replacements) == 0 {
		return 0, errors.New("ringo: fixed-file update is empty")
	}
	if uint64(offset)+uint64(len(replacements)) > uint64(files.count) {
		return 0, errors.New("ringo: fixed-file update is out of bounds")
	}

	descriptors := make([]int32, len(replacements))
	for index, file := range replacements {
		descriptor, err := fixedFileDescriptor(file)
		if err != nil {
			return 0, fmt.Errorf(
				"ringo: fixed-file replacement %d: %w",
				index,
				err,
			)
		}
		descriptors[index] = descriptor
	}
	update := rawFilesUpdate{
		Offset: offset,
		Fds:    uint64(uintptr(unsafe.Pointer(unsafe.SliceData(descriptors)))),
	}
	result, err := files.ring.register(
		rawRegisterFilesUpdate,
		unsafe.Pointer(&update),
		uint32(len(descriptors)),
	)
	// update.Fds is an integer-encoded pointer, and descriptors contain fd
	// numbers rather than references to their owners.
	runtime.KeepAlive(descriptors)
	runtime.KeepAlive(replacements)
	if err != nil {
		return 0, err
	}
	if result > uint(len(replacements)) {
		return 0, fmt.Errorf(
			"ringo: kernel reported %d fixed-file updates for %d replacements",
			result,
			len(replacements),
		)
	}

	updated := int(result)
	copy(files.owners[int(offset):], replacements[:updated])
	if updated != len(replacements) {
		return updated, fmt.Errorf(
			"ringo: kernel updated %d of %d fixed-file slots",
			updated,
			len(replacements),
		)
	}
	return updated, nil
}

// FixedFile is a comparable, ring-scoped registered-file slot.
//
// It remains a valid reference to the slot until Ring.Close. If the caller
// closes and reopens that slot, every retained FixedFile value refers to the
// new occupant.
type FixedFile struct {
	table *FixedFiles
	index uint32
}

func (file FixedFile) validate(ring *Ring) error {
	if file.table == nil || file.table.ring == nil {
		return errors.New("ringo: invalid registered-file slot")
	}
	if file.table.ring != ring {
		return ErrWrongRing
	}
	if file.index >= file.table.count {
		return errors.New("ringo: invalid registered-file index")
	}
	return nil
}

// FixedBuffers is one immutable fixed-buffer table owned by a Ring. Its
// backing buffers remain retained until Ring.Close.
type FixedBuffers struct {
	ring    *Ring
	buffers [][]byte
}

// Len returns the number of buffers in the table.
func (set *FixedBuffers) Len() int {
	if set == nil || set.ring == nil {
		return 0
	}
	return len(set.buffers)
}

// Buffer returns the entire fixed buffer at index.
func (set *FixedBuffers) Buffer(index uint32) (FixedBuffer, error) {
	if set == nil || set.ring == nil {
		return FixedBuffer{}, errors.New("ringo: invalid fixed-buffer table")
	}
	if uint64(index) >= uint64(len(set.buffers)) {
		return FixedBuffer{}, fmt.Errorf(
			"ringo: fixed-buffer index %d is outside table size %d",
			index, len(set.buffers),
		)
	}
	return FixedBuffer{
		set:   set,
		index: uint16(index),
		data:  set.buffers[index],
	}, nil
}

// Bind returns a typed fixed buffer for data, which must be a nonempty
// subslice of exactly one registered buffer.
func (set *FixedBuffers) Bind(data []byte) (FixedBuffer, error) {
	if set == nil || set.ring == nil {
		return FixedBuffer{}, errors.New("ringo: invalid fixed-buffer table")
	}
	if len(data) == 0 {
		return FixedBuffer{}, errors.New("ringo: cannot bind an empty fixed buffer")
	}
	start := uintptr(unsafe.Pointer(unsafe.SliceData(data)))
	end := start + uintptr(len(data))
	if end < start {
		return FixedBuffer{}, errors.New("ringo: buffer address overflow")
	}
	for index, buffer := range set.buffers {
		base := uintptr(unsafe.Pointer(unsafe.SliceData(buffer)))
		limit := base + uintptr(len(buffer))
		if limit < base {
			return FixedBuffer{}, errors.New("ringo: fixed buffer address overflow")
		}
		if start >= base && end <= limit {
			return FixedBuffer{
				set:   set,
				index: uint16(index),
				data:  data,
			}, nil
		}
	}
	return FixedBuffer{}, errors.New("ringo: buffer is not inside this fixed-buffer table")
}

// FixedBuffer is a typed slice of a ring-owned fixed buffer.
type FixedBuffer struct {
	set   *FixedBuffers
	index uint16
	data  []byte
}

func (buffer FixedBuffer) contains(data []byte) bool {
	if buffer.set == nil || int(buffer.index) >= len(buffer.set.buffers) ||
		len(data) == 0 {
		return false
	}
	registered := buffer.set.buffers[buffer.index]
	start := uintptr(unsafe.Pointer(unsafe.SliceData(data)))
	end := start + uintptr(len(data))
	base := uintptr(unsafe.Pointer(unsafe.SliceData(registered)))
	limit := base + uintptr(len(registered))
	return end >= start && limit >= base && start >= base && end <= limit
}

// Slice returns a subrange of buffer.
func (buffer FixedBuffer) Slice(offset, length int) (FixedBuffer, error) {
	if offset < 0 || length < 0 || offset > len(buffer.data) || length > len(buffer.data)-offset {
		return FixedBuffer{}, errors.New("ringo: fixed-buffer slice is out of bounds")
	}
	buffer.data = buffer.data[offset : offset+length]
	return buffer, nil
}

func (buffer FixedBuffer) validate(ring *Ring) error {
	if buffer.set == nil || buffer.set.ring == nil {
		return errors.New("ringo: invalid fixed buffer")
	}
	if buffer.set.ring != ring {
		return ErrWrongRing
	}
	if int(buffer.index) >= len(buffer.set.buffers) {
		return errors.New("ringo: invalid fixed-buffer index")
	}
	return nil
}

// RegisterBuffers registers one immutable fixed-buffer table. A Ring supports
// one table, and the table remains registered until Ring.Close.
// liburing: io_uring_register_buffers - https://man7.org/linux/man-pages/man3/io_uring_register_buffers.3.html
func (ring *Ring) RegisterBuffers(buffers ...[]byte) (*FixedBuffers, error) {
	if err := ring.ready(); err != nil {
		return nil, err
	}
	if ring.buffers != nil {
		return nil, errors.New("ringo: buffers are already registered")
	}
	if len(buffers) == 0 {
		return nil, errors.New("ringo: cannot register an empty buffer set")
	}
	if len(buffers) > math.MaxUint16+1 {
		return nil, errors.New("ringo: too many fixed buffers")
	}

	retained := append([][]byte(nil), buffers...)
	iovecs := make([]syscall.Iovec, len(retained))
	for i, buffer := range retained {
		if len(buffer) == 0 {
			return nil, fmt.Errorf("ringo: fixed buffer %d is empty", i)
		}
		if uint64(len(buffer)) > 1<<30 {
			return nil, fmt.Errorf("ringo: fixed buffer %d exceeds 1 GiB", i)
		}
		iovecs[i].Base = unsafe.SliceData(buffer)
		iovecs[i].SetLen(len(buffer))
	}
	set := &FixedBuffers{ring: ring, buffers: retained}
	if _, err := ring.backend.registerBuffers(iovecs); err != nil {
		return nil, err
	}
	ring.buffers = set
	return set, nil
}
