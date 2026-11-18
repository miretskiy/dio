//go:build linux

package ringo

import (
	"cmp"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"sort"
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
// Update issues a registration syscall on the Ring and rewrites the table's
// retained files, so unlike the table's lookups it must be serialized with Ring
// calls exactly as though it were one.
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

// Linux's fixed-buffer registration ABI limits each entry to 1 GiB. The
// registration path splits caller regions at this constant; the ABI supplies
// no query for a larger per-entry limit.
const (
	maxRegisteredBufferBytes = 1 << 30
	maxRegisteredBuffers     = 1 << 14 // Supported kernel baseline's registration-table limit.
	// MaxRegisteredMemory is the total memory one Ring can register: 32 GiB.
	MaxRegisteredMemory = 32 << 30
)

// bufferRegion locates one caller region's consecutive registration entries.
type bufferRegion struct {
	base  uintptr
	size  uintptr
	first uint32 // Index of the region's first entry.
}

// FixedBuffers is one immutable fixed-buffer table owned by a Ring. It stays
// registered with the kernel until Ring.Close and retains its backing buffers
// for as long as the table itself is reachable. Because it is never mutated,
// its lookups are safe to call from any goroutine.
type FixedBuffers struct {
	ring    *Ring
	buffers [][]byte
	regions []bufferRegion
}

// Len returns the number of kernel registration entries in the table.
func (set *FixedBuffers) Len() int {
	if set == nil || set.ring == nil {
		return 0
	}
	return len(set.buffers)
}

// Buffer returns the entire kernel registration entry at index.
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

var errNotRegistered = errors.New("ringo: buffer is not inside one fixed-buffer entry")

// Bind returns a typed fixed buffer for data, which must be a nonempty
// subslice of exactly one registration entry. The lookup is a binary search of
// the regions by address, then arithmetic within the region.
func (set *FixedBuffers) Bind(data []byte) (FixedBuffer, error) {
	if set == nil || set.ring == nil {
		return FixedBuffer{}, errors.New("ringo: invalid fixed-buffer table")
	}
	start := slicePtr(data)
	// The last region starting at or before data is the only one that can hold it.
	i := sort.Search(len(set.regions), func(i int) bool { return set.regions[i].base > start }) - 1
	if i < 0 || len(data) == 0 {
		return FixedBuffer{}, errNotRegistered
	}
	region := set.regions[i]
	offset := start - region.base
	if offset >= region.size {
		return FixedBuffer{}, errNotRegistered
	}
	index := region.first + uint32(offset/maxRegisteredBufferBytes)
	within := offset % maxRegisteredBufferBytes
	if uintptr(len(data)) > uintptr(len(set.buffers[index]))-within {
		return FixedBuffer{}, errNotRegistered
	}
	return FixedBuffer{set: set, index: uint16(index), data: data}, nil
}

// FixedBuffer is a typed slice of a ring-owned registration entry.
type FixedBuffer struct {
	set   *FixedBuffers
	index uint16
	data  []byte
}

// Len reports the bytes selected by Buffer or Bind.
func (buffer FixedBuffer) Len() int { return len(buffer.data) }

// EntryRemaining reports the bytes from the start of the selected range to
// the end of its registration entry.
func (buffer FixedBuffer) EntryRemaining() int {
	if buffer.set == nil || int(buffer.index) >= len(buffer.set.buffers) {
		return 0
	}
	entry := buffer.set.buffers[buffer.index]
	return len(entry) - int(slicePtr(buffer.data)-slicePtr(entry))
}

// containsIovec reports whether one vector of a fixed vectored operation falls
// inside this registered buffer. It reads the iovec rather than a parallel
// [][]byte because the iovec is what the operation retains and what the kernel
// will actually dereference.
func (buffer FixedBuffer) containsIovec(iovec syscall.Iovec) bool {
	if buffer.set == nil || int(buffer.index) >= len(buffer.set.buffers) {
		return false
	}
	registered := buffer.set.buffers[buffer.index]
	start := uintptr(unsafe.Pointer(iovec.Base))
	end := start + uintptr(iovec.Len)
	base := uintptr(unsafe.Pointer(unsafe.SliceData(registered)))
	limit := base + uintptr(len(registered))
	return end >= start && limit >= base && start >= base && end <= limit
}

// contains reports whether data lies inside this registered buffer's entry.
func (buffer FixedBuffer) contains(data []byte) bool {
	var iovec syscall.Iovec
	iovec.Base = unsafe.SliceData(data)
	iovec.SetLen(len(data))
	return buffer.containsIovec(iovec)
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
// one table, and the table remains registered until Ring.Close. Each caller
// region is split into entries of at most 1 GiB, so a 16 GiB region uses
// sixteen entries. A bound range must lie within one entry. Regions total at
// most MaxRegisteredMemory (32 GiB).
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
	entries := 0
	var total uint64
	for i, buffer := range buffers {
		if len(buffer) == 0 {
			return nil, fmt.Errorf("ringo: fixed buffer %d is empty", i)
		}
		if uint64(len(buffer)) > MaxRegisteredMemory-total {
			return nil, errors.New("ringo: registered memory exceeds 32 GiB per ring")
		}
		total += uint64(len(buffer))
		entries += 1 + (len(buffer)-1)/maxRegisteredBufferBytes
		if entries > maxRegisteredBuffers {
			return nil, errors.New("ringo: regions require more than 16384 fixed-buffer entries")
		}
	}
	set := &FixedBuffers{ring: ring, buffers: make([][]byte, 0, entries), regions: make([]bufferRegion, 0, len(buffers))}
	iovecs := make([]syscall.Iovec, 0, entries)
	for _, buffer := range buffers {
		set.regions = append(set.regions, bufferRegion{base: slicePtr(buffer), size: uintptr(len(buffer)), first: uint32(len(set.buffers))})
		for len(buffer) != 0 {
			n := min(len(buffer), maxRegisteredBufferBytes)
			entry := buffer[:n:n]
			set.buffers = append(set.buffers, entry)
			iovec := syscall.Iovec{Base: unsafe.SliceData(entry)}
			iovec.SetLen(len(entry))
			iovecs = append(iovecs, iovec)
			buffer = buffer[len(entry):]
		}
	}
	if _, err := ring.backend.registerBuffers(iovecs); err != nil {
		return nil, err
	}
	slices.SortFunc(set.regions, func(a, b bufferRegion) int { return cmp.Compare(a.base, b.base) })
	ring.buffers = set
	return set, nil
}
