//go:build linux

package ringo

import (
	"errors"
	"math"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

var readOpPool sync.Pool

type readOp struct {
	opBase
	fd         FD
	buffer     []byte
	offset     int64
	registered FixedBuffer
	fixed      bool
}

func newReadOp(
	fd FD,
	buffer []byte,
	offset int64,
	registered FixedBuffer,
	fixed bool,
) *readOp {
	op, _ := readOpPool.Get().(*readOp)
	if op == nil {
		op = new(readOp)
	}
	*op = readOp{
		fd: fd, buffer: buffer, offset: offset,
		registered: registered, fixed: fixed,
	}
	if uint64(len(buffer)) > math.MaxUint32 {
		op.fail(errors.New("read buffer is too large for an SQE"))
	}
	return op
}

func (op *readOp) opcode() rawOpcode {
	if op.fixed {
		return rawOpReadFixed
	}
	return rawOpRead
}

func (op *readOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	if err := op.fd.validate(ring); err != nil {
		return err
	}
	if op.fixed {
		return op.registered.validate(ring)
	}
	return nil
}

func (op *readOp) prepare(sqe *rawSQE) {
	fd, flags := op.fd.sqe()
	*sqe = rawSQE{
		Opcode: uint8(op.opcode()),
		Flags:  uint8(flags),
		Fd:     fd,
		Off:    uint64(op.offset),
		Addr:   uint64(slicePtr(op.buffer)),
		Len:    uint32(len(op.buffer)),
	}
	if op.fixed {
		sqe.Buf_index = op.registered.index
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

func (op *readOp) release() {
	// Keep stale aliases consumed while this object is sitting in the pool.
	*op = readOp{opBase: opBase{consumed: true}}
	readOpPool.Put(op)
}

var writeOpPool sync.Pool

type writeOp struct {
	opBase
	fd         FD
	buffer     []byte
	offset     int64
	registered FixedBuffer
	fixed      bool
}

func newWriteOp(
	fd FD,
	buffer []byte,
	offset int64,
	registered FixedBuffer,
	fixed bool,
) *writeOp {
	op, _ := writeOpPool.Get().(*writeOp)
	if op == nil {
		op = new(writeOp)
	}
	*op = writeOp{
		fd: fd, buffer: buffer, offset: offset,
		registered: registered, fixed: fixed,
	}
	if uint64(len(buffer)) > math.MaxUint32 {
		op.fail(errors.New("write buffer is too large for an SQE"))
	}
	return op
}

func (op *writeOp) opcode() rawOpcode {
	if op.fixed {
		return rawOpWriteFixed
	}
	return rawOpWrite
}

func (op *writeOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	if err := op.fd.validate(ring); err != nil {
		return err
	}
	if op.fixed {
		return op.registered.validate(ring)
	}
	return nil
}

func (op *writeOp) prepare(sqe *rawSQE) {
	fd, flags := op.fd.sqe()
	*sqe = rawSQE{
		Opcode: uint8(op.opcode()),
		Flags:  uint8(flags),
		Fd:     fd,
		Off:    uint64(op.offset),
		Addr:   uint64(slicePtr(op.buffer)),
		Len:    uint32(len(op.buffer)),
	}
	if op.fixed {
		sqe.Buf_index = op.registered.index
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

func (op *writeOp) release() {
	// Keep stale aliases consumed while this object is sitting in the pool.
	*op = writeOp{opBase: opBase{consumed: true}}
	writeOpPool.Put(op)
}

type readvOp struct {
	opBase
	fd            FD
	buffers       [][]byte
	iovecs        []syscall.Iovec
	inlineBuffers [inlineIovecCount][]byte
	inlineIovecs  [inlineIovecCount]syscall.Iovec
	offset        int64
	flags         uint32
	registered    FixedBuffer
	fixed         bool
}

func newReadvOp(
	fd FD,
	buffers [][]byte,
	offset int64,
	flags int,
	registered FixedBuffer,
	fixed bool,
) *readvOp {
	op := &readvOp{
		fd: fd, offset: offset, flags: uint32(flags),
		registered: registered, fixed: fixed,
	}
	if flags < 0 || uint64(flags) > math.MaxUint32 {
		op.fail(errors.New("readv flags do not fit the kernel ABI"))
	}
	var err error
	op.buffers, op.iovecs, err = makeIovecs(
		buffers,
		&op.inlineBuffers,
		&op.inlineIovecs,
	)
	op.fail(err)
	return op
}

func (op *readvOp) opcode() rawOpcode {
	if op.fixed {
		return rawOpReadvFixed
	}
	return rawOpReadv
}

func (op *readvOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	if err := op.fd.validate(ring); err != nil {
		return err
	}
	if !op.fixed {
		return nil
	}
	if err := op.registered.validate(ring); err != nil {
		return err
	}
	for _, buffer := range op.buffers {
		if len(buffer) != 0 && !op.registered.contains(buffer) {
			return errors.New("ringo: vector is outside the fixed buffer")
		}
	}
	return nil
}

func (op *readvOp) prepare(sqe *rawSQE) {
	fd, flags := op.fd.sqe()
	*sqe = rawSQE{
		Opcode:   uint8(op.opcode()),
		Flags:    uint8(flags),
		Fd:       fd,
		Off:      uint64(op.offset),
		Addr:     uint64(slicePtr(op.iovecs)),
		Len:      uint32(len(op.iovecs)),
		Rw_flags: int32(op.flags),
	}
	if op.fixed {
		sqe.Buf_index = op.registered.index
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

type writevOp struct {
	opBase
	fd            FD
	buffers       [][]byte
	iovecs        []syscall.Iovec
	inlineBuffers [inlineIovecCount][]byte
	inlineIovecs  [inlineIovecCount]syscall.Iovec
	offset        int64
	flags         uint32
	registered    FixedBuffer
	fixed         bool
}

func newWritevOp(
	fd FD,
	buffers [][]byte,
	offset int64,
	flags int,
	registered FixedBuffer,
	fixed bool,
) *writevOp {
	op := &writevOp{
		fd: fd, offset: offset, flags: uint32(flags),
		registered: registered, fixed: fixed,
	}
	if flags < 0 || uint64(flags) > math.MaxUint32 {
		op.fail(errors.New("writev flags do not fit the kernel ABI"))
	}
	var err error
	op.buffers, op.iovecs, err = makeIovecs(
		buffers,
		&op.inlineBuffers,
		&op.inlineIovecs,
	)
	op.fail(err)
	return op
}

func (op *writevOp) opcode() rawOpcode {
	if op.fixed {
		return rawOpWritevFixed
	}
	return rawOpWritev
}

func (op *writevOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	if err := op.fd.validate(ring); err != nil {
		return err
	}
	if !op.fixed {
		return nil
	}
	if err := op.registered.validate(ring); err != nil {
		return err
	}
	for _, buffer := range op.buffers {
		if len(buffer) != 0 && !op.registered.contains(buffer) {
			return errors.New("ringo: vector is outside the fixed buffer")
		}
	}
	return nil
}

func (op *writevOp) prepare(sqe *rawSQE) {
	fd, flags := op.fd.sqe()
	*sqe = rawSQE{
		Opcode:   uint8(op.opcode()),
		Flags:    uint8(flags),
		Fd:       fd,
		Off:      uint64(op.offset),
		Addr:     uint64(slicePtr(op.iovecs)),
		Len:      uint32(len(op.iovecs)),
		Rw_flags: int32(op.flags),
	}
	if op.fixed {
		sqe.Buf_index = op.registered.index
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// Read constructs a positioned read from file into buffer.
// liburing: io_uring_prep_read - https://man7.org/linux/man-pages/man3/io_uring_prep_read.3.html
func Read(file *os.File, buffer []byte, offset int64) Op {
	return newReadOp(FileFD(file), buffer, offset, FixedBuffer{}, false)
}

// ReadDirect constructs a positioned read from a registered-file slot.
// liburing: io_uring_prep_read - https://man7.org/linux/man-pages/man3/io_uring_prep_read.3.html
func ReadDirect(file FixedFile, buffer []byte, offset int64) Op {
	return newReadOp(FixedFD(file), buffer, offset, FixedBuffer{}, false)
}

// ReadFixed constructs a positioned read into a fixed buffer.
// liburing: io_uring_prep_read_fixed - https://man7.org/linux/man-pages/man3/io_uring_prep_read_fixed.3.html
func ReadFixed(file *os.File, buffer FixedBuffer, offset int64) Op {
	return newReadOp(FileFD(file), buffer.data, offset, buffer, true)
}

// ReadFixedDirect constructs a positioned read using both a registered-file
// slot and a fixed buffer.
// liburing: io_uring_prep_read_fixed - https://man7.org/linux/man-pages/man3/io_uring_prep_read_fixed.3.html
func ReadFixedDirect(file FixedFile, buffer FixedBuffer, offset int64) Op {
	return newReadOp(FixedFD(file), buffer.data, offset, buffer, true)
}

// ReadFD is the descriptor-generic form of Read.
// liburing: io_uring_prep_read - https://man7.org/linux/man-pages/man3/io_uring_prep_read.3.html
func ReadFD(fd FD, buffer []byte, offset int64) Op {
	return newReadOp(fd, buffer, offset, FixedBuffer{}, false)
}

// Write constructs a positioned write from buffer to file.
// liburing: io_uring_prep_write - https://man7.org/linux/man-pages/man3/io_uring_prep_write.3.html
func Write(file *os.File, buffer []byte, offset int64) Op {
	return newWriteOp(FileFD(file), buffer, offset, FixedBuffer{}, false)
}

// WriteDirect constructs a positioned write to a registered-file slot.
// liburing: io_uring_prep_write - https://man7.org/linux/man-pages/man3/io_uring_prep_write.3.html
func WriteDirect(file FixedFile, buffer []byte, offset int64) Op {
	return newWriteOp(FixedFD(file), buffer, offset, FixedBuffer{}, false)
}

// WriteFixed constructs a positioned write from a fixed buffer.
// liburing: io_uring_prep_write_fixed - https://man7.org/linux/man-pages/man3/io_uring_prep_write_fixed.3.html
func WriteFixed(file *os.File, buffer FixedBuffer, offset int64) Op {
	return newWriteOp(FileFD(file), buffer.data, offset, buffer, true)
}

// WriteFixedDirect constructs a positioned write using a registered-file slot
// and fixed buffer.
// liburing: io_uring_prep_write_fixed - https://man7.org/linux/man-pages/man3/io_uring_prep_write_fixed.3.html
func WriteFixedDirect(file FixedFile, buffer FixedBuffer, offset int64) Op {
	return newWriteOp(FixedFD(file), buffer.data, offset, buffer, true)
}

// WriteFD is the descriptor-generic form of Write.
// liburing: io_uring_prep_write - https://man7.org/linux/man-pages/man3/io_uring_prep_write.3.html
func WriteFD(fd FD, buffer []byte, offset int64) Op {
	return newWriteOp(fd, buffer, offset, FixedBuffer{}, false)
}

// Readv constructs a vectored positioned read from file.
// liburing: io_uring_prep_readv - https://man7.org/linux/man-pages/man3/io_uring_prep_readv.3.html
func Readv(file *os.File, buffers [][]byte, offset int64) Op {
	return newReadvOp(FileFD(file), buffers, offset, 0, FixedBuffer{}, false)
}

// ReadvDirect constructs a vectored read from a registered-file slot.
// liburing: io_uring_prep_readv - https://man7.org/linux/man-pages/man3/io_uring_prep_readv.3.html
func ReadvDirect(file FixedFile, buffers [][]byte, offset int64) Op {
	return newReadvOp(FixedFD(file), buffers, offset, 0, FixedBuffer{}, false)
}

// ReadvFD is the descriptor-generic form of Readv.
// liburing: io_uring_prep_readv2 - https://man7.org/linux/man-pages/man3/io_uring_prep_readv2.3.html
func ReadvFD(fd FD, buffers [][]byte, offset int64, flags int) Op {
	return newReadvOp(fd, buffers, offset, flags, FixedBuffer{}, false)
}

// ReadvFixed constructs IORING_OP_READV_FIXED. Every vector must fall within
// the same selected fixed-buffer slot.
// liburing: io_uring_prep_readv_fixed - https://man7.org/linux/man-pages/man3/io_uring_prep_readv_fixed.3.html
func ReadvFixed(
	fd FD,
	registered FixedBuffer,
	buffers [][]byte,
	offset int64,
	flags int,
) Op {
	return newReadvOp(fd, buffers, offset, flags, registered, true)
}

// Writev constructs one vectored positioned write containing exactly buffers.
// It does not discover or merge adjacent operations.
// liburing: io_uring_prep_writev - https://man7.org/linux/man-pages/man3/io_uring_prep_writev.3.html
func Writev(file *os.File, buffers [][]byte, offset int64) Op {
	return newWritevOp(FileFD(file), buffers, offset, 0, FixedBuffer{}, false)
}

// WritevDirect constructs a vectored write to a registered-file slot.
// liburing: io_uring_prep_writev - https://man7.org/linux/man-pages/man3/io_uring_prep_writev.3.html
func WritevDirect(file FixedFile, buffers [][]byte, offset int64) Op {
	return newWritevOp(FixedFD(file), buffers, offset, 0, FixedBuffer{}, false)
}

// WritevFD is the descriptor-generic form of Writev.
// liburing: io_uring_prep_writev2 - https://man7.org/linux/man-pages/man3/io_uring_prep_writev2.3.html
func WritevFD(fd FD, buffers [][]byte, offset int64, flags int) Op {
	return newWritevOp(fd, buffers, offset, flags, FixedBuffer{}, false)
}

// WritevFixed constructs IORING_OP_WRITEV_FIXED. Every vector must fall
// within the same selected fixed-buffer slot.
// liburing: io_uring_prep_writev_fixed - https://man7.org/linux/man-pages/man3/io_uring_prep_writev_fixed.3.html
func WritevFixed(
	fd FD,
	registered FixedBuffer,
	buffers [][]byte,
	offset int64,
	flags int,
) Op {
	return newWritevOp(fd, buffers, offset, flags, registered, true)
}

type fsyncOp struct {
	opBase
	fd    FD
	flags uint32
}

func (op *fsyncOp) opcode() rawOpcode { return rawOpFsync }
func (op *fsyncOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	return op.fd.validate(ring)
}
func (op *fsyncOp) prepare(sqe *rawSQE) {
	fd, flags := op.fd.sqe()
	*sqe = rawSQE{
		Opcode:   uint8(rawOpFsync),
		Flags:    uint8(flags),
		Fd:       fd,
		Rw_flags: int32(op.flags),
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// FsyncFD constructs IORING_OP_FSYNC with raw fsync flags.
// liburing: io_uring_prep_fsync - https://man7.org/linux/man-pages/man3/io_uring_prep_fsync.3.html
func FsyncFD(fd FD, flags uint32) Op { return &fsyncOp{fd: fd, flags: flags} }

// Fsync constructs IORING_OP_FSYNC.
// liburing: io_uring_prep_fsync - https://man7.org/linux/man-pages/man3/io_uring_prep_fsync.3.html
func Fsync(file *os.File) Op { return FsyncFD(FileFD(file), 0) }

// FsyncDirect syncs a registered-file slot.
// liburing: io_uring_prep_fsync - https://man7.org/linux/man-pages/man3/io_uring_prep_fsync.3.html
func FsyncDirect(file FixedFile) Op { return FsyncFD(FixedFD(file), 0) }

// Fdatasync constructs IORING_OP_FSYNC with IORING_FSYNC_DATASYNC.
// liburing: io_uring_prep_fsync - https://man7.org/linux/man-pages/man3/io_uring_prep_fsync.3.html
func Fdatasync(file *os.File) Op {
	return FsyncFD(FileFD(file), rawFsyncDatasync)
}

// FdatasyncDirect syncs data for a registered-file slot.
// liburing: io_uring_prep_fsync - https://man7.org/linux/man-pages/man3/io_uring_prep_fsync.3.html
func FdatasyncDirect(file FixedFile) Op {
	return FsyncFD(FixedFD(file), rawFsyncDatasync)
}

type fallocateOp struct {
	opBase
	fd             FD
	mode           int
	offset, length uint64
}

func (op *fallocateOp) opcode() rawOpcode { return rawOpFallocate }
func (op *fallocateOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	return op.fd.validate(ring)
}
func (op *fallocateOp) prepare(sqe *rawSQE) {
	fd, flags := op.fd.sqe()
	*sqe = rawSQE{
		Opcode: uint8(rawOpFallocate),
		Flags:  uint8(flags),
		Fd:     fd,
		Off:    op.offset,
		Addr:   op.length,
		Len:    uint32(op.mode),
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// FallocateMode constructs IORING_OP_FALLOCATE with a Linux fallocate mode.
// liburing: io_uring_prep_fallocate - https://man7.org/linux/man-pages/man3/io_uring_prep_fallocate.3.html
func FallocateMode(fd FD, mode int, offset, length uint64) Op {
	op := &fallocateOp{fd: fd, mode: mode, offset: offset, length: length}
	if mode < 0 || !fitsInt32(mode) {
		op.fail(errors.New("fallocate mode does not fit the kernel ABI"))
	}
	return op
}

// Fallocate allocates file space.
// liburing: io_uring_prep_fallocate - https://man7.org/linux/man-pages/man3/io_uring_prep_fallocate.3.html
func Fallocate(file *os.File, offset, length int64) Op {
	op := &fallocateOp{
		fd: FileFD(file), offset: uint64(offset), length: uint64(length),
	}
	if offset < 0 || length < 0 {
		op.fail(errors.New("fallocate offset and length must be nonnegative"))
	}
	return op
}

// FallocateDirect allocates space in a registered file.
// liburing: io_uring_prep_fallocate - https://man7.org/linux/man-pages/man3/io_uring_prep_fallocate.3.html
func FallocateDirect(file FixedFile, offset, length int64) Op {
	op := &fallocateOp{
		fd: FixedFD(file), offset: uint64(offset), length: uint64(length),
	}
	if offset < 0 || length < 0 {
		op.fail(errors.New("fallocate offset and length must be nonnegative"))
	}
	return op
}

type openAtOp struct {
	opBase
	dir    FD
	path   []byte
	flags  int
	mode   uint32
	direct bool
	target FixedFile
}

// encodeFileIndex writes the sqe.file_index union member, which cgo -godefs
// exposes through its overlapping splice_fd_in field. Zero means no target,
// so the kernel ABI stores an actual fixed-file index plus one.
func (sqe *rawSQE) encodeFileIndex(fileIndex uint32) {
	sqe.Splice_fd_in = int32(fileIndex + 1)
}

func newOpenAtOp(
	dir FD,
	path string,
	flags int,
	mode uint32,
	target *FixedFile,
) *openAtOp {
	copied, err := copyCString(path)
	op := &openAtOp{dir: dir, path: copied, flags: flags, mode: mode}
	op.fail(err)
	if !fitsInt32(flags) {
		op.fail(errors.New("open flags do not fit the kernel ABI"))
	}
	if target != nil {
		op.direct = true
		op.target = *target
	}
	return op
}

func (op *openAtOp) opcode() rawOpcode { return rawOpOpenat }
func (op *openAtOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	if err := op.dir.validate(ring); err != nil {
		return err
	}
	if op.direct {
		if op.dir.kind == descriptorDirect && op.dir.direct == op.target {
			return errors.New("ringo: open directory and target use the same fixed-file slot")
		}
		return op.target.validate(ring)
	}
	return nil
}
func (op *openAtOp) prepare(sqe *rawSQE) {
	dir, flags := op.dir.sqe()
	*sqe = rawSQE{
		Opcode:   uint8(rawOpOpenat),
		Flags:    uint8(flags),
		Fd:       dir,
		Addr:     uint64(slicePtr(op.path)),
		Len:      op.mode,
		Rw_flags: int32(op.flags),
	}
	if op.direct {
		sqe.encodeFileIndex(op.target.index)
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// Open is OpenAt relative to the current working directory.
// liburing: io_uring_prep_openat - https://man7.org/linux/man-pages/man3/io_uring_prep_openat.3.html
func Open(path string, flags int, mode uint32) Op {
	return OpenAt(BorrowedFD(unix.AT_FDCWD), path, flags, mode)
}

// OpenAt constructs IORING_OP_OPENAT relative to dir and copies path.
// liburing: io_uring_prep_openat - https://man7.org/linux/man-pages/man3/io_uring_prep_openat.3.html
func OpenAt(dir FD, path string, flags int, mode uint32) Op {
	return newOpenAtOp(dir, path, flags, mode, nil)
}

// OpenDirect is OpenAtDirect relative to the current working directory.
// liburing: io_uring_prep_openat_direct - https://man7.org/linux/man-pages/man3/io_uring_prep_openat_direct.3.html
func OpenDirect(path string, flags int, mode uint32, file FixedFile) Op {
	return OpenAtDirect(BorrowedFD(unix.AT_FDCWD), path, flags, mode, file)
}

// OpenAtDirect opens relative to dir and installs the result in file. New
// requires IORING_FEAT_LINKED_FILE, so this operation may be linked to a
// following operation that uses the installed slot.
// liburing: io_uring_prep_openat_direct - https://man7.org/linux/man-pages/man3/io_uring_prep_openat_direct.3.html
func OpenAtDirect(dir FD, path string, flags int, mode uint32, file FixedFile) Op {
	return newOpenAtOp(dir, path, flags, mode, &file)
}

type openAt2Op struct {
	opBase
	dir    FD
	path   []byte
	how    unix.OpenHow
	direct bool
	target FixedFile
}

func newOpenAt2Op(
	dir FD,
	path string,
	how unix.OpenHow,
	target *FixedFile,
) *openAt2Op {
	copied, err := copyCString(path)
	op := &openAt2Op{dir: dir, path: copied, how: how}
	op.fail(err)
	if target != nil {
		op.direct = true
		op.target = *target
	}
	return op
}

func (op *openAt2Op) opcode() rawOpcode { return rawOpOpenat2 }
func (op *openAt2Op) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	if err := op.dir.validate(ring); err != nil {
		return err
	}
	if op.direct {
		if op.dir.kind == descriptorDirect && op.dir.direct == op.target {
			return errors.New("ringo: open directory and target use the same fixed-file slot")
		}
		return op.target.validate(ring)
	}
	return nil
}
func (op *openAt2Op) prepare(sqe *rawSQE) {
	dir, flags := op.dir.sqe()
	*sqe = rawSQE{
		Opcode: uint8(rawOpOpenat2),
		Flags:  uint8(flags),
		Fd:     dir,
		Off:    uint64(uintptr(unsafe.Pointer(&op.how))),
		Addr:   uint64(slicePtr(op.path)),
		Len:    uint32(unsafe.Sizeof(op.how)),
	}
	if op.direct {
		sqe.encodeFileIndex(op.target.index)
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// OpenAt2 constructs IORING_OP_OPENAT2 relative to dir and copies how.
// liburing: io_uring_prep_openat2 - https://man7.org/linux/man-pages/man3/io_uring_prep_openat2.3.html
func OpenAt2(dir FD, path string, how unix.OpenHow) Op {
	return newOpenAt2Op(dir, path, how, nil)
}

// OpenAt2Direct installs the opened file into a fixed-file slot. New requires
// IORING_FEAT_LINKED_FILE, so this operation may be linked to a following
// operation that uses the installed slot.
// liburing: io_uring_prep_openat2_direct - https://man7.org/linux/man-pages/man3/io_uring_prep_openat2_direct.3.html
func OpenAt2Direct(dir FD, path string, how unix.OpenHow, file FixedFile) Op {
	return newOpenAt2Op(dir, path, how, &file)
}

type statxOp struct {
	opBase
	dir    FD
	path   []byte
	flags  int
	mask   uint32
	result *unix.Statx_t
}

func (op *statxOp) opcode() rawOpcode { return rawOpStatx }
func (op *statxOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	if err := op.dir.validate(ring); err != nil {
		return err
	}
	if op.result == nil {
		return errors.New("ringo: statx result is nil")
	}
	return nil
}
func (op *statxOp) prepare(sqe *rawSQE) {
	dir, flags := op.dir.sqe()
	*sqe = rawSQE{
		Opcode:   uint8(rawOpStatx),
		Flags:    uint8(flags),
		Fd:       dir,
		Off:      uint64(uintptr(unsafe.Pointer(op.result))),
		Addr:     uint64(slicePtr(op.path)),
		Len:      op.mask,
		Rw_flags: int32(op.flags),
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// Statx is StatxAt relative to the current working directory.
// liburing: io_uring_prep_statx - https://man7.org/linux/man-pages/man3/io_uring_prep_statx.3.html
func Statx(
	path string,
	flags int,
	mask uint32,
	result *unix.Statx_t,
) Op {
	return StatxAt(BorrowedFD(unix.AT_FDCWD), path, flags, mask, result)
}

// StatxAt constructs IORING_OP_STATX relative to dir and retains result until
// final completion. An empty path is accepted only with AT_EMPTY_PATH.
// liburing: io_uring_prep_statx - https://man7.org/linux/man-pages/man3/io_uring_prep_statx.3.html
func StatxAt(
	dir FD,
	path string,
	flags int,
	mask uint32,
	result *unix.Statx_t,
) Op {
	copied, err := copyCStringAllowEmpty(path, flags&unix.AT_EMPTY_PATH != 0)
	op := &statxOp{
		dir: dir, path: copied, flags: flags, mask: mask, result: result,
	}
	op.fail(err)
	if !fitsInt32(flags) {
		op.fail(errors.New("statx flags do not fit the kernel ABI"))
	}
	return op
}

type closeDirectOp struct {
	opBase
	file FixedFile
}

func (op *closeDirectOp) opcode() rawOpcode { return rawOpClose }
func (op *closeDirectOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	return op.file.validate(ring)
}
func (op *closeDirectOp) prepare(sqe *rawSQE) {
	*sqe = rawSQE{Opcode: uint8(rawOpClose)}
	sqe.encodeFileIndex(op.file.index)
	sqe.Flags |= uint8(op.sqeFlags)
}

// CloseDirect removes and closes the file in a registered-file slot.
// liburing: io_uring_prep_close_direct - https://man7.org/linux/man-pages/man3/io_uring_prep_close_direct.3.html
func CloseDirect(file FixedFile) Op { return &closeDirectOp{file: file} }
