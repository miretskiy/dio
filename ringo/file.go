//go:build linux

package ringo

import (
	"errors"
	"math"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type readOp struct {
	opBase
	fd         FD
	buffer     []byte
	offset     int64
	registered FixedBuffer
	fixed      bool
}

func newReadOp(
	alloc *OpAlloc,
	fd FD,
	buffer []byte,
	offset int64,
	registered FixedBuffer,
	fixed bool,
) *readOp {
	op := alloc.newReadOp()
	op.fd, op.buffer, op.offset = fd, buffer, offset
	op.registered, op.fixed = registered, fixed
	if uint64(len(buffer)) > math.MaxUint32 {
		op.fail(errors.New("read buffer is too large for an SQE"))
	}
	return op
}

func (op *readOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.reads.put(op)
}

func (op *readOp) reset() { *op = readOp{} }

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

type writeOp struct {
	opBase
	fd         FD
	buffer     []byte
	offset     int64
	registered FixedBuffer
	fixed      bool
}

func newWriteOp(
	alloc *OpAlloc,
	fd FD,
	buffer []byte,
	offset int64,
	registered FixedBuffer,
	fixed bool,
) *writeOp {
	op := alloc.newWriteOp()
	op.fd, op.buffer, op.offset = fd, buffer, offset
	op.registered, op.fixed = registered, fixed
	if uint64(len(buffer)) > math.MaxUint32 {
		op.fail(errors.New("write buffer is too large for an SQE"))
	}
	return op
}

func (op *writeOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.writes.put(op)
}

func (op *writeOp) reset() { *op = writeOp{} }

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

// readvOp retains its vectors only as iovecs. Each iovec.Base is a typed *byte,
// which is what keeps the caller's buffers reachable; a parallel [][]byte would
// duplicate that and cost the operation an inline array.
type readvOp struct {
	opBase
	fd           FD
	iovecs       []syscall.Iovec
	inlineIovecs [inlineIovecCount]syscall.Iovec
	offset       int64
	flags        uint32
	registered   FixedBuffer
	fixed        bool
}

func newReadvOp(
	alloc *OpAlloc,
	fd FD,
	buffers [][]byte,
	offset int64,
	flags int,
	registered FixedBuffer,
	fixed bool,
) *readvOp {
	op := alloc.newReadvOp()
	op.fd, op.offset, op.flags = fd, offset, uint32(flags)
	op.registered, op.fixed = registered, fixed
	if flags < 0 || uint64(flags) > math.MaxUint32 {
		op.fail(errors.New("readv flags do not fit the kernel ABI"))
	}
	var err error
	op.iovecs, err = makeIovecs(buffers, &op.inlineIovecs, op.iovecs)
	op.fail(err)
	return op
}

func (op *readvOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.readvs.put(op)
}

// reset keeps the heap-allocated iovec array, which is the one allocation a
// vectored operation makes and the whole reason recycling helps it. Vectors that
// fit inline are not worth keeping: that array is part of the operation and the
// zeroing below clears it anyway.
//
// Clearing the retained array is mandatory, not tidiness. Go traces a backing
// array to its capacity, so an iovec left pointing into a caller's buffer would
// pin that buffer for the life of the OpAlloc.
func (op *readvOp) reset() {
	spare := op.iovecs
	if cap(spare) <= len(op.inlineIovecs) {
		spare = nil
	} else {
		clear(spare[:cap(spare)])
		spare = spare[:0]
	}
	*op = readvOp{}
	op.iovecs = spare
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
	for _, iovec := range op.iovecs {
		if !op.registered.containsIovec(iovec) {
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

// writevOp retains its vectors only as iovecs, for the reason readvOp does.
type writevOp struct {
	opBase
	fd           FD
	iovecs       []syscall.Iovec
	inlineIovecs [inlineIovecCount]syscall.Iovec
	offset       int64
	flags        uint32
	registered   FixedBuffer
	fixed        bool
}

func newWritevOp(
	alloc *OpAlloc,
	fd FD,
	buffers [][]byte,
	offset int64,
	flags int,
	registered FixedBuffer,
	fixed bool,
) *writevOp {
	op := alloc.newWritevOp()
	op.fd, op.offset, op.flags = fd, offset, uint32(flags)
	op.registered, op.fixed = registered, fixed
	if flags < 0 || uint64(flags) > math.MaxUint32 {
		op.fail(errors.New("writev flags do not fit the kernel ABI"))
	}
	var err error
	op.iovecs, err = makeIovecs(buffers, &op.inlineIovecs, op.iovecs)
	op.fail(err)
	return op
}

func (op *writevOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.writevs.put(op)
}

// reset keeps the heap-allocated iovec array, which is the one allocation a
// vectored operation makes and the whole reason recycling helps it. Vectors that
// fit inline are not worth keeping: that array is part of the operation and the
// zeroing below clears it anyway.
//
// Clearing the retained array is mandatory, not tidiness. Go traces a backing
// array to its capacity, so an iovec left pointing into a caller's buffer would
// pin that buffer for the life of the OpAlloc.
func (op *writevOp) reset() {
	spare := op.iovecs
	if cap(spare) <= len(op.inlineIovecs) {
		spare = nil
	} else {
		clear(spare[:cap(spare)])
		spare = spare[:0]
	}
	*op = writevOp{}
	op.iovecs = spare
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
	for _, iovec := range op.iovecs {
		if !op.registered.containsIovec(iovec) {
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

// Read constructs a positioned read from fd into buffer.
// liburing: io_uring_prep_read - https://man7.org/linux/man-pages/man3/io_uring_prep_read.3.html
func Read(fd FD, buffer []byte, offset int64, options ...OpOption) Op {
	return newReadOp(optionAlloc(options), fd, buffer, offset, FixedBuffer{}, false)
}

// ReadFixed constructs a positioned read from fd into a registered buffer.
// liburing: io_uring_prep_read_fixed - https://man7.org/linux/man-pages/man3/io_uring_prep_read_fixed.3.html
func ReadFixed(fd FD, buffer FixedBuffer, offset int64, options ...OpOption) Op {
	return newReadOp(optionAlloc(options), fd, buffer.data, offset, buffer, true)
}

// Write constructs a positioned write from buffer to fd.
// liburing: io_uring_prep_write - https://man7.org/linux/man-pages/man3/io_uring_prep_write.3.html
func Write(fd FD, buffer []byte, offset int64, options ...OpOption) Op {
	return newWriteOp(optionAlloc(options), fd, buffer, offset, FixedBuffer{}, false)
}

// WriteFixed constructs a positioned write from a registered buffer to fd.
// liburing: io_uring_prep_write_fixed - https://man7.org/linux/man-pages/man3/io_uring_prep_write_fixed.3.html
func WriteFixed(fd FD, buffer FixedBuffer, offset int64, options ...OpOption) Op {
	return newWriteOp(optionAlloc(options), fd, buffer.data, offset, buffer, true)
}

// Readv constructs a vectored positioned read from fd. flags carries RWF_*
// preadv2 flags, or zero for none.
// liburing: io_uring_prep_readv2 - https://man7.org/linux/man-pages/man3/io_uring_prep_readv2.3.html
func Readv(fd FD, buffers [][]byte, offset int64, flags int, options ...OpOption) Op {
	return newReadvOp(
		optionAlloc(options), fd, buffers, offset, flags, FixedBuffer{}, false,
	)
}

// ReadvFixed constructs IORING_OP_READV_FIXED. Every vector must fall within
// the same selected fixed-buffer slot. The opcode requires Linux 6.15, later
// than the minimum New enforces, so an older kernel fails the operation in its
// completion; Ring.Probe reports support up front.
// liburing: io_uring_prep_readv_fixed - https://man7.org/linux/man-pages/man3/io_uring_prep_readv_fixed.3.html
func ReadvFixed(
	fd FD,
	registered FixedBuffer,
	buffers [][]byte,
	offset int64,
	flags int,
	options ...OpOption,
) Op {
	return newReadvOp(
		optionAlloc(options), fd, buffers, offset, flags, registered, true,
	)
}

// Writev constructs one vectored positioned write to fd containing exactly
// buffers. flags carries RWF_* pwritev2 flags, or zero for none. It does not
// discover or merge adjacent operations.
// liburing: io_uring_prep_writev2 - https://man7.org/linux/man-pages/man3/io_uring_prep_writev2.3.html
func Writev(fd FD, buffers [][]byte, offset int64, flags int, options ...OpOption) Op {
	return newWritevOp(
		optionAlloc(options), fd, buffers, offset, flags, FixedBuffer{}, false,
	)
}

// WritevFixed constructs IORING_OP_WRITEV_FIXED. Every vector must fall
// within the same selected fixed-buffer slot. The opcode requires Linux 6.15,
// later than the minimum New enforces, so an older kernel fails the operation
// in its completion; Ring.Probe reports support up front.
// liburing: io_uring_prep_writev_fixed - https://man7.org/linux/man-pages/man3/io_uring_prep_writev_fixed.3.html
func WritevFixed(
	fd FD,
	registered FixedBuffer,
	buffers [][]byte,
	offset int64,
	flags int,
	options ...OpOption,
) Op {
	return newWritevOp(
		optionAlloc(options), fd, buffers, offset, flags, registered, true,
	)
}

type fsyncOp struct {
	opBase
	fd    FD
	flags uint32
}

func newFsyncOp(alloc *OpAlloc, fd FD, flags uint32) *fsyncOp {
	op := alloc.newFsyncOp()
	op.fd, op.flags = fd, flags
	return op
}

func (op *fsyncOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.fsyncs.put(op)
}

func (op *fsyncOp) reset() { *op = fsyncOp{} }

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

// Fsync constructs IORING_OP_FSYNC for fd.
// liburing: io_uring_prep_fsync - https://man7.org/linux/man-pages/man3/io_uring_prep_fsync.3.html
func Fsync(fd FD, options ...OpOption) Op {
	return newFsyncOp(optionAlloc(options), fd, 0)
}

// Fdatasync constructs IORING_OP_FSYNC with IORING_FSYNC_DATASYNC for fd.
// liburing: io_uring_prep_fsync - https://man7.org/linux/man-pages/man3/io_uring_prep_fsync.3.html
func Fdatasync(fd FD, options ...OpOption) Op {
	return newFsyncOp(optionAlloc(options), fd, rawFsyncDatasync)
}

type fallocateOp struct {
	opBase
	fd             FD
	mode           FallocateFlags
	offset, length uint64
}

func newFallocateOp(
	alloc *OpAlloc,
	fd FD,
	mode FallocateFlags,
	offset, length int64,
) *fallocateOp {
	op := alloc.newFallocateOp()
	op.fd, op.mode = fd, mode
	op.offset, op.length = uint64(offset), uint64(length)
	if offset < 0 || length < 0 {
		op.fail(errors.New("fallocate offset and length must be nonnegative"))
	}
	return op
}

func (op *fallocateOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.fallocates.put(op)
}

func (op *fallocateOp) reset() { *op = fallocateOp{} }

func (op *fallocateOp) opcode() rawOpcode { return rawOpFallocate }
func (op *fallocateOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	if op.mode&^allFallocateFlags != 0 {
		return errors.New("ringo: invalid fallocate mode")
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

// Fallocate allocates space in fd using mode 0. offset and length must be
// nonnegative.
// liburing: io_uring_prep_fallocate - https://man7.org/linux/man-pages/man3/io_uring_prep_fallocate.3.html
func Fallocate(fd FD, offset, length int64, options ...OpOption) Op {
	return newFallocateOp(optionAlloc(options), fd, 0, offset, length)
}

// FallocateMode constructs IORING_OP_FALLOCATE with an explicit fallocate
// mode. offset and length must be nonnegative.
// liburing: io_uring_prep_fallocate - https://man7.org/linux/man-pages/man3/io_uring_prep_fallocate.3.html
func FallocateMode(
	fd FD,
	mode FallocateFlags,
	offset, length int64,
	options ...OpOption,
) Op {
	return newFallocateOp(optionAlloc(options), fd, mode, offset, length)
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
	alloc *OpAlloc,
	dir FD,
	path string,
	flags int,
	mode uint32,
	target *FixedFile,
) *openAtOp {
	copied, err := copyCString(path)
	op := alloc.newOpenAtOp()
	op.dir, op.path, op.flags, op.mode = dir, copied, flags, mode
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

func (op *openAtOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.openAts.put(op)
}

func (op *openAtOp) reset() { *op = openAtOp{} }

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

// OpenAt constructs IORING_OP_OPENAT relative to dir and copies path. Use
// AtCWD as dir to open relative to the current working directory.
// liburing: io_uring_prep_openat - https://man7.org/linux/man-pages/man3/io_uring_prep_openat.3.html
func OpenAt(dir FD, path string, flags int, mode uint32, options ...OpOption) Op {
	return newOpenAtOp(optionAlloc(options), dir, path, flags, mode, nil)
}

// OpenAtDirect opens relative to dir and installs the result in file. New
// requires IORING_FEAT_LINKED_FILE, so this operation may be linked to a
// following operation that uses the installed slot.
// liburing: io_uring_prep_openat_direct - https://man7.org/linux/man-pages/man3/io_uring_prep_openat_direct.3.html
func OpenAtDirect(
	dir FD,
	path string,
	flags int,
	mode uint32,
	file FixedFile,
	options ...OpOption,
) Op {
	return newOpenAtOp(optionAlloc(options), dir, path, flags, mode, &file)
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
	alloc *OpAlloc,
	dir FD,
	path string,
	how unix.OpenHow,
	target *FixedFile,
) *openAt2Op {
	copied, err := copyCString(path)
	op := alloc.newOpenAt2Op()
	op.dir, op.path, op.how = dir, copied, how
	op.fail(err)
	if target != nil {
		op.direct = true
		op.target = *target
	}
	return op
}

func (op *openAt2Op) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.openAt2s.put(op)
}

func (op *openAt2Op) reset() { *op = openAt2Op{} }

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
func OpenAt2(dir FD, path string, how unix.OpenHow, options ...OpOption) Op {
	return newOpenAt2Op(optionAlloc(options), dir, path, how, nil)
}

// OpenAt2Direct installs the opened file into a fixed-file slot. New requires
// IORING_FEAT_LINKED_FILE, so this operation may be linked to a following
// operation that uses the installed slot.
// liburing: io_uring_prep_openat2_direct - https://man7.org/linux/man-pages/man3/io_uring_prep_openat2_direct.3.html
func OpenAt2Direct(
	dir FD,
	path string,
	how unix.OpenHow,
	file FixedFile,
	options ...OpOption,
) Op {
	return newOpenAt2Op(optionAlloc(options), dir, path, how, &file)
}

type statxOp struct {
	opBase
	dir    FD
	path   []byte
	flags  int
	mask   uint32
	result *unix.Statx_t
}

func (op *statxOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.statxes.put(op)
}

func (op *statxOp) reset() { *op = statxOp{} }

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

// StatxAt constructs IORING_OP_STATX relative to dir and retains result until
// final completion. Use AtCWD as dir to resolve relative to the current
// working directory. An empty path is accepted only with AT_EMPTY_PATH.
// liburing: io_uring_prep_statx - https://man7.org/linux/man-pages/man3/io_uring_prep_statx.3.html
func StatxAt(
	dir FD,
	path string,
	flags int,
	mask uint32,
	result *unix.Statx_t,
	options ...OpOption,
) Op {
	copied, err := copyCStringAllowEmpty(path, flags&unix.AT_EMPTY_PATH != 0)
	alloc := optionAlloc(options)
	op := alloc.newStatxOp()
	op.dir, op.path, op.flags = dir, copied, flags
	op.mask, op.result = mask, result
	op.fail(err)
	if !fitsInt32(flags) {
		op.fail(errors.New("statx flags do not fit the kernel ABI"))
	}
	return op
}

type ftruncateOp struct {
	opBase
	fd     FD
	length int64
}

func (op *ftruncateOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.ftruncates.put(op)
}

func (op *ftruncateOp) reset() { *op = ftruncateOp{} }

func (op *ftruncateOp) opcode() rawOpcode { return rawOpFtruncate }
func (op *ftruncateOp) validate(ring *Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	return op.fd.validate(ring)
}
func (op *ftruncateOp) prepare(sqe *rawSQE) {
	fd, flags := op.fd.sqe()
	*sqe = rawSQE{
		Opcode: uint8(rawOpFtruncate),
		Flags:  uint8(flags),
		Fd:     fd,
		Off:    uint64(op.length),
	}
	sqe.Flags |= uint8(op.sqeFlags)
}

// Ftruncate constructs IORING_OP_FTRUNCATE. The opcode requires Linux 6.9,
// later than the minimum New enforces, so an older kernel fails the operation
// in its completion; Ring.Probe reports support up front.
// liburing: io_uring_prep_ftruncate - https://man7.org/linux/man-pages/man3/io_uring_prep_ftruncate.3.html
func Ftruncate(fd FD, length int64, options ...OpOption) Op {
	alloc := optionAlloc(options)
	op := alloc.newFtruncateOp()
	op.fd, op.length = fd, length
	if length < 0 {
		op.fail(errors.New("ftruncate length must be nonnegative"))
	}
	return op
}

type closeDirectOp struct {
	opBase
	file FixedFile
}

func (op *closeDirectOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.closeDirects.put(op)
}

func (op *closeDirectOp) reset() { *op = closeDirectOp{} }

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
func CloseDirect(file FixedFile, options ...OpOption) Op {
	alloc := optionAlloc(options)
	op := alloc.newCloseDirectOp()
	op.file = file
	return op
}

type closeFDOp struct {
	opBase
	fd int
}

func (op *closeFDOp) release() {
	alloc := op.alloc
	if alloc == nil {
		return
	}
	op.reset()
	alloc.closeFDs.put(op)
}

func (op *closeFDOp) reset() { *op = closeFDOp{} }

func (op *closeFDOp) opcode() rawOpcode { return rawOpClose }
func (op *closeFDOp) validate(*Ring) error {
	if err := op.opBase.validate(); err != nil {
		return err
	}
	if !fitsInt32(op.fd) {
		return errors.New("ringo: descriptor does not fit the kernel ABI")
	}
	return nil
}
func (op *closeFDOp) prepare(sqe *rawSQE) {
	*sqe = rawSQE{Opcode: uint8(rawOpClose), Fd: int32(op.fd)}
	sqe.Flags |= uint8(op.sqeFlags)
}

// CloseFD asynchronously closes a borrowed process descriptor.
// liburing: io_uring_prep_close - https://man7.org/linux/man-pages/man3/io_uring_prep_close.3.html
func CloseFD(fd int, options ...OpOption) Op {
	alloc := optionAlloc(options)
	op := alloc.newCloseFDOp()
	op.fd = fd
	return op
}
