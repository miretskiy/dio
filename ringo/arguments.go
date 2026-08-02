//go:build linux

package ringo

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type descriptorKind uint8

const (
	descriptorRegular descriptorKind = iota
	descriptorDirect
	descriptorBorrowed
)

// FD identifies a descriptor argument to an operation. Its three constructors
// differ only in how the descriptor's lifetime is anchored; none of them
// transfers ownership of the descriptor to the Ring, and the Ring never closes
// a descriptor it did not open:
//
//   - FileFD retains a reference to an *os.File so the file cannot be finalized
//     while the kernel may still use its descriptor. The caller keeps ownership
//     and remains responsible for closing the file.
//   - FixedFD names a ring-scoped registered-file slot; the slot, not this FD,
//     owns the installed file.
//   - BorrowedFD wraps a raw descriptor and retains nothing; its lifetime is
//     entirely the caller's responsibility.
type FD struct {
	kind   descriptorKind
	file   *os.File
	direct FixedFile
	fd     int
}

// FileFD returns a descriptor backed by file. The Ring retains file through
// the operation's final completion so the descriptor stays valid, but does not
// take ownership: the caller still closes the file and must not do so until the
// operation completes. There is deliberately no way to close an *os.File
// through the Ring; use CloseFD with a borrowed descriptor or CloseDirect with
// a fixed-file slot instead.
func FileFD(file *os.File) FD {
	return FD{kind: descriptorRegular, file: file}
}

// BorrowedFD returns a raw descriptor whose lifetime remains the caller's
// responsibility. The Ring retains nothing.
func BorrowedFD(fd int) FD {
	return FD{kind: descriptorBorrowed, fd: fd}
}

// FixedFD returns a descriptor backed by a ring-scoped fixed-file slot.
func FixedFD(file FixedFile) FD {
	return FD{kind: descriptorDirect, direct: file}
}

// AtCWD names the current working directory as the directory argument for path
// operations such as OpenAt and StatxAt. It corresponds to AT_FDCWD and, like
// BorrowedFD, retains nothing.
func AtCWD() FD {
	return BorrowedFD(unix.AT_FDCWD)
}

func (fd FD) validate(ring *Ring) error {
	switch fd.kind {
	case descriptorRegular:
		if fd.file == nil {
			return errors.New("ringo: operation requires a non-nil file")
		}
		value := int(fd.file.Fd())
		if !fitsInt32(value) {
			return fmt.Errorf(
				"ringo: file descriptor %d does not fit the kernel ABI",
				value,
			)
		}
	case descriptorDirect:
		return fd.direct.validate(ring)
	case descriptorBorrowed:
		if !fitsInt32(fd.fd) {
			return fmt.Errorf(
				"ringo: descriptor %d does not fit the kernel ABI",
				fd.fd,
			)
		}
	default:
		return errors.New("ringo: invalid descriptor")
	}
	return nil
}

func (fd FD) value() (value int, direct bool) {
	switch fd.kind {
	case descriptorRegular:
		return int(fd.file.Fd()), false
	case descriptorDirect:
		return int(fd.direct.index), true
	case descriptorBorrowed:
		return fd.fd, false
	default:
		panic("ringo: invalid validated descriptor")
	}
}

func (fd FD) sqe() (int32, rawSQEFlags) {
	value, direct := fd.value()
	if direct {
		return int32(value), rawSqeFixedFile
	}
	return int32(value), 0
}

func (fd FD) cancel() (int32, rawCancelFlags) {
	value, direct := fd.value()
	if direct {
		return int32(value), rawAsyncCancelFDFixed
	}
	return int32(value), 0
}

func copyCString(value string) ([]byte, error) {
	return copyCStringAllowEmpty(value, false)
}

func copyCStringAllowEmpty(value string, allowEmpty bool) ([]byte, error) {
	if value == "" && !allowEmpty {
		return nil, errors.New("path is empty")
	}
	if strings.IndexByte(value, 0) >= 0 {
		return nil, errors.New("path contains NUL")
	}
	return append([]byte(value), 0), nil
}

// makeIovecs converts vectors into operation-owned iovecs, skipping empty
// buffers. It does not retain the [][]byte: each iovec.Base is a typed *byte
// into the buffer it describes, and that is what keeps the buffer reachable
// through the operation's final completion. The caller still owns vectors, and
// the operation never reads it again -- validation works from the iovecs.
// spare is an already-cleared array a recycled operation kept from its previous
// use, or nil. It is only consulted when the vectors do not fit inline.
func makeIovecs(
	vectors [][]byte,
	inlineIovecs *[inlineIovecCount]syscall.Iovec,
	spare []syscall.Iovec,
) ([]syscall.Iovec, error) {
	if len(vectors) > maxIovecs {
		return nil, errors.New("too many vectors")
	}
	count := nonemptyBuffers(vectors)
	var iovecs []syscall.Iovec
	switch {
	case count == 0:
		// Leave iovecs nil so the SQE encodes address 0 rather than the
		// unspecified address unsafe.SliceData yields for a zero-capacity slice.
	case count <= len(inlineIovecs):
		iovecs = inlineIovecs[:0:count]
	case count <= cap(spare):
		iovecs = spare[:0]
	default:
		iovecs = make([]syscall.Iovec, 0, count)
	}
	for _, buffer := range vectors {
		if len(buffer) == 0 {
			continue
		}
		iovec := syscall.Iovec{Base: unsafe.SliceData(buffer)}
		iovec.SetLen(len(buffer))
		iovecs = append(iovecs, iovec)
	}
	return iovecs, nil
}

func nonemptyBuffers(vectors [][]byte) int {
	count := 0
	for _, buffer := range vectors {
		if len(buffer) != 0 {
			count++
		}
	}
	return count
}

func slicePtr[T any](slice []T) uintptr {
	return uintptr(unsafe.Pointer(unsafe.SliceData(slice)))
}
