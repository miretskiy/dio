//go:build linux

package ringo

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"unsafe"
)

type descriptorKind uint8

const (
	descriptorRegular descriptorKind = iota
	descriptorDirect
	descriptorBorrowed
)

// FD identifies a descriptor argument to an operation. FileFD retains an
// *os.File through final completion, BorrowedFD retains no owner, and FixedFD
// names a ring-scoped registered-file slot.
type FD struct {
	kind   descriptorKind
	file   *os.File
	direct FixedFile
	fd     int
}

// FileFD returns a descriptor that retains file through final completion.
func FileFD(file *os.File) FD {
	return FD{kind: descriptorRegular, file: file}
}

// BorrowedFD returns a raw descriptor whose lifetime remains the caller's
// responsibility.
func BorrowedFD(fd int) FD {
	return FD{kind: descriptorBorrowed, fd: fd}
}

// FixedFD returns a descriptor backed by a ring-scoped fixed-file slot.
func FixedFD(file FixedFile) FD {
	return FD{kind: descriptorDirect, direct: file}
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

func makeIovecs(
	vectors [][]byte,
	inlineVectors *[inlineIovecCount][]byte,
	inlineIovecs *[inlineIovecCount]syscall.Iovec,
) ([][]byte, []syscall.Iovec, error) {
	if len(vectors) > maxIovecs {
		return nil, nil, errors.New("too many vectors")
	}
	var owned [][]byte
	if len(vectors) <= len(inlineVectors) {
		owned = inlineVectors[:len(vectors)]
		copy(owned, vectors)
	} else {
		owned = append([][]byte(nil), vectors...)
	}
	count := nonemptyBuffers(owned)
	var iovecs []syscall.Iovec
	if count <= len(inlineIovecs) {
		iovecs = inlineIovecs[:0:count]
	} else {
		iovecs = make([]syscall.Iovec, 0, count)
	}
	for _, buffer := range owned {
		if len(buffer) == 0 {
			continue
		}
		iovec := syscall.Iovec{Base: unsafe.SliceData(buffer)}
		iovec.SetLen(len(buffer))
		iovecs = append(iovecs, iovec)
	}
	return owned, iovecs, nil
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
