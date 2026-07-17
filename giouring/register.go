//go:build linux

// MIT License
//
// Copyright (c) 2023 Paweł Gaczyński
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
// TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
// SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package giouring

import (
	"fmt"
	"os"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

func (ring *Ring) doRegisterErrno(opCode uint32, arg unsafe.Pointer, nrArgs uint32) (uint, syscall.Errno) {
	var fd int

	if ring.intFlags&IntFlagRegRing != 0 {
		opCode |= RegisterUseRegisteredRing
		fd = ring.enterRingFd
	} else {
		fd = ring.ringFd
	}

	return ring.Register(fd, opCode, arg, nrArgs)
}

// syscallRegister retains arg itself through the syscall. Wrappers whose arg
// contains integer-encoded addresses must additionally retain the referenced
// Go objects until doRegister returns.
func (ring *Ring) doRegister(opCode uint32, arg unsafe.Pointer, nrArgs uint32) (uint, error) {
	ret, errno := ring.doRegisterErrno(opCode, arg, nrArgs)
	if errno != 0 {
		return 0, os.NewSyscallError("io_uring_register", errno)
	}

	return ret, nil
}

// liburing: io_uring_register_buffers_update_tag - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_buffers_update_tag.3.en.html
func (ring *Ring) RegisterBuffersUpdateTag(off uint32, iovecs []syscall.Iovec, tags *uint64, nr uint32) (uint, error) {
	rsrcUpdate := &RsrcUpdate2{
		Offset: off,
		Data:   uint64(slicePtr(iovecs)),
		Tags:   uint64(uintptr(unsafe.Pointer(tags))),
		Nr:     nr,
	}

	result, err := ring.doRegister(RegisterBuffersUpdate, unsafe.Pointer(rsrcUpdate), uint32(unsafe.Sizeof(*rsrcUpdate)))
	runtime.KeepAlive(iovecs)
	runtime.KeepAlive(tags)

	return result, err
}

// liburing: io_uring_register_buffers_tags - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_buffers_tags.3.en.html
func (ring *Ring) RegisterBuffersTags(iovecs []syscall.Iovec, tags []uint64) (uint, error) {
	if len(iovecs) != len(tags) {
		return 0, syscall.EINVAL
	}
	reg := &RsrcRegister{
		Nr:   uint32(len(iovecs)),
		Data: uint64(slicePtr(iovecs)),
		Tags: uint64(slicePtr(tags)),
	}

	result, err := ring.doRegister(RegisterBuffers2, unsafe.Pointer(reg), uint32(unsafe.Sizeof(*reg)))
	runtime.KeepAlive(iovecs)
	runtime.KeepAlive(tags)

	return result, err
}

// liburing: io_uring_register_buffers_sparse - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_buffers_sparse.3.en.html
func (ring *Ring) RegisterBuffersSparse(nr uint32) (uint, error) {
	reg := &RsrcRegister{
		Flags: RsrcRegisterSparse,
		Nr:    nr,
	}

	result, err := ring.doRegister(RegisterBuffers2, unsafe.Pointer(reg), uint32(unsafe.Sizeof(*reg)))
	runtime.KeepAlive(reg)

	return result, err
}

// RegisterBuffers registers the memory described by iovecs. It retains the
// iovec metadata through the registration syscall; the caller must retain the
// described backing memory until UnregisterBuffers or QueueExit.
// liburing: io_uring_register_buffers - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_buffers.3.en.html
func (ring *Ring) RegisterBuffers(iovecs []syscall.Iovec) (uint, error) {
	result, err := ring.doRegister(RegisterBuffers, unsafe.Pointer(unsafe.SliceData(iovecs)), uint32(len(iovecs)))
	runtime.KeepAlive(iovecs)
	return result, err
}

// liburing: io_uring_unregister_buffers - https://manpages.debian.org/unstable/liburing-dev/io_uring_unregister_buffers.3.en.html
func (ring *Ring) UnregisterBuffers() (uint, error) {
	return ring.doRegister(UnregisterBuffers, unsafe.Pointer(nil), 0)
}

// liburing: io_uring_register_files_update_tag - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_files_update_tag.3.en.html
func (ring *Ring) RegisterFilesUpdateTag(off uint, files []int32, tags []uint64) (uint, error) {
	if len(files) != len(tags) {
		return 0, syscall.EINVAL
	}
	update := &RsrcUpdate2{
		Offset: uint32(off),
		Data:   uint64(slicePtr(files)),
		Tags:   uint64(slicePtr(tags)),
		Nr:     uint32(len(files)),
	}

	result, err := ring.doRegister(RegisterFilesUpdate2, unsafe.Pointer(update), uint32(unsafe.Sizeof(*update)))
	runtime.KeepAlive(files)
	runtime.KeepAlive(tags)

	return result, err
}

// liburing: io_uring_register_files_update - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_files_update.3.en.html
func (ring *Ring) RegisterFilesUpdate(off uint, files []int32) (uint, error) {
	update := &FilesUpdate{
		Offset: uint32(off),
		Fds:    uint64(slicePtr(files)),
	}

	result, err := ring.doRegister(RegisterFilesUpdate, unsafe.Pointer(update), uint32(len(files)))
	runtime.KeepAlive(files)

	return result, err
}

// liburing: increase_rlimit_nofile
func increaseRlimitNofile(nr uint64) error {
	rlim := syscall.Rlimit{}

	err := syscall.Getrlimit(unix.RLIMIT_NOFILE, &rlim)
	if err != nil {
		return err
	}

	if rlim.Cur < nr {
		rlim.Cur += nr

		err = syscall.Setrlimit(unix.RLIMIT_NOFILE, &rlim)
		if err != nil {
			return err
		}
	}

	return nil
}

// liburing: io_uring_register_files_sparse - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_files_sparse.3.en.html
func (ring *Ring) RegisterFilesSparse(nr uint32) (uint, error) {
	reg := &RsrcRegister{
		Flags: RsrcRegisterSparse,
		Nr:    nr,
	}

	var (
		ret         uint
		err         error
		errno       syscall.Errno
		didIncrease bool
	)

	for {
		ret, errno = ring.doRegisterErrno(RegisterFiles2, unsafe.Pointer(reg), uint32(unsafe.Sizeof(*reg)))
		if errno == 0 {
			return ret, nil
		}
		if errno == syscall.EMFILE && !didIncrease {
			didIncrease = true

			err = increaseRlimitNofile(uint64(nr))
			if err != nil {
				return ret, err
			}

			continue
		}

		return ret, os.NewSyscallError("io_uring_register", errno)
	}
}

// liburing: io_uring_register_files_tags - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_files_tags.3.en.html
func (ring *Ring) RegisterFilesTags(files []int32, tags []uint64) (uint, error) {
	if len(files) != len(tags) {
		return 0, syscall.EINVAL
	}
	nr := len(files)
	reg := &RsrcRegister{
		Nr:   uint32(nr),
		Data: uint64(slicePtr(files)),
		Tags: uint64(slicePtr(tags)),
	}

	var (
		ret         uint
		err         error
		errno       syscall.Errno
		didIncrease bool
	)

	for {
		ret, errno = ring.doRegisterErrno(RegisterFiles2, unsafe.Pointer(reg), uint32(unsafe.Sizeof(*reg)))
		if errno == 0 {
			runtime.KeepAlive(files)
			runtime.KeepAlive(tags)
			return ret, nil
		}
		if errno == syscall.EMFILE && !didIncrease {
			didIncrease = true
			err = increaseRlimitNofile(uint64(nr))
			if err != nil {
				break
			}

			continue
		}

		runtime.KeepAlive(files)
		runtime.KeepAlive(tags)
		return ret, os.NewSyscallError("io_uring_register", errno)
	}
	runtime.KeepAlive(files)
	runtime.KeepAlive(tags)
	return ret, err
}

// liburing: io_uring_register_files - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_files.3.en.html
func (ring *Ring) RegisterFiles(files []int32) (uint, error) {
	var (
		ret         uint
		err         error
		errno       syscall.Errno
		didIncrease bool
	)

	for {
		ret, errno = ring.doRegisterErrno(RegisterFiles, unsafe.Pointer(unsafe.SliceData(files)), uint32(len(files)))
		if errno == 0 {
			runtime.KeepAlive(files)
			return ret, nil
		}
		if errno == syscall.EMFILE && !didIncrease {
			didIncrease = true
			err = increaseRlimitNofile(uint64(len(files)))
			if err != nil {
				break
			}

			continue
		}

		runtime.KeepAlive(files)
		return ret, os.NewSyscallError("io_uring_register", errno)
	}
	runtime.KeepAlive(files)
	return ret, err
}

// liburing: io_uring_unregister_files - https://manpages.debian.org/unstable/liburing-dev/io_uring_unregister_files.3.en.html
func (ring *Ring) UnregisterFiles() (uint, error) {
	return ring.doRegister(UnregisterFiles, unsafe.Pointer(nil), 0)
}

// liburing: io_uring_register_eventfd - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_eventfd.3.en.html
func (ring *Ring) RegisterEventFd(fd int) (uint, error) {
	return ring.doRegister(RegisterEventFD, unsafe.Pointer(&fd), 1)
}

// liburing: io_uring_unregister_eventfd - https://manpages.debian.org/unstable/liburing-dev/io_uring_unregister_eventfd.3.en.html
func (ring *Ring) UnregisterEventFd() (uint, error) {
	return ring.doRegister(UnregisterEventFD, nil, 0)
}

// liburing: io_uring_register_eventfd_async - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_eventfd_async.3.en.html
func (ring *Ring) RegisterEventFdAsync(fd int) (uint, error) {
	return ring.doRegister(RegisterEventFDAsync, unsafe.Pointer(&fd), 1)
}

// liburing: io_uring_register_probe
func (ring *Ring) RegisterProbe(probe *Probe, nrOps int) (uint, error) {
	result, err := ring.doRegister(RegisterProbe, unsafe.Pointer(probe), uint32(nrOps))
	runtime.KeepAlive(probe)

	return result, err
}

// liburing: io_uring_register_personality
func (ring *Ring) RegisterPersonality() (uint, error) {
	return ring.doRegister(RegisterPersonality, unsafe.Pointer(nil), 0)
}

// liburing: io_uring_unregister_personality
func (ring *Ring) UnregisterPersonality(id int) (uint, error) {
	return ring.doRegister(UnregisterPersonality, unsafe.Pointer(nil), uint32(id))
}

// liburing: io_uring_register_restrictions
func (ring *Ring) RegisterRestrictions(res []Restriction) (uint, error) {
	result, err := ring.doRegister(RegisterRestrictions, unsafe.Pointer(unsafe.SliceData(res)), uint32(len(res)))
	runtime.KeepAlive(res)
	return result, err
}

// liburing: io_uring_enable_rings
func (ring *Ring) EnableRings() (uint, error) {
	return ring.doRegister(RegisterEnableRings, unsafe.Pointer(nil), 0)
}

// liburing: io_uring_register_iowq_aff - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_iowq_aff.3.en.html
func (ring *Ring) RegisterIOWQAff(cpusz uint64, mask *unix.CPUSet) error {
	if cpusz >= 1<<31 {
		return syscall.EINVAL
	}
	_, err := ring.doRegister(RegisterIOWQAff, unsafe.Pointer(mask), uint32(cpusz))

	runtime.KeepAlive(mask)

	return err
}

// liburing: io_uring_unregister_iowq_aff - https://manpages.debian.org/unstable/liburing-dev/io_uring_unregister_iowq_aff.3.en.html
func (ring *Ring) UnregisterIOWQAff() (uint, error) {
	return ring.doRegister(UnregisterIOWQAff, unsafe.Pointer(nil), 0)
}

const iowqMaxWorkersNrArgs = 2

// liburing: io_uring_register_iowq_max_workers - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_iowq_max_workers.3.en.html
func (ring *Ring) RegisterIOWQMaxWorkers(val []uint32) (uint, error) {
	if len(val) != iowqMaxWorkersNrArgs {
		return 0, syscall.EINVAL
	}
	result, err := ring.doRegister(RegisterIOWQMaxWorkers, unsafe.Pointer(unsafe.SliceData(val)), iowqMaxWorkersNrArgs)
	runtime.KeepAlive(val)
	return result, err
}

// liburing: io_uring_register_ring_fd - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_ring_fd.3.en.html
func (ring *Ring) RegisterRingFd() (uint, error) {
	if (ring.intFlags & IntFlagRegRing) != 0 {
		return 0, syscall.EEXIST
	}

	rsrcUpdate := &RsrcUpdate{
		Data:   uint64(ring.ringFd),
		Offset: registerRingFdOffset,
	}

	ret, err := ring.doRegister(RegisterRingFDs, unsafe.Pointer(rsrcUpdate), 1)
	if err != nil {
		return ret, err
	}

	if ret == 1 {
		ring.enterRingFd = int(rsrcUpdate.Offset)
		ring.intFlags |= IntFlagRegRing

		if ring.features&FeatRegRegRing != 0 {
			ring.intFlags |= IntFlagRegRegRing
		}
	} else {
		return ret, fmt.Errorf("unexpected return from ring.Register: %d", ret)
	}

	return ret, nil
}

// liburing: io_uring_unregister_ring_fd - https://manpages.debian.org/unstable/liburing-dev/io_uring_unregister_ring_fd.3.en.html
func (ring *Ring) UnregisterRingFd() (uint, error) {
	rsrcUpdate := &RsrcUpdate{
		Offset: uint32(ring.enterRingFd),
	}

	if (ring.intFlags & IntFlagRegRing) == 0 {
		return 0, syscall.EINVAL
	}

	ret, err := ring.doRegister(UnregisterRingFDs, unsafe.Pointer(rsrcUpdate), 1)
	if err != nil {
		return ret, err
	}

	if ret == 1 {
		ring.enterRingFd = ring.ringFd
		ring.intFlags &= ^(IntFlagRegRing | IntFlagRegRegRing)
	}

	return ret, nil
}

// liburing: io_uring_close_ring_fd - https://manpages.debian.org/unstable/liburing-dev/io_uring_close_ring_fd.3.en.html
func (ring *Ring) CloseRingFd() (uint, error) {
	if ring.features&FeatRegRegRing == 0 {
		return 0, syscall.EOPNOTSUPP
	}

	if (ring.intFlags & IntFlagRegRing) == 0 {
		return 0, syscall.EINVAL
	}

	if ring.ringFd == -1 {
		return 0, syscall.EBADF
	}

	syscall.Close(ring.ringFd)
	ring.ringFd = -1

	return 1, nil
}

// RegisterBufferRing retains reg through the registration syscall. The caller
// must retain the described buffer ring until it is unregistered or QueueExit.
// liburing: io_uring_register_buf_ring - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_buf_ring.3.en.html
func (ring *Ring) RegisterBufferRing(reg *BufReg, flags uint32) (uint, error) {
	reg.Flags |= uint16(flags)
	result, err := ring.doRegister(RegisterPbufRing, unsafe.Pointer(reg), 1)
	runtime.KeepAlive(reg)

	return result, err
}

// liburing: io_uring_unregister_buf_ring - https://manpages.debian.org/unstable/liburing-dev/io_uring_unregister_buf_ring.3.en.html
func (ring *Ring) UnregisterBufferRing(bgid int) (uint, error) {
	reg := &BufReg{
		Bgid: uint16(bgid),
	}
	result, err := ring.doRegister(UnregisterPbufRing, unsafe.Pointer(reg), 1)
	runtime.KeepAlive(reg)

	return result, err
}

// liburing: io_uring_register_sync_cancel - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_sync_cancel.3.en.html
func (ring *Ring) RegisterSyncCancel(reg *SyncCancelReg) (uint, error) {
	return ring.doRegister(RegisterSyncCancel, unsafe.Pointer(reg), 1)
}

// liburing: io_uring_register_file_alloc_range - https://manpages.debian.org/unstable/liburing-dev/io_uring_register_file_alloc_range.3.en.html
func (ring *Ring) RegisterFileAllocRange(off, length uint32) (uint, error) {
	fileRange := &FileIndexRange{
		Off: off,
		Len: length,
	}

	result, err := ring.doRegister(RegisterFileAllocRange, unsafe.Pointer(fileRange), 0)
	runtime.KeepAlive(fileRange)

	return result, err
}
