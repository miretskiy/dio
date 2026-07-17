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
	"os"
	"syscall"
	"testing"
	"time"

	. "github.com/stretchr/testify/require"
)

func TestRegisterRingFd(t *testing.T) {
	var maxSQEs uint32 = 1024
	ring, err := CreateRing(maxSQEs)
	Nil(t, err)

	_, err = ring.RegisterRingFd()
	Nil(t, err)

	defer ring.QueueExit()

	var (
		entry *SubmissionQueueEntry
		cqe   *CompletionQueueEvent
	)

	for i := 0; i < 1000; i++ {
		entry = ring.GetSQE()
		NotNil(t, entry)

		entry.PrepareNop()
		entry.UserData = uint64(i)

		timespec := syscall.NsecToTimespec((time.Millisecond).Nanoseconds())
		cqe, err = ring.SubmitAndWaitTimeout(1, &timespec, nil)
		Nil(t, err)
		NotNil(t, cqe)

		ring.CQESeen(cqe)
	}

	_, err = ring.UnregisterRingFd()
	NoError(t, err)
}

func TestRegisterFilesUsesKernelDescriptorWidth(t *testing.T) {
	ring, err := CreateRing(8)
	NoError(t, err)
	defer ring.QueueExit()

	_, err = ring.RegisterFiles([]int32{-1, -1})
	NoError(t, err)
	_, err = ring.RegisterFilesUpdate(1, []int32{-1})
	NoError(t, err)
	_, err = ring.UnregisterFiles()
	NoError(t, err)
}

func TestRegisterFilesTagsAndUpdate(t *testing.T) {
	ring, err := CreateRing(8)
	NoError(t, err)
	defer ring.QueueExit()

	f, err := os.Open("/dev/null")
	NoError(t, err)
	defer f.Close()
	files := []int32{int32(f.Fd())}

	_, err = ring.RegisterFilesTags(files, []uint64{1})
	NoError(t, err)
	_, err = ring.RegisterFilesUpdateTag(0, files, []uint64{2})
	NoError(t, err)
	_, err = ring.UnregisterFiles()
	NoError(t, err)
}

func TestRegisterPersonalityRoundTrip(t *testing.T) {
	ring, err := CreateRing(8)
	NoError(t, err)
	defer ring.QueueExit()

	id, err := ring.RegisterPersonality()
	NoError(t, err)
	_, err = ring.UnregisterPersonality(int(id))
	NoError(t, err)
}

func TestRegisterIOWQMaxWorkersRequiresTwoValues(t *testing.T) {
	ring := &Ring{}

	_, err := ring.RegisterIOWQMaxWorkers(nil)
	ErrorIs(t, err, syscall.EINVAL)
	_, err = ring.RegisterIOWQMaxWorkers([]uint32{1})
	ErrorIs(t, err, syscall.EINVAL)
	_, err = ring.RegisterIOWQMaxWorkers([]uint32{1, 2, 3})
	ErrorIs(t, err, syscall.EINVAL)
}
