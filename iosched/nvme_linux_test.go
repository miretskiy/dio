package iosched

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"
	"strconv"
	"syscall"
	"testing"
	"unsafe"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/nvme"
	"github.com/miretskiy/dio/v2/ringo"
	"github.com/stretchr/testify/require"
)

func TestNVMeSubmissionSemantics(t *testing.T) {
	var device nvme.Device
	buf := make([]byte, 4096)
	read := NVMeReadOp(&device, buf, 0)
	s := &URingScheduler{config: makeSchedulerConfig(nil)}
	_, err := s.prepareSubmission(&read)
	require.ErrorContains(t, err, "WithNVMeCommands")
	s.config.nvmeCommands = true
	_, err = s.prepareSubmission(&read)
	require.ErrorContains(t, err, "open device")

	posix := NewPOSIXScheduler()
	defer posix.Close()
	_, err = posix.Submit(read)
	require.ErrorIs(t, err, syscall.EOPNOTSUPP)

	// FUA stays on each write; the chain contains exactly the two requested ops.
	write := NVMeWriteOp(&device, buf, 0).Durable()
	chain := write.HardLink(write)
	work, err := copySubmission(&chain, maxChainLength)
	require.NoError(t, err)
	require.Equal(t, 2, work.opCount())
	require.Nil(t, work.durable())
	require.False(t, write.operation.coalescibleWrite())
	plainWrite := NVMeWriteOp(&device, buf, 0)
	require.False(t, plainWrite.operation.coalescibleWrite())

	n, err := nvmeResult(&write.operation, ringo.Completion{})
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.NoError(t, writeResultError(&write.operation, n, err))
	n, err = nvmeResult(&write.operation, ringo.Completion{Result: 0x281})
	require.Zero(t, n)
	require.ErrorIs(t, err, nvme.StatusError(0x281))
	_, err = nvmeResult(&read.operation, ringo.Completion{Err: syscall.EACCES})
	require.ErrorIs(t, err, syscall.EACCES)
}

// TestNVMeHardware validates the scheduled path; writes require the same explicit
// dedicated-range environment variables as nvme.TestNamespaceHardware.
func TestNVMeHardware(t *testing.T) {
	path := os.Getenv("DIO_NVME_DEVICE")
	if path == "" {
		t.Skip("set DIO_NVME_DEVICE to a namespace character device")
	}
	t.Logf("process uid=%d euid=%d", os.Getuid(), os.Geteuid())
	if os.Getenv("DIO_NVME_REQUIRE_NONROOT") == "1" {
		require.NotZero(t, os.Getuid())
		require.NotZero(t, os.Geteuid())
	}
	status, err := os.ReadFile("/proc/self/status")
	require.NoError(t, err)
	for line := range bytes.SplitSeq(status, []byte{'\n'}) {
		if bytes.HasPrefix(line, []byte("Cap")) {
			t.Log(string(line))
		}
	}
	device, err := nvme.Open(path)
	require.NoError(t, err)
	defer device.Close()
	info := device.Info()
	t.Logf("device: %+v", info)
	require.NotZero(t, info.QueueDepth)
	bounded, err := NewURingScheduler(WithRingDepth(4*info.QueueDepth), WithNVMeCommands(device))
	require.NoError(t, err)
	require.Equal(t, max(info.QueueDepth, minRingDepth), bounded.config.ringDepth, "slots fit one hardware queue")
	require.NoError(t, bounded.Close())
	scheduler, err := NewURingScheduler(WithRingDepth(16), WithNVMeCommands())
	require.NoError(t, err)
	defer func() { require.NoError(t, scheduler.Close()) }()
	run := func(op Op, want int) {
		ticket, err := scheduler.Submit(op)
		require.NoError(t, err)
		n, err := ticket.Wait()
		require.NoError(t, err)
		require.Equal(t, want, n)
	}
	buf := make([]byte, info.BlockSize)
	run(NVMeReadOp(device, buf, 0), len(buf))
	_, err = scheduler.Submit(NVMeReadOp(device, buf, 1))
	require.Error(t, err)
	_, err = scheduler.Submit(NVMeReadOp(device, buf, int64(device.Size())))
	require.Error(t, err)
	for _, hard := range []bool{false, true} {
		first := NVMeReadOp(device, buf, 0)
		next := NVMeReadOp(device, buf, 0)
		chain := first.Link(next)
		if hard {
			chain = first.HardLink(next)
		}
		linked, err := scheduler.Submit(chain)
		require.NoError(t, err)
		_, err = linked.Wait()
		require.NoError(t, err)
		for _, result := range linked.AllResults() {
			require.NoError(t, result.Err)
			require.Equal(t, len(buf), result.N)
		}
	}
	run(NVMeReadOp(device, nil, int64(device.Size())), 0)
	// Raw namespace I/O retains the driver's zero-on-success result.
	command, err := device.ReadCommand(buf, 0)
	require.NoError(t, err)
	request, err := command.Op(ringo.FileFD(device.File()))
	require.NoError(t, err)
	var raw ringo.Completion
	run(RawNVMeCmdOp(device.File(), request, &raw), 0)
	require.NoError(t, nvme.Status(raw))

	// A raw device error keeps its positive result and native soft-link behavior.
	invalid := nvme.Command{Opcode: nvme.OpcodeRead, NSID: info.NSID, Data: buf, CDW10: uint32(info.Blocks), CDW11: uint32(info.Blocks >> 32)}
	request, err = invalid.Op(ringo.FileFD(device.File()))
	require.NoError(t, err)
	ticket, err := scheduler.Submit(RawNVMeCmdOp(device.File(), request, &raw).Link(NVMeReadOp(device, buf, 0)))
	require.NoError(t, err)
	n, err := ticket.Wait()
	require.NoError(t, err)
	require.Positive(t, n)
	require.Error(t, nvme.Status(raw))
	require.NoError(t, ticket.AllResults()[1].Err)
	require.Equal(t, len(buf), ticket.AllResults()[1].N)

	if os.Getenv("DIO_NVME_STATS") == "1" {
		// AWS publishes this read-only log on the controller's Admin interface.
		require.Equal(t, "Amazon EC2 NVMe Instance Storage", info.Model)
		controller, err := os.Open(os.Getenv("DIO_NVME_CONTROLLER"))
		require.NoError(t, err)
		defer controller.Close()
		stats := make([]byte, 4096)
		command := nvme.Command{Opcode: nvme.OpcodeAdminGetLogPage, NSID: info.NSID, Data: stats, CDW10: ((4096/4 - 1) << 16) | 0xd0}
		request, err := command.AdminOp(ringo.FileFD(controller))
		require.NoError(t, err)
		run(RawNVMeCmdOp(controller, request, &raw), 0)
		require.NoError(t, nvme.Status(raw))
		require.Equal(t, uint32(0xec2c0d7e), binary.LittleEndian.Uint32(stats))
		t.Logf("AWS Admin stats: magic=%#x, version=%d, read ops=%d, write ops=%d", binary.LittleEndian.Uint32(stats), binary.LittleEndian.Uint32(stats[4:]), binary.LittleEndian.Uint64(stats[8:]), binary.LittleEndian.Uint64(stats[16:]))
	}

	// File byte counts and fdatasync keep their normal semantics on this ring.
	file, err := os.CreateTemp(t.TempDir(), "mixed-ring")
	require.NoError(t, err)
	defer file.Close()
	contents := []byte("ordinary file I/O")
	run(WriteOp(file, contents, 0).Durable(), len(contents))
	got := make([]byte, len(contents))
	run(ReadOp(file, got, 0), len(got))
	require.Equal(t, contents, got)
	rawBuffer := make([]byte, len(contents))
	request = ringo.Read(ringo.FileFD(file), rawBuffer, 0)
	run(NVMeReadOp(device, make([]byte, int(info.MaxTransferBytes)*40), 0), int(info.MaxTransferBytes)*40)
	run(RawNVMeCmdOp(file, request, &raw), len(contents))
	require.Equal(t, contents, rawBuffer)
	require.Equal(t, len(contents), raw.Result)

	if os.Getenv("DIO_NVME_WRITE") != "1" {
		return
	}
	offset, err := strconv.ParseUint(os.Getenv("DIO_NVME_OFFSET"), 10, 64)
	require.NoError(t, err)
	size, err := strconv.ParseUint(os.Getenv("DIO_NVME_SIZE"), 10, 64)
	require.NoError(t, err)
	scratch, err := nvme.Open(path, nvme.WithRange(offset, size))
	require.NoError(t, err)
	defer scratch.Close()
	require.GreaterOrEqual(t, size, uint64(info.MaxTransferBytes)*40)
	for _, length := range []int{int(info.BlockSize), min(4096, int(info.MaxTransferBytes)), int(info.MaxTransferBytes), int(info.MaxTransferBytes)*40 + int(info.BlockSize)} {
		t.Logf("write/read %d bytes, command limit %d, ring depth %d", length, info.MaxTransferBytes, scheduler.config.ringDepth)
		data := bytes.Repeat([]byte{byte(length ^ 0xa5)}, length)
		run(NVMeWriteOp(scratch, data, 0).Durable(), length)
		readback := make([]byte, length)
		run(NVMeReadOp(scratch, readback, 0), length)
		require.Equal(t, data, readback)
	}
	// Fragment allocator buffers into more pieces than a logical chain holds.
	memory := alloc.NewAllocator(16 << 20)
	defer memory.Close()
	var blockers []alloc.Memory
	for {
		block, err := memory.TryAcquire(4096)
		if err != nil {
			require.ErrorIs(t, err, alloc.ErrExhausted)
			break
		}
		blockers = append(blockers, block)
	}
	sort.Slice(blockers, func(i, j int) bool {
		return uintptr(unsafe.Pointer(&blockers[i].Bytes()[0])) < uintptr(unsafe.Pointer(&blockers[j].Bytes()[0]))
	})
	for i := 0; i < len(blockers); i += 2 {
		blockers[i].Unpin()
	}
	defer func() {
		for i := 1; i < len(blockers); i += 2 {
			blockers[i].Unpin()
		}
	}()
	fragmented := func(data []byte) *alloc.Buffer {
		buffer := alloc.NewAllocatingBuffer(memory)
		_, err := buffer.Write(data)
		require.NoError(t, err)
		if len(buffer.RawAllocations()) <= maxChainLength {
			buffer.Release()
			t.Fatal("expected a fragmented buffer")
		}
		return buffer
	}
	pattern := make([]byte, 20*65536+int(info.BlockSize))
	for i := range pattern {
		pattern[i] = byte(i/4096) ^ byte(i) ^ 0xa7
	}
	writeBuffer := fragmented(pattern)
	defer writeBuffer.Release()
	run(NVMeWriteBufferOp(scratch, writeBuffer, len(pattern), 0).Durable(), len(pattern))
	readBuffer := fragmented(make([]byte, len(pattern)))
	defer readBuffer.Release()
	run(NVMeReadBufferOp(scratch, readBuffer, len(pattern), 0), len(pattern))
	gotPattern := make([]byte, 0, len(pattern))
	for _, part := range readBuffer.RawAllocations() {
		gotPattern = append(gotPattern, part.Bytes()[:min(len(pattern)-len(gotPattern), len(part.Bytes()))]...)
	}
	require.Equal(t, pattern, gotPattern)
	last := int64(size - uint64(len(buf)))
	data := bytes.Repeat([]byte{0x5a}, len(buf))
	run(NVMeWriteOp(scratch, data, last), len(data))
	run(NVMeReadOp(scratch, buf, last), len(buf))
	require.Equal(t, data, buf)

	const count = 8
	length := min(4096, int(info.MaxTransferBytes))
	for batch := 0; batch < 4; batch++ {
		var expected, actual [count][]byte
		for i := range expected {
			expected[i] = bytes.Repeat([]byte{byte(batch*31 + i)}, length)
			actual[i] = make([]byte, length)
		}
		for _, writing := range []bool{true, false} {
			var tickets [count]Ticket
			for i := range count {
				op := NVMeReadOp(scratch, actual[i], int64(i*length))
				if writing {
					op = NVMeWriteOp(scratch, expected[i], int64(i*length)).Durable()
				}
				tickets[i], err = scheduler.Submit(op)
				require.NoError(t, err)
			}
			for _, ticket := range tickets {
				n, err := ticket.Wait()
				require.NoError(t, err)
				require.Equal(t, length, n)
			}
		}
		require.Equal(t, expected, actual)
	}
	// A transfer of many ring-depths of commands keeps one result. It needs
	// several commands, so it cannot be linked.
	large := bytes.Repeat([]byte{0xe7}, int(info.MaxTransferBytes)*40+int(info.BlockSize))
	gotLarge := make([]byte, len(large))
	run(NVMeWriteOp(scratch, large, 0).Durable(), len(large))
	run(NVMeReadOp(scratch, gotLarge, 0), len(large))
	require.Equal(t, large, gotLarge)
	_, err = scheduler.Submit(WriteOp(file, contents, 0).Link(NVMeReadOp(scratch, gotLarge, 0)))
	require.ErrorContains(t, err, "cannot be linked")

	// Draining this handle waits for every command of each transfer.
	var reads [4]Ticket
	for i := range reads {
		reads[i], err = scheduler.Submit(NVMeReadOp(scratch, make([]byte, len(large)), 0))
		require.NoError(t, err)
	}
	drain, err := scheduler.Submit(DrainOp(scratch.File()))
	require.NoError(t, err)
	_, err = drain.Wait()
	require.NoError(t, err)
	for _, read := range reads {
		n, err := read.Wait()
		require.NoError(t, err)
		require.Equal(t, len(large), n)
	}
	require.Equal(t, DiskStats{}, scheduler.config.disk.Stats(), "all dispatch charges are returned once")
	_, err = scheduler.Submit(NVMeWriteOp(scratch, large, int64(size)-int64(info.BlockSize)))
	require.Error(t, err, "validate the entire logical request before its first command")
	run(NVMeFlushOp(scratch), 0)
}

// Feed out-of-order completions without relying on the device to produce a
// particular error. Four of six commands are outstanding; two wait queued.
func TestNVMeSplitCompletions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure ringo.Completion
		wantErr error
	}{
		{"status", ringo.Completion{Result: 0x280}, nvme.StatusError(0x280)},
		{"linux-error", ringo.Completion{Err: syscall.EIO}, syscall.EIO},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := NVMeReadOp(nil, make([]byte, 4096*6), 0)
			work, err := copySubmission(&op, maxChainLength)
			require.NoError(t, err)
			work.state = workIssued
			work.root.advance(4096 * 4)
			work.sent, work.pending = 4096*4, 4
			c := &coordinator{sched: &URingScheduler{config: makeSchedulerConfig(nil)}}
			c.accepted.push(work)
			c.ready.push(work)
			for _, i := range []int{3, 0, 2, 1} {
				completion := ringo.Completion{}
				if i == 3 {
					completion.Result = 0x281
				}
				if i == 1 {
					completion = tc.failure
				}
				c.finishNVMePart(ringSlot{work: work, op: &work.root, partOffset: i * 4096}, completion)
				require.Zero(t, c.ready.len, "the first failure drops the unplaced commands")
			}
			n, err := (Ticket{&work.completion}).Wait()
			require.Equal(t, 4096, n, "the result is the prefix before the earliest failure")
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
	op := NVMeReadOp(nil, make([]byte, 4096*2), 0)
	work, err := copySubmission(&op, maxChainLength)
	require.NoError(t, err)
	work.state = workIssued
	work.root.advance(4096 * 2)
	work.sent, work.pending = 4096*2, 2
	c := &coordinator{sched: &URingScheduler{config: makeSchedulerConfig(nil)}}
	c.accepted.push(work)
	for _, i := range []int{1, 0} {
		c.finishNVMePart(ringSlot{work: work, op: &work.root, partOffset: i * 4096}, ringo.Completion{})
	}
	n, err := (Ticket{&work.completion}).Wait()
	require.NoError(t, err)
	require.Equal(t, 4096*2, n)
}

func TestRawNVMeCmdOpFileChainAndDrain(t *testing.T) {
	c := newTestCoordinator(t, 8, 0)
	file := testFile(t, 4096)
	data := []byte("raw positioned I/O")
	got := make([]byte, len(data))
	var written, read ringo.Completion
	chain := RawNVMeCmdOp(file, ringo.Write(ringo.FileFD(file), data, 0), &written).
		Link(RawNVMeCmdOp(file, ringo.Read(ringo.FileFD(file), got, 0), &read))
	tickets, _ := acceptOps(c, chain, DrainOp(file))
	for c.accepted.len != 0 {
		c.placeReady(false)
		require.NoError(t, c.enter(1))
		c.reap()
	}
	for _, ticket := range tickets {
		_, err := ticket.Wait()
		require.NoError(t, err)
	}
	require.Equal(t, data, got)
	require.Equal(t, len(data), written.Result)
	require.Equal(t, len(data), read.Result)
	require.Equal(t, []Result{{N: len(data)}, {N: len(data)}}, tickets[0].AllResults())
}

func TestRawNVMeCmdOpCompletionAndFailure(t *testing.T) {
	c := newTestCoordinator(t, 8, 0)
	var result ringo.Completion
	tickets, works := acceptOps(c, RawNVMeCmdOp(nil, ringo.Nop(), &result))
	c.placeReady(false)
	require.NoError(t, c.enter(1))
	for completion := range c.ring.Reap() {
		intermediate := completion
		intermediate.Flags |= ringo.CompletionFlags(1 << 1) // IORING_CQE_F_MORE
		c.reapOne(intermediate)
		require.Equal(t, workIssued, works[0].state)
		require.Equal(t, 1, c.occupied)
		completion.Result = 0x281
		completion.Extra = [2]uint64{0x123456789abcdef0, 0xfedcba9876543210}
		c.reapOne(completion)
		require.Equal(t, completion, result)
	}
	n, err := tickets[0].Wait()
	require.NoError(t, err)
	require.Equal(t, 0x281, n, "positive driver statuses are preserved")

	for _, hard := range []bool{false, true} {
		var first, next ringo.Completion
		bad := RawNVMeCmdOp(nil, ringo.Read(ringo.BorrowedFD(-1), make([]byte, 1), 0), &first)
		follower := RawNVMeCmdOp(nil, ringo.Nop(), &next)
		chain := bad.Link(follower)
		if hard {
			chain = bad.HardLink(follower)
		}
		tickets, _ := acceptOps(c, chain)
		c.placeReady(false)
		submitAndReapUntil(t, c, func() bool { return c.accepted.len == 0 })
		_, err := tickets[0].Wait()
		require.ErrorIs(t, err, syscall.EBADF)
		require.ErrorIs(t, first.Err, syscall.EBADF)
		if hard {
			require.NoError(t, next.Err)
		} else {
			require.ErrorIs(t, next.Err, syscall.ECANCELED)
		}
	}
}

func TestRawNVMeCmdOpSubmissionAndUnplacedFailure(t *testing.T) {
	posix := NewPOSIXScheduler()
	defer posix.Close()
	var result ringo.Completion
	var pool ringo.OpAlloc
	request := ringo.Read(ringo.BorrowedFD(-1), make([]byte, 1), 0, ringo.WithAlloc(&pool))
	op := RawNVMeCmdOp(nil, request, &result)
	_, err := posix.Submit(op)
	require.ErrorIs(t, err, syscall.EOPNOTSUPP)
	require.Same(t, request, op.raw.request, "Submit rejection retains caller ownership")

	work, err := copySubmission(&op, maxChainLength)
	require.NoError(t, err)
	work.fail(errSchedulerClosed)
	_, err = (Ticket{&work.completion}).Wait()
	require.ErrorIs(t, err, errSchedulerClosed)
	require.ErrorIs(t, result.Err, errSchedulerClosed)
	require.Nil(t, op.raw.request)
	recycled := ringo.Read(ringo.BorrowedFD(-1), nil, 0, ringo.WithAlloc(&pool))
	require.Same(t, request, recycled, "cancellation returns the unsubmitted request to its pool")
	ringo.Discard(recycled)

	c := newTestCoordinator(t, 8, 0)
	invalid := ringo.UringCmd(ringo.BorrowedFD(-1), 0, make([]byte, 81), nil, ringo.WithAlloc(&pool))
	tickets, _ := acceptOps(c, RawNVMeCmdOp(nil, invalid, &result))
	c.placeReady(false) // Ringo rejects the oversized inline payload before Push.
	_, err = tickets[0].Wait()
	require.ErrorContains(t, err, "command exceeds 80 inline bytes")
	require.Equal(t, err, result.Err)
	reused := ringo.UringCmd(ringo.BorrowedFD(-1), 0, nil, nil, ringo.WithAlloc(&pool))
	require.Same(t, invalid, reused, "Push rejection returns the unsubmitted request to its pool")
	ringo.Discard(reused)
}

// Commands whose data lies in a registered buffer use the registered-buffer
// path; data outside every registration still uses ordinary addresses. Entries
// are 128 KiB: before Linux 6.15, io_import_fixed counts every page to the end
// of the entry, and the block layer rejects more pages than the queue's
// segment limit (33 on EC2 instance storage) with EINVAL.
func TestNVMeRegisteredBuffers(t *testing.T) {
	path := os.Getenv("DIO_NVME_DEVICE")
	if path == "" || os.Getenv("DIO_NVME_WRITE") != "1" {
		t.Skip("set DIO_NVME_DEVICE, DIO_NVME_WRITE=1, DIO_NVME_OFFSET, and DIO_NVME_SIZE")
	}
	offset, err := strconv.ParseUint(os.Getenv("DIO_NVME_OFFSET"), 10, 64)
	require.NoError(t, err)
	scratch, err := nvme.Open(path, nvme.WithRange(offset, 8<<20))
	require.NoError(t, err)
	defer scratch.Close()
	region := align.AllocAligned(2 << 20)
	defer align.FreeAligned(region)
	var entries [][]byte
	for e := 0; e < len(region); e += 128 << 10 {
		entries = append(entries, region[e:e+128<<10])
	}
	scheduler, err := NewURingScheduler(WithRingDepth(16), WithNVMeCommands(scratch), WithDMABuffers(entries...))
	require.NoError(t, err)
	defer func() { require.NoError(t, scheduler.Close()) }()
	run := func(op Op) {
		t.Helper()
		ticket, err := scheduler.Submit(op)
		require.NoError(t, err)
		_, err = ticket.Wait()
		require.NoError(t, err)
	}
	// 1 MiB is eight commands, one per entry; 64 KiB covers part of an entry.
	written, readback := region[:1<<20], region[1<<20:]
	for i := range written {
		written[i] = byte(i*7 + i>>12)
	}
	run(NVMeWriteOp(scratch, written, 0))
	run(NVMeReadOp(scratch, readback, 0))
	require.Equal(t, written, readback)
	run(NVMeReadOp(scratch, readback[64<<10:128<<10], 0))
	require.Equal(t, written[:64<<10], readback[64<<10:128<<10])

	outside := align.AllocAligned(1 << 20)
	defer align.FreeAligned(outside)
	run(NVMeReadOp(scratch, outside, 0))
	require.Equal(t, written, outside, "unregistered data uses ordinary addresses")

	// One large entry: before 6.15 its commands keep ordinary addresses rather
	// than failing.
	large, err := NewURingScheduler(WithRingDepth(16), WithNVMeCommands(scratch), WithDMABuffers(region))
	require.NoError(t, err)
	defer func() { require.NoError(t, large.Close()) }()
	clear(readback)
	ticket, err := large.Submit(NVMeReadOp(scratch, readback, 0))
	require.NoError(t, err)
	_, err = ticket.Wait()
	require.NoError(t, err)
	require.Equal(t, written, readback)
}
