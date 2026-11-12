//go:build linux

package iosched

import (
	"bytes"
	"encoding/binary"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"

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
	run(NVMeReadOp(device, make([]byte, int(info.MaxTransferBytes)*40), 0).Link(RawNVMeCmdOp(file, request, &raw)), int(info.MaxTransferBytes)*40)
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
	// A multi-batch logical transfer preserves chain order and ordinary file I/O.
	large := bytes.Repeat([]byte{0xe7}, int(info.MaxTransferBytes)*40+int(info.BlockSize))
	gotLarge := make([]byte, len(large))
	for _, hard := range []bool{false, true} {
		first := NVMeWriteOp(scratch, large, 0).Durable()
		next := NVMeReadOp(scratch, gotLarge, 0)
		chain := first.Link(next).Link(WriteOp(file, contents, 0).Durable())
		if hard {
			chain = first.HardLink(next).HardLink(WriteOp(file, contents, 0).Durable())
		}
		ticket, err := scheduler.Submit(chain)
		require.NoError(t, err)
		n, err := ticket.Wait()
		require.NoError(t, err)
		require.Equal(t, len(large), n)
		require.Equal(t, large, gotLarge)
		require.Len(t, ticket.AllResults(), 3)
		for i, result := range ticket.AllResults() {
			require.NoError(t, result.Err)
			want := len(large)
			if i == 2 {
				want = len(contents)
			}
			require.Equal(t, want, result.N)
		}
	}

	// A non-final ordinary Durable write gets its synthesized fdatasync before
	// the split transfer. Draining this handle also waits for every chunk.
	run(WriteOp(file, contents, 0).Durable().Link(NVMeWriteOp(scratch, large, 0)).Link(DrainOp(scratch.File())), len(contents))
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

// A split transfer keeps its in-flight reservation between batches and returns
// it when shutdown stops placement, even though it is back on the ready queue.
func TestNVMeSplitShutdownAccounting(t *testing.T) {
	policy := NewDiskPolicy(WithReadBacklogLimit(time.Second), WithLatencyGoal(time.Second))
	op := ReadOp(nil, make([]byte, 4096), 0)
	work, err := copySubmission(&op, maxChainLength)
	require.NoError(t, err)
	charge := policy.device.reads.operationTime(&work.root)
	work.reads = ioCharge{backlog: charge, inFlight: charge}
	policy.account(work)
	c := &coordinator{sched: &URingScheduler{config: makeSchedulerConfig([]Option{WithDiskPolicy(policy)})}}
	require.True(t, policy.dispatch(work, &c.inFlight))
	work.state = workIssued
	work.nvmeSplit = &nvmeSplit{started: true}
	c.accepted.push(work)
	c.resumeNVMe(work)
	require.Positive(t, policy.Stats().Reads.InFlight)
	c.failRemaining(nil, errSchedulerClosed, errSchedulerClosed)
	require.Equal(t, DiskStats{}, policy.Stats())
	_, err = (Ticket{&work.completion}).Wait()
	require.ErrorIs(t, err, errSchedulerClosed)
}

// Feed out-of-order completions to exercise partial results and native link
// failure rules without relying on the device to produce a particular error.
func TestNVMeSplitCompletions(t *testing.T) {
	for _, tc := range []struct {
		name       string
		hard       bool
		linuxError bool
	}{
		{"positive-soft", false, false},
		{"negative-soft", false, true},
		{"negative-hard", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := NVMeReadOp(nil, make([]byte, 4096*6), 0)
			next := NVMeReadOp(nil, nil, 0)
			chain := first.Link(next)
			if tc.hard {
				chain = first.HardLink(next)
			}
			work, err := copySubmission(&chain, maxChainLength)
			require.NoError(t, err)
			work.state = workIssued
			work.nvmeSplit = &nvmeSplit{started: true, cursor: 4096 * 4, prefix: len(first.buf), pending: 4}
			c := &coordinator{sched: &URingScheduler{config: makeSchedulerConfig(nil)}}
			c.accepted.push(work)
			for _, i := range []int{3, 0, 2, 1} {
				completion := ringo.Completion{}
				if i == 3 {
					completion.Result = 0x281
				}
				if i == 1 {
					completion.Result = 0x280
					if tc.linuxError {
						completion.Err = syscall.EIO
					}
				}
				c.finishNVMePart(ringSlot{work: work, op: &work.root, partOffset: i * 4096, partLength: 4096}, completion)
			}
			require.Equal(t, 4096, work.n)
			if tc.linuxError {
				require.ErrorIs(t, work.err, syscall.EIO)
			} else {
				require.ErrorIs(t, work.err, nvme.StatusError(0x280))
			}
			if tc.linuxError && !tc.hard {
				require.Equal(t, workDone, work.state)
				require.ErrorIs(t, work.results[1].Err, syscall.ECANCELED)
				require.Zero(t, c.ready.len)
			} else {
				require.Equal(t, 1, c.ready.len)
				require.Equal(t, 0, work.dispatchClass())
				require.Equal(t, 1, work.nvmeSplit.index)
				c.take(work)
				work.nvmeSplit.pending = 1
				c.finishNVMePart(ringSlot{work: work, op: work.op(1)}, ringo.Completion{})
				require.Equal(t, workDone, work.state)
				require.NoError(t, work.results[1].Err)
			}
		})
	}
}
