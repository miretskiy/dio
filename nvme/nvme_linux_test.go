//go:build linux

package nvme

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/miretskiy/dio/v2/ringo"
	"github.com/stretchr/testify/require"
)

func TestIdentifyGeometry(t *testing.T) {
	ns, ctrl := make([]byte, 4096), make([]byte, 4096)
	binary.LittleEndian.PutUint64(ns, 0xdd30699c)
	ns[130] = 9
	copy(ctrl[24:64], "Amazon EC2 NVMe Instance Storage")
	ctrl[77] = 5
	info, err := parseIdentify(Info{NSID: 7}, ns, ctrl)
	require.NoError(t, err)
	require.Equal(t, uint64(1900000000000), info.SizeBytes)
	require.Equal(t, uint32(512), info.BlockSize)
	require.Equal(t, uint8(5), info.MDTS)
	require.Equal(t, uint32(7), info.NSID)
	for _, change := range []func([]byte){
		func(n []byte) { n[128] = 8 }, // Active metadata format, even with PI disabled.
		func(n []byte) { n[29] = 1 },
		func(n []byte) { n[26] = 1 }, // Format exceeds nlbaf.
		func(n []byte) { n[130] = 63 },
		func(n []byte) { binary.LittleEndian.PutUint64(n, math.MaxUint64) },
	} {
		bad := append([]byte(nil), ns...)
		change(bad)
		_, err := parseIdentify(Info{}, bad, ctrl)
		require.Error(t, err)
	}
	// Extended FLBAS bits must be decoded as in fio_nvme_get_info.
	ns[25] = 63
	ns[26] = 0x21
	ns[128+17*4+2] = 12
	info, err = parseIdentify(Info{}, ns, ctrl)
	require.NoError(t, err)
	require.Equal(t, uint32(4096), info.BlockSize)
}

func TestDeviceEncodingAndBounds(t *testing.T) {
	n := &Device{info: Info{NSID: 9, BlockSize: 512, SizeBytes: 8 << 40, MaxTransferBytes: 128 << 10}}
	n.offset, n.length = 4<<40, 16<<20
	require.NoError(t, n.validateRange())
	device := n
	cmd, err := device.command(true, make([]byte, 4096), 8192, true)
	require.NoError(t, err)
	require.Equal(t, uint32(16), cmd.CDW10)
	require.Equal(t, uint32(2), cmd.CDW11) // SLBA is above 32 bits.
	require.Equal(t, uint32(7)|ForceUnitAccess, cmd.CDW12)
	require.Equal(t, uint32(9), cmd.NSID)
	require.Equal(t, OpcodeWrite, cmd.Opcode)
	payload := cmd.payload()
	require.Equal(t, uint32(4096), binary.NativeEndian.Uint32(payload[36:]))
	require.Equal(t, uint32(7)|ForceUnitAccess, binary.NativeEndian.Uint32(payload[48:]))
	require.Equal(t, make([]byte, 16), payload[16:32]) // Pointer binding happens in Push.
	_, err = device.command(false, make([]byte, 512), device.Size()-512, false)
	require.NoError(t, err)
	for _, tt := range []struct {
		length int
		offset uint64
	}{
		{0, 0}, {511, 0}, {512, 1}, {512, device.Size()}, {1024, device.Size() - 512},
		{512, math.MaxUint64}, {129 << 10, 0},
	} {
		_, err := device.command(false, make([]byte, tt.length), tt.offset, false)
		require.Error(t, err)
	}
	for _, tt := range [][2]uint64{{0, 0}, {1, 512}, {0, 513}, {n.info.SizeBytes, 512}, {math.MaxUint64 - 511, 1024}} {
		bounded := *n
		bounded.offset, bounded.length = tt[0], tt[1]
		require.Error(t, bounded.validateRange())
	}
	_, err = (&Device{}).ReadCommand(make([]byte, 512), 0)
	require.Error(t, err)
	require.NoError(t, device.ValidateIO(0, uint64(n.info.MaxTransferBytes)*2))
	require.NoError(t, device.ValidateIO(device.Size(), 0))
	require.Error(t, device.ValidateIO(device.Size()+512, 0))
	// NLB's maximum is 65536 blocks, encoded as 65535, never wrapped to 0.
	n.info.MaxTransferBytes = math.MaxUint32
	n.offset, n.length = 0, n.info.SizeBytes
	require.NoError(t, n.validateRange())
	cmd, err = device.command(false, make([]byte, 65536*512), 0, false)
	require.NoError(t, err)
	require.Equal(t, uint32(65535), cmd.CDW12)
	_, err = device.command(false, make([]byte, 65537*512), 0, false)
	require.Error(t, err)
}

func TestStatus(t *testing.T) {
	require.NoError(t, Status(ringo.Completion{Extra: [2]uint64{123, 0}}))
	require.ErrorIs(t, Status(ringo.Completion{Err: syscall.EACCES}), syscall.EACCES)
	var status StatusError
	require.ErrorAs(t, Status(ringo.Completion{Result: 0x281}), &status)
	require.Equal(t, StatusError(0x281), status)
}

func TestTopologyDependencies(t *testing.T) {
	root := t.TempDir()
	disk := filepath.Join(root, "nvme37n9")
	require.NoError(t, os.MkdirAll(filepath.Join(disk, "slaves"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(disk, "size"), []byte("100000"), 0644))
	part := filepath.Join(disk, "nvme37n9p3")
	require.NoError(t, os.MkdirAll(part, 0755))
	for file, value := range map[string]string{"size": "1000", "start": "2048", "partition": "3"} {
		require.NoError(t, os.WriteFile(filepath.Join(part, file), []byte(value), 0644))
	}
	dm := filepath.Join(root, "dm-23")
	require.NoError(t, os.MkdirAll(filepath.Join(dm, "slaves"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dm, "size"), []byte("1000"), 0644))
	require.NoError(t, os.Symlink(part, filepath.Join(dm, "slaves", "nvme37n9p3")))
	tree, err := topologyAt(dm, map[string]bool{})
	require.NoError(t, err)
	require.Equal(t, "/dev/dm-23", tree.Path)
	require.Equal(t, uint64(2048*512), tree.Backing[0].StartBytes)
	require.Equal(t, uint64(100000*512), tree.Backing[0].Backing[0].SizeBytes)
	require.NoError(t, os.Symlink(dm, filepath.Join(disk, "slaves", "cycle")))
	_, err = topologyAt(dm, map[string]bool{})
	require.ErrorContains(t, err, "cyclic")
}

// TestNamespaceHardware reads only unless DIO_NVME_WRITE=1 is explicitly set.
// Write testing requires DIO_NVME_OFFSET and DIO_NVME_SIZE naming a dedicated
// physical range. Never use a mounted filesystem's free space as a raw range.
func TestNamespaceHardware(t *testing.T) {
	path := os.Getenv("DIO_NVME_DEVICE")
	if path == "" {
		t.Skip("set DIO_NVME_DEVICE to a namespace character device")
	}
	t.Logf("process uid=%d euid=%d", os.Getuid(), os.Geteuid())
	if os.Getenv("DIO_NVME_REQUIRE_NONROOT") == "1" {
		require.NotZero(t, os.Getuid(), "hardware test must run as a non-root user")
		require.NotZero(t, os.Geteuid(), "hardware test must not have effective root identity")
	}
	processStatus, err := os.ReadFile("/proc/self/status")
	require.NoError(t, err)
	for line := range strings.SplitSeq(string(processStatus), "\n") {
		if strings.HasPrefix(line, "Cap") || strings.HasPrefix(line, "NoNewPrivs:") {
			t.Log(line)
		}
	}
	n, err := Open(path)
	require.NoError(t, err)
	defer n.Close()
	t.Logf("%+v", n.Info())
	topo, err := DiscoverTopology(path)
	require.NoError(t, err)
	require.Equal(t, n.info.CharDevice, topo.CharDevice)
	require.Equal(t, n.info.SizeBytes, topo.SizeBytes)
	ring, err := ringo.New(ringo.WithDepth(8), ringo.WithUringCmd())
	require.NoError(t, err)
	defer ring.Close()
	prepare := func(cmd Command) ringo.Op {
		op, err := cmd.Op(ringo.FileFD(n.File()))
		require.NoError(t, err)
		return op
	}
	run := func(cmd Command) ringo.Completion {
		h, err := ring.Push(prepare(cmd))
		require.NoError(t, err)
		for {
			_, err := ring.SubmitAndWait(1)
			require.NoError(t, err)
			var result ringo.Completion
			found := false
			for c := range ring.Reap() {
				require.Equal(t, h, c.Handle)
				result = c
				found = true
			}
			if found {
				return result
			}
		}
	}
	buf := make([]byte, n.info.BlockSize)
	read, err := n.ReadCommand(buf, 0)
	require.NoError(t, err)
	require.NoError(t, Status(run(read)))
	block, err := os.Open(n.info.BlockDevice)
	require.NoError(t, err)
	defer block.Close()
	expected := make([]byte, len(buf))
	_, err = block.ReadAt(expected, 0)
	require.NoError(t, err)
	require.Equal(t, expected, buf, "character and block interfaces must address the same namespace")
	// A read beyond the namespace exposes a positive NVMe error status.
	invalid := Command{Opcode: OpcodeRead, NSID: n.info.NSID, Data: buf, CDW10: uint32(n.info.Blocks), CDW11: uint32(n.info.Blocks >> 32)}
	status := run(invalid)
	require.NoError(t, status.Err)
	require.Positive(t, status.Result)
	var nvmeStatus StatusError
	require.ErrorAs(t, Status(status), &nvmeStatus)
	// Both kernel link kinds preserve execution ordering after a device status
	// error. A soft link's cancellation predicate covers negative Linux errors.
	for _, link := range []struct {
		name string
		kind ringo.LinkType
	}{{"soft", ringo.LinkSoft}, {"hard", ringo.LinkHard}} {
		handles, err := ring.PushLinked(prepare(invalid), ringo.Then(link.kind, ringo.Nop()))
		require.NoError(t, err)
		var results [2]ringo.Completion
		remaining := 2
		for remaining != 0 {
			_, err := ring.SubmitAndWait(1)
			require.NoError(t, err)
			for completion := range ring.Reap() {
				switch completion.Handle {
				case handles[0]:
					results[0] = completion
				case handles[1]:
					results[1] = completion
				default:
					t.Fatal("unexpected linked completion")
				}
				remaining--
			}
		}
		require.Positive(t, results[0].Result)
		require.NoError(t, results[0].Err)
		require.Zero(t, results[1].Result)
		require.NoError(t, results[1].Err)
		t.Logf("%s link: NVMe status=%#x, successor result=%d (executed)", link.name, results[0].Result, results[1].Result)
	}
	if os.Getenv("DIO_NVME_WRITE") != "1" {
		return
	}
	offset, err := strconv.ParseUint(os.Getenv("DIO_NVME_OFFSET"), 10, 64)
	require.NoError(t, err)
	size, err := strconv.ParseUint(os.Getenv("DIO_NVME_SIZE"), 10, 64)
	require.NoError(t, err)
	scratch, err := Open(path, WithRange(offset, size))
	require.NoError(t, err)
	defer scratch.Close()
	require.GreaterOrEqual(t, scratch.Size(), uint64(n.info.MaxTransferBytes))
	// Exercise full transfers, FUA, and the last block of the bounded range.
	for _, length := range []int{int(n.info.BlockSize), min(max(4096, int(n.info.BlockSize)), int(n.info.MaxTransferBytes)), int(n.info.MaxTransferBytes)} {
		data := make([]byte, length)
		for i := range data {
			data[i] = byte(i ^ length ^ 0xa5)
		}
		write, err := scratch.WriteCommand(data, 0, true)
		require.NoError(t, err)
		require.NoError(t, Status(run(write)))
		got := make([]byte, length)
		read, err := scratch.ReadCommand(got, 0)
		require.NoError(t, err)
		require.NoError(t, Status(run(read)))
		require.Equal(t, data, got)
	}
	last := scratch.Size() - uint64(len(buf))
	for i := range buf {
		buf[i] = byte(i ^ 0x5a)
	}
	write, err := scratch.WriteCommand(buf, last, false)
	require.NoError(t, err)
	require.NoError(t, Status(run(write)))
	got := make([]byte, len(buf))
	read, err = scratch.ReadCommand(got, last)
	require.NoError(t, err)
	require.NoError(t, Status(run(read)))
	require.Equal(t, buf, got)

	// Batches wrap both extended queues while keeping each buffer associated
	// with its handle. Completion order is deliberately not assumed.
	for batch := 0; batch < 4; batch++ {
		bytes := min(max(4096, int(n.info.BlockSize)), int(n.info.MaxTransferBytes))
		count := ring.Capacity()
		defer scratch.Close()
		require.GreaterOrEqual(t, scratch.Size(), uint64(count*bytes))
		expected := make([][]byte, count)
		actual := make([][]byte, count)
		for i := range expected {
			expected[i] = make([]byte, bytes)
			actual[i] = make([]byte, bytes)
			for j := range expected[i] {
				expected[i][j] = byte(batch*31 + i*7 + j)
			}
		}
		for _, writing := range []bool{true, false} {
			handles := make(map[ringo.Handle]int, count)
			for i := 0; i < count; i++ {
				var op Command
				if writing {
					op, err = scratch.WriteCommand(expected[i], uint64(i*bytes), false)
				} else {
					op, err = scratch.ReadCommand(actual[i], uint64(i*bytes))
				}
				require.NoError(t, err)
				h, err := ring.Push(prepare(op))
				require.NoError(t, err)
				handles[h] = i
			}
			for len(handles) > 0 {
				_, err := ring.SubmitAndWait(1)
				require.NoError(t, err)
				for c := range ring.Reap() {
					_, found := handles[c.Handle]
					require.True(t, found, "unexpected completion")
					require.NoError(t, Status(c))
					delete(handles, c.Handle)
				}
			}
		}
		for i := range expected {
			require.Equal(t, expected[i], actual[i])
		}
	}
	flush := n.FlushCommand()
	require.NoError(t, Status(run(flush)))
}
