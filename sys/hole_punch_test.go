//go:build linux

package sys_test

// Linux-only: these tests rely on sparse-file block counts via
// syscall.Stat_t.Blocks, which differ across filesystems and are only
// meaningful on Linux in the context of FALLOC_FL_PUNCH_HOLE.

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/sys"
)

func TestPunchHole_DataIntegrity(t *testing.T) {
	bs := int64(align.BlockSize)
	testFile := filepath.Join(t.TempDir(), "integrity.dat")

	// 1. Create a file: [AAAA…][BBBB…][CCCC…]
	f, err := os.OpenFile(testFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)
	defer f.Close()

	blockA := bytes.Repeat([]byte{0xA1}, int(bs))
	blockB := bytes.Repeat([]byte{0xB2}, int(bs))
	blockC := bytes.Repeat([]byte{0xC3}, int(bs))

	_, err = f.Write(append(append(blockA, blockB...), blockC...))
	require.NoError(t, err)
	f.Sync()

	// 2. Record physical block count before punch.
	fiBefore, err := f.Stat()
	require.NoError(t, err)
	blocksBefore := fiBefore.Sys().(*syscall.Stat_t).Blocks
	t.Logf("before punch: %d blocks (%.2f KiB)", blocksBefore, float64(blocksBefore*512)/1024)

	// 3. Punch the middle block (B) only.
	reclaimed, err := sys.PunchHole(f, bs, bs)
	require.NoError(t, err)
	require.Equal(t, bs, reclaimed, "aligned punch should reclaim exactly one block")
	f.Sync()

	// 4. Physical block count must decrease.
	fiPath, err := os.Stat(testFile)
	require.NoError(t, err)
	blocksAfter := fiPath.Sys().(*syscall.Stat_t).Blocks
	t.Logf("after punch: %d blocks (%.2f KiB)", blocksAfter, float64(blocksAfter*512)/1024)

	expectedBlocks := (bs * 2) / 512
	require.EqualValues(t, expectedBlocks, blocksAfter,
		"exactly two blocks should remain after punching the middle one")

	// 5. Data integrity: A and C intact; B zeroed.
	buf := make([]byte, bs*3)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)

	require.Equal(t, blockA, buf[:bs], "block A (before hole) must be intact")
	require.Equal(t, make([]byte, bs), buf[bs:bs*2], "punched block B must read as zeroes")
	require.Equal(t, blockC, buf[bs*2:], "block C (after hole) must be intact")
}

func TestPunchHole_PartialBlockSafety(t *testing.T) {
	bs := int64(align.BlockSize)
	f, err := os.CreateTemp(t.TempDir(), "punch-partial-*.dat")
	require.NoError(t, err)
	defer f.Close()

	data := bytes.Repeat([]byte{0xFF}, int(bs*2))
	_, err = f.Write(data)
	require.NoError(t, err)

	// Range entirely within one block — no complete block to punch.
	reclaimed, err := sys.PunchHole(f, bs/2, bs/2)
	require.NoError(t, err)
	require.Equal(t, int64(0), reclaimed, "sub-block range must reclaim 0 bytes")

	// Verify the file is untouched.
	buf := make([]byte, bs*2)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, data, buf, "partial punch must not modify data")
}

func TestPunchHole_NonAlignedReclamation(t *testing.T) {
	bs := int64(align.BlockSize)
	f, err := os.CreateTemp(t.TempDir(), "punch-nonaligned-*.dat")
	require.NoError(t, err)
	defer f.Close()

	data := bytes.Repeat([]byte{0xAA}, int(bs*10))
	_, err = f.Write(data)
	require.NoError(t, err)
	f.Sync()

	// Case 1: misaligned blob spanning ~3 blocks → only 2 complete blocks punched.
	r1, err := sys.PunchHole(f, 100, 3*bs+200)
	require.NoError(t, err)
	require.Equal(t, 2*bs, r1,
		"alignment should conserve the edge blocks and punch only 2 complete blocks")

	// Case 2: range smaller than one block → 0 reclaimed.
	r2, err := sys.PunchHole(f, 5*bs+100, bs-500)
	require.NoError(t, err)
	require.Equal(t, int64(0), r2)

	// Case 3: perfectly aligned → reclaims full amount.
	r3, err := sys.PunchHole(f, 7*bs, 2*bs)
	require.NoError(t, err)
	require.Equal(t, 2*bs, r3)

	t.Logf("case 1: reclaimed %d", r1)
	t.Logf("case 2: reclaimed %d", r2)
	t.Logf("case 3: reclaimed %d", r3)
}
