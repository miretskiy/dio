package sys_test

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/miretskiy/dio/align"
	"github.com/miretskiy/dio/sys"
)

// ─── AlignForHolePunch ───────────────────────────────────────────────────────

func TestAlignForHolePunch(t *testing.T) {
	bs := int64(align.BlockSize)
	tests := []struct {
		name                       string
		offset, length             int64
		wantOffset, wantLength     int64
		wantCanPunch               bool
	}{
		{
			name:         "perfect alignment",
			offset:       bs, length: bs,
			wantOffset: bs, wantLength: bs, wantCanPunch: true,
		},
		{
			name:         "sub-block — cannot punch",
			offset:       0, length: bs - 1,
			wantCanPunch: false,
		},
		{
			name:         "offset=1, length=4096 — rounds up past end",
			offset:       1, length: bs,
			wantCanPunch: false,
		},
		{
			name:         "offset just past page (bs+1)",
			offset:       bs + 1, length: bs,
			wantCanPunch: false,
		},
		{
			name:   "large blob with small misalignment",
			offset: 100, length: 3*bs + 200,
			wantOffset: bs, wantLength: 2 * bs, wantCanPunch: true,
		},
		{
			name:   "exactly 2 blocks, offset=1",
			offset: 1, length: 2 * bs,
			wantOffset: bs, wantLength: bs, wantCanPunch: true,
		},
		{
			name:   "large aligned punch",
			offset: 10 * bs, length: 100 * bs,
			wantOffset: 10 * bs, wantLength: 100 * bs, wantCanPunch: true,
		},
		{
			name:   "end-of-file scenario",
			offset: 100*bs - 10, length: bs + 100,
			wantOffset: 100 * bs, wantLength: bs, wantCanPunch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOff, gotLen, gotCan := sys.AlignForHolePunch(tt.offset, tt.length)

			require.Equal(t, tt.wantCanPunch, gotCan, "canPunch")
			if !tt.wantCanPunch {
				return
			}
			require.Equal(t, tt.wantOffset, gotOff, "alignedOffset")
			require.Equal(t, tt.wantLength, gotLen, "alignedLength")

			// Invariants
			require.Zero(t, gotOff%bs, "offset must be block-aligned")
			require.Zero(t, gotLen%bs, "length must be block-aligned")
			require.GreaterOrEqual(t, gotLen, bs, "length must be at least one block")
			// The punched range must not extend into the preceding blob.
			require.GreaterOrEqual(t, gotOff, tt.offset)
		})
	}
}

// ─── Fdatasync ───────────────────────────────────────────────────────────────

func TestFdatasync_Buffered(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "fdatasync-*.bin")
	require.NoError(t, err)
	defer f.Close()

	_, err = f.Write([]byte("hello fdatasync"))
	require.NoError(t, err)

	require.NoError(t, sys.Fdatasync(f))
}

func TestFdatasync_MultipleWritesThenSync(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "fdatasync-multi-*.bin")
	require.NoError(t, err)
	defer f.Close()

	for i := 0; i < 100; i++ {
		_, err = f.Write([]byte("line of data\n"))
		require.NoError(t, err)
	}
	require.NoError(t, sys.Fdatasync(f))

	info, err := f.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(100*13), info.Size())
}

func TestFdatasync_AppendMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fdatasync-append.bin")
	f, err := os.Create(path)
	require.NoError(t, err)
	_, _ = f.Write([]byte("initial\n"))
	f.Close()

	f, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	defer f.Close()

	_, err = f.Write([]byte("appended\n"))
	require.NoError(t, err)
	require.NoError(t, sys.Fdatasync(f))
}

// ─── Fallocate ───────────────────────────────────────────────────────────────

func TestFallocate_SetsFileSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fallocate.bin")

	// CreateDirect opens O_WRONLY; we need O_RDWR to read back.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	require.NoError(t, err)
	defer f.Close()

	info, _ := f.Stat()
	require.Equal(t, int64(0), info.Size(), "newly created file must be empty")

	const allocSize = int64(16 << 20) // 16 MiB
	if err = sys.Fallocate(f, allocSize); err != nil {
		t.Logf("Fallocate returned error (expected on some FS): %v", err)
	}

	// Write and read back at a non-zero offset to exercise the allocation.
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}
	_, err = f.WriteAt(data, 8192)
	require.NoError(t, err)

	got := make([]byte, 4096)
	_, err = f.ReadAt(got, 8192)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

// ─── CreateDirect / OpenDirect ────────────────────────────────────────────────

func TestCreateDirect_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "direct.bin")
	data := align.AllocAligned(4096)
	defer align.FreeAligned(data)
	for i := range data {
		data[i] = byte(i % 251)
	}

	// Write via CreateDirect.
	f, err := sys.CreateDirect(path, sys.FlDirectIO)
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Read back normally.
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestOpenDirect_ReadsContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "open-direct.bin")
	payload := []byte("hello from OpenDirect\n")
	require.NoError(t, os.WriteFile(path, payload, 0644))

	f, err := sys.OpenDirect(path, sys.SyncNone)
	require.NoError(t, err)
	defer f.Close()

	buf := make([]byte, len(payload))
	n, err := f.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.Equal(t, payload, buf)
}

// ─── WriteFile ───────────────────────────────────────────────────────────────

func TestWriteFile_AlignedWithDirectIO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "write-aligned.bin")
	data := align.AllocAligned(8192)
	defer align.FreeAligned(data)
	for i := range data {
		data[i] = byte(i % 256)
	}

	err := sys.WriteFile(path, data, sys.FlDirectIO|sys.SyncData)
	require.NoError(t, err)

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestWriteFile_UnalignedWithDirectIO_Fails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unaligned.bin")
	base := align.AllocAligned(8192)
	defer align.FreeAligned(base)

	// An unaligned sub-slice.
	unaligned := base[1:4097]
	err := sys.WriteFile(path, unaligned, sys.FlDirectIO)
	require.ErrorIs(t, err, sys.ErrAlignment)
}

func TestWriteFile_UnalignedWithoutDirectIO_Succeeds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unaligned-nodirect.bin")
	data := []byte("hello world — no alignment required")

	require.NoError(t, sys.WriteFile(path, data, sys.SyncNone))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

// ─── writev patterns (via net.Buffers) ───────────────────────────────────────

func TestWritev_AppendMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "writev-append.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	defer f.Close()

	const (
		records    = 100
		recordSize = 1024
	)
	var buffers net.Buffers
	for i := 0; i < records; i++ {
		buf := make([]byte, recordSize)
		for j := range buf {
			buf[j] = byte((i + j) % 256)
		}
		buffers = append(buffers, buf)
	}

	n, err := buffers.WriteTo(f)
	require.NoError(t, err)
	require.Equal(t, int64(records*recordSize), n)

	require.NoError(t, sys.Fdatasync(f))

	info, err := f.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(records*recordSize), info.Size())
}

func TestWritev_LargeBatch(t *testing.T) {
	// 2000 buffers exceeds IOV_MAX (1024) on Linux; Go's net.Buffers must chunk.
	path := filepath.Join(t.TempDir(), "writev-large.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	defer f.Close()

	const (
		records    = 2000
		recordSize = 512
	)
	var buffers net.Buffers
	for i := 0; i < records; i++ {
		buf := make([]byte, recordSize)
		for j := range buf {
			buf[j] = byte((i + j) % 256)
		}
		buffers = append(buffers, buf)
	}

	n, err := buffers.WriteTo(f)
	require.NoError(t, err)
	require.Equal(t, int64(records*recordSize), n)
	require.NoError(t, sys.Fdatasync(f))
}

// ─── IsTransientIOError ──────────────────────────────────────────────────────

func TestIsTransientIOError(t *testing.T) {
	require.False(t, sys.IsTransientIOError(nil))
	require.False(t, sys.IsTransientIOError(os.ErrNotExist))
	require.False(t, sys.IsTransientIOError(os.ErrPermission))
}
